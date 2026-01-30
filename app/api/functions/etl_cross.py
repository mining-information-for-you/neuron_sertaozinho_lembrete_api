from datetime import datetime
from io import BytesIO
from typing import List

import bs4
from asyncpg import Connection

from ...db import close_db_connection, get_db_connection


def parse_headers(soup: bs4.BeautifulSoup) -> List:
    """
    Extrai os dados das tags <center> do arquivo HTML em um dicionário
    e faz o append() em uma lista de headers

    soup: objeto soup com as tags HTML
    """
    headers = soup.find_all("center")
    header_agenda = []

    for header in headers:
        rows = header.find_all("td")
        if len(rows) < 2:
            continue

        unidade_executante = rows[0].get_text().split(":")[1].strip()
        profissional_info = rows[1].get_text().split(":")

        dados_agenda = {
            "unidade_executante": unidade_executante,
            "profissional": profissional_info[1].split("\n")[0].strip(),
            "data_agenda": profissional_info[2].split("\n")[0].strip(),
            "especialidade": profissional_info[4].split("\n")[0].strip(),
        }

        header_agenda.append(dados_agenda)

    return header_agenda


def parse_table_data(soup: bs4.BeautifulSoup, header_agenda: List):
    """
    Faz o parse de todas as tags <table> do HTML, e extrai os dados de
    agendamentos. É retornado (yield) um dicionário com os dados do header de
    mesmo index da tabela (table) através de desempacotamento **.

    soup: objeto soup com as tags HTML
    header_agenda: lista de dicionários com dados do cabeçalho do agendamento
    """
    data_tables = soup.find_all("table", {"cellpadding": "3", "border": "1"})

    for i, table in enumerate(data_tables):
        rows = table.find_all("tr")[1:]

        for j in range(0, len(rows), 2):
            # Evita exceção IndexError
            if j + 1 >= len(rows):
                break

            # next_row é utilizado para selecionar os telefones, que ficam
            # na próxima linha <tr>
            current_row, next_row = rows[j], rows[j + 1]
            campos = current_row.find_all("td")
            campos_next_row = next_row.find_all("td")

            # Elimina dados desnecessários e vazios da tabela
            horario = campos[0].get_text()
            codigo = campos[1].get_text()
            paciente = campos[2].get_text()
            solicitante = campos[-3].get_text()
            if (
                not campos
                or codigo in ("", "Horário")
                or paciente in ("", "Agendamento")
            ):  # noqa
                continue

            # Faz o split de todos os telefones contidos na agenda do usuário
            telefones = campos_next_row[0].get_text().split(":")[1].split(" | ")  # noqa
            yield {
                **header_agenda[i],
                "horario": horario,
                "codigo": codigo,
                "paciente": paciente,
                "solicitante": solicitante,
                "telefones": telefones,
            }


async def insert_data(
    company_id: int,
    filename,
    user_id: int,
    data_hora_enviar: datetime,
    data_hora_upload: datetime,
    data: List,
    conn: Connection,
    tipo_envio: str,
    template_id: str,
) -> None:
    """
    Faz a inserção no banco de dados utilizando executemany
    """

    valores = []
    nome_arquivo = filename

    def parse_datetime_flex(value):
        if isinstance(value, datetime):
            return value

        formatos = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
        ]

        for fmt in formatos:
            try:
                return datetime.strptime(value, fmt)
            except:
                pass

        # última tentativa: formato ISO automático
        try:
            return datetime.fromisoformat(value)
        except:
            pass

        raise ValueError(f"Formato de data inválido: {value}")

    for record in data:
        data_agenda = datetime.strptime(record["data_agenda"], "%d-%m-%Y").date()
        horario = datetime.strptime(record["horario"], "%H:%M").time()
        codigo = int(record["codigo"])
        telefones = [telefone.strip() for telefone in record["telefones"]]
        data_hora_envio = parse_datetime_flex(data_hora_enviar)
        data_hora_upar = parse_datetime_flex(data_hora_upload)
        nome_completo = await conn.fetchval(
            f"SELECT nomecompleto FROM usuarios WHERE id = {user_id}"
        )

        for telefone in telefones:
            valores.append(
                (
                    company_id,
                    record["unidade_executante"],
                    record["profissional"],
                    data_agenda,
                    record["especialidade"],
                    horario,
                    str(codigo),
                    record["paciente"],
                    telefone,
                    data_hora_envio,
                    data_hora_upar,
                    record["solicitante"],
                    nome_arquivo,
                    user_id,
                    nome_completo,
                    template_id,
                    tipo_envio,
                )
            )

    # Insere os dados em lote utilizando executemany
    if valores:
        query = """
            INSERT INTO lembrete_sertaozinho (
                empresa_id, unidade_executante, profissional,
                data_agenda, especialidade, horario, codigo, paciente,
                telefone, data_hora_enviar, data_hora_upload, solicitante, nome_arquivo,
                id_usuario, nome_usuario, template_id, tipo_envio
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        """
        await conn.executemany(query, valores)


async def etl(
    company_id: int,
    filename: str,
    user_id: int,
    data_hora_enviar: datetime,
    data_hora_upload: datetime,
    blob_file: BytesIO,
    tipo_envio: str,
    template_id: str,
) -> None:
    soup = bs4.BeautifulSoup(blob_file, features="html.parser")

    header_agenda = parse_headers(soup)

    conn = await get_db_connection()

    data = parse_table_data(soup, header_agenda)
    await insert_data(
        company_id,
        filename,
        user_id,
        data_hora_enviar,
        data_hora_upload,
        data,
        conn,
        tipo_envio,
        template_id,
    )

    await close_db_connection(conn)


if __name__ == "__main__":
    with open("app/api/functions/teste_erro.xls", "r") as f:
        file = f.read()

    soup = bs4.BeautifulSoup(file, features="html.parser")
    header = parse_headers(soup)
    data = parse_table_data(soup, header)

    for record in data:
        print(record)
        # for telefone in record['telefones']:
        #     print(telefone)

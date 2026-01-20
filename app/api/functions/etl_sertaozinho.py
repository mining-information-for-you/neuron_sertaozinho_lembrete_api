from datetime import datetime
from io import BytesIO
from typing import List, Dict
import re

import pdfplumber
import pandas as pd
from asyncpg import Connection

from ...db import get_db_connection, close_db_connection


def pdf_to_text(pdf_file: BytesIO) -> str:
    pdf_file.seek(0)
    text = ""
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text


def parse_header(text: str) -> Dict:
    header = {}

    m = re.search(r'Unidade de Saúde\s+(.*)', text)
    if m:
        header["unidade_saude"] = m.group(1).strip()

    m = re.search(r'Data Atendimento\s+(\d{2}/\d{2}/\d{4})', text)
    if m:
        header["data_atendimento"] = datetime.strptime(
            m.group(1), "%d/%m/%Y"
        ).date()

    m = re.search(r'Profissional:\s+(.*?)\s+CRM[:\s]*(\d+)', text)
    if m:
        header["profissional"] = m.group(1).strip()
        header["crm_profissional"] = m.group(2).strip()

    m = re.search(r'Especialidade:\s+(.*)', text)
    if m:
        header["especialidade"] = m.group(1).strip()

    return header


def parse_patients_tables(pdf_file: BytesIO) -> List[Dict]:
    pdf_file.seek(0)
    dados_extraidos = []

    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    clean_row = [
                        str(cell).replace('\n', ' ').strip() if cell else ''
                        for cell in row
                    ]
                    if any(clean_row):
                        dados_extraidos.append(clean_row)

    df = pd.DataFrame(dados_extraidos)

    colunas_sugeridas = [
        "Prontuario", "Nome Paciente", "Idade", "CNS", "Tel.Cell",
        "Data/Hora Agendamento", "Data/Hora Recepção",
        "Data/Hora Atendimento", "Data/Hora Encerramento",
        "Status", "Assinatura"
    ]

    if df.shape[1] >= len(colunas_sugeridas):
        df = df.iloc[:, :len(colunas_sugeridas)]
        df.columns = colunas_sugeridas

        if not df.empty and df.iloc[0]["Nome Paciente"] == "Nome Paciente":
            df = df.iloc[1:]

    pacientes = []

    for _, row in df.iterrows():
        try:
            nome = str(row["Nome Paciente"]).strip()
            linha_completa = " ".join(str(v) for v in row.values)

            numeros = re.findall(r"\b\d+\b", linha_completa)
            cns = None
            for i in range(len(numeros) - 1):
                combinado = numeros[i] + numeros[i + 1]
                if len(combinado) == 15:
                    cns = combinado
                    break

            tel_match = re.search(r"\b\d{10,11}\b", linha_completa)

            data_hora = None
            campo_data_hora = str(row["Data/Hora Atendimento"]).strip()

            if campo_data_hora:
                try:
                    data_hora = datetime.strptime(
                        campo_data_hora, "%d/%m/%Y %H:%M"
                    )
                except ValueError:
                    data_hora = None

            if not data_hora:
                data_match = re.search(r"\d{2}/\d{2}/\d{4}", linha_completa)
                hora_match = re.search(r"\b\d{2}:\d{2}\b", linha_completa)
                if data_match and hora_match:
                    data_hora = datetime.strptime(
                        f"{data_match.group()} {hora_match.group()}",
                        "%d/%m/%Y %H:%M"
                    )

            if not nome or not cns or not data_hora:
                continue

            pacientes.append({
                "paciente": nome.title(),
                "cns": cns,
                "telefone": tel_match.group() if tel_match else None,
                "data_hora_agendamento": data_hora,
                "classificacao": "CONSULTA",
                "status": "AGENDADO",
            })

        except Exception:
            continue

    return pacientes


async def insert_data(
    conn: Connection,
    company_id: int,
    filename: str,
    user_id: int,
    data_hora_enviar: datetime,
    data_hora_upload: datetime,
    header: Dict,
    pacientes: List[Dict],
) -> None:

    nome_usuario = await conn.fetchval(
        "SELECT nomecompleto FROM usuarios WHERE id = $1",
        user_id
    )

    valores = [
        (
            company_id,
            header.get("unidade_saude"),
            header.get("profissional"),
            header.get("crm_profissional"),
            header.get("especialidade"),
            header.get("data_atendimento"),
            p["data_hora_agendamento"],
            p["paciente"],
            p["cns"],
            p["telefone"],
            p["classificacao"],
            p["status"],
            data_hora_enviar,
            data_hora_upload,
            filename,
            user_id,
            nome_usuario,
        )
        for p in pacientes
    ]

    await conn.executemany(
        """
        INSERT INTO lembrete_sertaozinho (
            empresa_id, unidade_saude, profissional, crm_profissional,
            especialidade, data_atendimento, data_hora_agendamento,
            paciente, cns, telefone, classificacao, status,
            data_hora_enviar, data_hora_upload, nome_arquivo,
            id_usuario, nome_usuario
        )
        VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
            $11,$12,$13,$14,$15,$16,$17
        )
        """,
        valores
    )


async def etl_sertaozinho(
    company_id: int,
    filename: str,
    user_id: int,
    data_hora_enviar: datetime,
    data_hora_upload: datetime,
    blob_file: BytesIO
) -> None:

    header_text = pdf_to_text(blob_file)
    header = parse_header(header_text)

    pacientes = parse_patients_tables(blob_file)

    conn = await get_db_connection()
    try:
        await insert_data(
            conn,
            company_id,
            filename,
            user_id,
            data_hora_enviar,
            data_hora_upload,
            header,
            pacientes
        )
    finally:
        await close_db_connection(conn)

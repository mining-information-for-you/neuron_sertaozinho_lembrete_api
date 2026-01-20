import jwt
from datetime import date, time, datetime

from fastapi import APIRouter, HTTPException, Query, status

from ...db import close_db_connection, get_db_connection
from ..functions.utils import require_valid_token

router = APIRouter()


@router.get('/schedule')
@require_valid_token
async def get_schedule(
    permission_token: str,
    mi4u_access_token: str,

    # Identificação
    id: int = Query(None),
    paciente: str = Query(None),
    prontuario: str = Query(None),
    cns: str = Query(None),
    telefone: str = Query(None),

    # Cabeçalho
    unidade_saude: str = Query(None),
    profissional: str = Query(None),
    crm_profissional: str = Query(None),
    especialidade: str = Query(None),
    data_atendimento: date = Query(None),

    # Agenda
    horario_agendamento: time = Query(None),
    classificacao: str = Query(None),
    escala: str = Query(None),
    status_atendimento: str = Query(None, alias="status"),

    # Controle
    data_hora_enviar: datetime = Query(None),
    data_envio: str = Query(None, description="dd/mm/yyyy"),
    nome_arquivo: str = Query(None),
    id_usuario: int = Query(None),
    nome_usuario: str = Query(None),

    # WhatsApp
    wa_message_id: str = Query(None),
    resposta: str = Query(None),
    dt_resposta: datetime = Query(None),
):
    try:
        decoded_token = jwt.decode(
            mi4u_access_token,
            options={'verify_signature': False}
        )
        company_id = decoded_token['sub']['company_id']

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Invalid mi4u_access_token: {e}'
        )

    conn = await get_db_connection()

    try:
        query = "SELECT * FROM lembrete_sertaozinho"
        conditions = []
        params = []
        param_index = 1

        # 📅 data_envio (atalho)
        if data_envio:
            try:
                parsed_date = datetime.strptime(data_envio, '%d/%m/%Y')
                data_hora_enviar = parsed_date
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Formato inválido. Use dd/mm/yyyy"
                )

        filters = {
            'id': id,
            'empresa_id': company_id,
            'paciente': paciente,
            'prontuario': prontuario,
            'cns': cns,
            'telefone': telefone,
            'unidade_saude': unidade_saude,
            'profissional': profissional,
            'crm_profissional': crm_profissional,
            'especialidade': especialidade,
            'data_atendimento': data_atendimento,
            'horario_agendamento': horario_agendamento,
            'classificacao': classificacao,
            'escala': escala,
            'status': status_atendimento,
            'nome_arquivo': nome_arquivo,
            'id_usuario': id_usuario,
            'nome_usuario': nome_usuario,
            'wa_message_id': wa_message_id,
            'resposta': resposta,
            'dt_resposta': dt_resposta,
        }

        for field, value in filters.items():
            if value is None:
                continue

            if field == 'data_hora_enviar' and isinstance(value, datetime):
                start = value.replace(hour=0, minute=0, second=0, microsecond=0)
                end = value.replace(hour=23, minute=59, second=59, microsecond=999999)
                conditions.append(
                    f"data_hora_enviar BETWEEN ${param_index} AND ${param_index + 1}"
                )
                params.extend([start, end])
                param_index += 2

            elif isinstance(value, str):
                conditions.append(f"{field} ILIKE ${param_index}")
                params.append(f"%{value}%")
                param_index += 1

            else:
                conditions.append(f"{field} = ${param_index}")
                params.append(value)
                param_index += 1

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY data_atendimento, horario_agendamento LIMIT 10000"

        return await conn.fetch(query, *params)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao buscar agenda: {e}"
        )
    finally:
        await close_db_connection(conn)

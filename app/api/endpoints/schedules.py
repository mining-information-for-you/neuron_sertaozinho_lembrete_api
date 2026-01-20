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
        id: int = Query(None),
        unidade_executante: str = Query(None),
        profissional: str = Query(None),
        data_agenda: date = Query(None),
        especialidade: str = Query(None),
        horario: time = Query(None),
        codigo: int = Query(None),
        paciente: str = Query(None),
        telefone: str = Query(None),
        data_hora_enviar: datetime = Query(None),
        data_envio: str = Query(None, description="Data no formato dd/mm/yyyy para filtrar data_hora_enviar"),
        customer_service_id: int = Query(None),
        wa_message_id: str = Query(None),
        resposta: str = Query(None),
        dt_resposta: datetime = Query(None),
        nome_arquivo: str = Query(None),
        id_usuario: int = Query(None),
        nome_usuario: str = Query(None),

):
    try:
        decoded_token = jwt.decode(
            mi4u_access_token,
            options={'verify_signature': False}
        )
        company_id = decoded_token.get('sub').get('company_id')

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Invalid mi4u_access_token. {e}'
        )

    conn = await get_db_connection()

    try:
        query = 'SELECT * FROM lembrete_sertaozinho'
        conditions = []
        params = {}

        # Processar data_envio se fornecida
        if data_envio:
            try:
                # Converter dd/mm/yyyy para datetime
                parsed_date = datetime.strptime(data_envio, '%d/%m/%Y')
                data_hora_enviar = parsed_date  # Sobrescrever se data_envio foi fornecida
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail='Formato de data inválido. Use dd/mm/yyyy (ex: 09/09/2025)'
                )

            print(data_hora_enviar)

        filters = {
            'id': id,
            'empresa_id': company_id,
            'unidade_executante': unidade_executante,
            'profissional': profissional,
            'data_agenda': data_agenda,
            'especialidade': especialidade,
            'horario': horario,
            'codigo': codigo,
            'paciente': paciente,
            'telefone': telefone,
            'data_hora_enviar': data_hora_enviar,
            'customer_service_id': customer_service_id,
            'wa_message_id': wa_message_id,
            'resposta': resposta,
            'dt_resposta': dt_resposta,
            'nome_arquivo': nome_arquivo,
            'id_usuario': id_usuario,
            'nome_usuario': nome_usuario
        }

        param_index = 1
        for field, value in filters.items():
            if value is not None:
                if field == 'data_hora_enviar' and isinstance(value, datetime):
                    # Filtrar por data completa (ignorando horário)
                    if data_envio:
                        start_date = value.replace(hour=0, minute=0, second=0, microsecond=0)
                        end_date = value.replace(hour=23, minute=59, second=59, microsecond=999999)
                    else:
                        start_date = value
                        end_date = value
                    conditions.append(f'{field} >= ${param_index} AND {field} <= ${param_index + 1}')
                    params[f'{field}_start'] = start_date
                    params[f'{field}_end'] = end_date
                    param_index += 2
                    print(value)
                    print(data_hora_enviar)
                    print(start_date)
                    print(end_date)
                    print(value)
                    print(data_hora_enviar)
                    print(data_envio)
                elif isinstance(value, str):
                    conditions.append(f'{field} ILIKE ${param_index}')
                    params[field] = f'%{value}%'
                    param_index += 1
                else:
                    conditions.append(f'{field} = ${param_index}')
                    params[field] = value
                    param_index += 1

        if conditions:
            query += ' WHERE ' + ' AND '.join(conditions)

        query += ' LIMIT 10000'

        data = await conn.fetch(query, *params.values())
        return data

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Failed to fetch schedule: {str(e)}'
        )
    finally:
        await close_db_connection(conn)


@router.post('/schedule/set_response')
@require_valid_token
async def update_response(
        permission_token: str,
        wa_message_id: str,
        resposta: str,
):
    try:
        conn = await get_db_connection()
        dt_resposta = datetime.now().replace(microsecond=0)

        query = '''
                UPDATE lembrete_sertaozinho \
                SET resposta    = $1, \
                    dt_resposta = $2
                WHERE wa_message_id = $3 \
                '''

        data = await conn.execute(
            query,
            resposta,
            dt_resposta,
            wa_message_id
        )
        return data

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Falha ao fazer o update: {str(e)}'
        )
    finally:
        await close_db_connection(conn)
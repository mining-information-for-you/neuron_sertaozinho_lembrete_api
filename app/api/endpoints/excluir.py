from typing import List, Union

import jwt
from fastapi import APIRouter, Body, HTTPException, status

from ...db import close_db_connection, get_db_connection
from ..functions.utils import require_valid_token

router = APIRouter(tags=["Excluir"], prefix="/excluir")


@router.delete("")
@require_valid_token
async def delete_excluir(
    permission_token: str,
    mi4u_access_token: str,
    ids: Union[int, List[int]] = Body(...),
):

    try:
        decoded_token = jwt.decode(
            mi4u_access_token, options={"verify_signature": False}
        )
        company_id = decoded_token.get("sub").get("company_id")

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid mi4u_access_token. {e}",
        )

    try:
        if isinstance(ids, int):
            ids = [ids]

        conn = await get_db_connection()

        already_inactive = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1
                FROM lembrete_sertaozinho
                WHERE id = ANY($1)
                  AND ativo = 'N'
            )
            """,
            ids,
        )

        if already_inactive:
            await close_db_connection(conn)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Um ou mais registros já estão inativos.",
            )

        await conn.execute(
            """
            UPDATE lembrete_sertaozinho
            SET ativo = 'N'
            WHERE id = ANY($1)
            """,
            ids,
        )

        await close_db_connection(conn)

        return {
            "status": "success",
            "message": f"{len(ids)} registro(s) desativado(s) com sucesso.",
            "data": {"ids": ids},
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Falha ao fazer o update: {str(e)}",
        )

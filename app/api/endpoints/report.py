from collections import defaultdict
from datetime import datetime

import jwt
import pandas as pd
from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse

from ...db import close_db_connection, get_db_connection
from ..functions.utils import require_valid_token

router = APIRouter()


@router.get("/report", status_code=status.HTTP_200_OK)
@require_valid_token
async def get_report(
    permission_token: str, mi4u_access_token: str, dt_start: str, dt_end: str
):
    try:
        decoded_token = jwt.decode(
            mi4u_access_token, options={"verify_signature": False}
        )
        company_id = decoded_token.get("sub", {}).get("company_id")
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid mi4u_access_token. {e}",
        )

    dt_start = datetime.strptime(dt_start, "%d-%m-%Y").date()
    dt_end = datetime.strptime(dt_end, "%d-%m-%Y").date()

    conn = await get_db_connection()

    try:
        query = """
            SELECT
                solicitante,
                date_trunc('month', data_agenda) AS periodo_ordem,
                TO_CHAR(date_trunc('month', data_agenda), 'MM-YYYY') AS periodo,

                COUNT(*) FILTER (WHERE resposta = 'CONFIRMO')            AS confirmado,
                COUNT(*) FILTER (WHERE resposta = 'NÃOㅤCONFIRMO')       AS nao_confirmado,
                COUNT(*) FILTER (WHERE resposta = 'NÃOㅤCONHEÇO')        AS nao_conheco,
                COUNT(*) FILTER (
                    WHERE resposta IS NULL OR TRIM(resposta) = ''
                ) AS nao_respondido

            FROM cross_agendamentos
            WHERE data_agenda BETWEEN $1 AND $2
              AND solicitante IS NOT NULL
              AND solicitante <> ''
            GROUP BY solicitante, periodo_ordem
            ORDER BY solicitante, periodo_ordem
        """

        rows = await conn.fetch(query, dt_start, dt_end)

        if not rows:
            return {
                "success": True,
                "message": "Não há registros para o período informado",
                "data": [],
            }

        agrupado = defaultdict(list)

        for r in rows:
            agrupado[r["solicitante"]].append(
                {
                    "periodo_ordem": r["periodo_ordem"],
                    "periodo": r["periodo"],
                    "status": {
                        "confirmado": r["confirmado"],
                        "nao_confirmado": r["nao_confirmado"],
                        "nao_conheco": r["nao_conheco"],
                        "nao_respondido": r["nao_respondido"],
                    },
                }
            )

        data = []
        for solicitante, meses in agrupado.items():
            data.append(
                {
                    "solicitante": solicitante,
                    "meses": [
                        {
                            "periodo": m["periodo"],
                            "status": m["status"],
                        }
                        for m in meses
                    ],
                }
            )

        return {
            "success": True,
            "message": "Relatório gerado com sucesso",
            "data": data,
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch report: {str(e)}",
        )
    finally:
        await close_db_connection(conn)


@router.get("/report/details", status_code=status.HTTP_200_OK)
@require_valid_token
async def get_report_details(
    permission_token: str, mi4u_access_token: str, dt_start: str, dt_end: str
):
    # Valida o token mi4u
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

    conn = await get_db_connection()

    dt_start = datetime.strptime(dt_start, "%d-%m-%Y").date()
    dt_end = datetime.strptime(dt_end, "%d-%m-%Y").date()

    query = """
        SELECT paciente, telefone, solicitante, resposta
        FROM cross_agendamentos
        WHERE data_agenda BETWEEN $1 AND $2
          AND solicitante IS NOT NULL
    """

    data = await conn.fetch(query, dt_start, dt_end)

    df = pd.DataFrame(data, columns=["paciente", "telefone", "solicitante", "resposta"])

    resultado = defaultdict(
        lambda: {
            "confirmados": [],
            "nao_confirmados": [],
            "nao_reconhecidos": [],
            "nao_respondidos": [],
        }
    )

    for _, row in df.iterrows():
        solicitante = row["solicitante"]

        item = {"cliente": row["paciente"], "telefone": row["telefone"]}

        resposta = (row["resposta"] or "").strip().upper()

        if resposta == "CONFIRMO":
            resultado[solicitante]["confirmados"].append(item)

        elif resposta == "NÃOㅤCONFIRMO":
            resultado[solicitante]["nao_confirmados"].append(item)

        elif resposta == "NÃOㅤCONHEÇO":
            resultado[solicitante]["nao_reconhecidos"].append(item)

        else:
            resultado[solicitante]["nao_respondidos"].append(item)

    data_final = [
        {
            "solicitante": solicitante,
            "confirmados": dados["confirmados"],
            "nao_confirmados": dados["nao_confirmados"],
            "nao_reconhecidos": dados["nao_reconhecidos"],
            "nao_respondidos": dados["nao_respondidos"],
        }
        for solicitante, dados in resultado.items()
    ]

    return JSONResponse(
        content={
            "success": True,
            "message": "Relatório gerado com sucesso",
            "data": data_final,
        },
        media_type="application/json",
    )

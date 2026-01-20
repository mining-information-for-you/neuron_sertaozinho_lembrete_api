import os
from datetime import datetime
from io import BytesIO

import jwt
from fastapi import APIRouter, File, HTTPException, Query, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse
from google.cloud import storage

from ...db import close_db_connection, get_db_connection
from ..functions.etl_sertaozinho import etl_sertaozinho
from ..functions.utils import require_valid_token

router = APIRouter()

BUCKET_NAME = os.getenv('GCP_BUCKET_NAME', 'cross-mi4u')
CREDENTIALS_PATH = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'mi4u-303100-66ee217229dd.json')


def get_storage_client():
    if os.path.exists(CREDENTIALS_PATH):
        return storage.Client.from_service_account_json(CREDENTIALS_PATH)
    return storage.Client()


@router.get("/test-db")
@require_valid_token
async def test_db_connection(permission_token: str):
    conn = await get_db_connection()
    if conn:
        await close_db_connection(conn)
        return {"message": "Conexão com o banco de dados bem-sucedida!"}
    return {"message": "Falha na conexão com o banco de dados."}


@router.get('/files/')
@require_valid_token
async def get_files(permission_token: str):
    client = get_storage_client()
    bucket = client.get_bucket(BUCKET_NAME)

    files = {
        blob.name: blob.size
        for blob in bucket.list_blobs()
    }

    return JSONResponse(status_code=status.HTTP_200_OK, content=files)


@router.get('/file/{blob_name}')
@require_valid_token
async def get_file(permission_token: str, blob_name: str):
    client = get_storage_client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.get_blob(blob_name)

    if not blob:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'O arquivo {blob_name} não existe!'
        )

    return {
        "filename": blob.name,
        "size": blob.size,
        "content_type": blob.content_type
    }


@router.post('/file/post/')
@require_valid_token
async def post_file(
    permission_token: str,
    mi4u_access_token: str,
    data_hora_enviar: datetime = Query(None),
    file: UploadFile = File(...),
):
    # 🔐 Token MI4U
    try:
        decoded_token = jwt.decode(
            mi4u_access_token,
            options={'verify_signature': False}
        )
        company_id = decoded_token['sub']['company_id']
        user_id = decoded_token['sub']['user_id']

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f'Invalid mi4u_access_token: {e}'
        )

    # 📄 Validação do arquivo
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail='Apenas arquivos PDF são suportados'
        )

    upload_date = datetime.now()
    timestamp = upload_date.strftime('%Y-%m-%d-%H-%M-%S')

    data_hora_enviar = data_hora_enviar or upload_date

    filename = file.filename.replace('.pdf', '')
    destination_blob_name = f'{filename}-{timestamp}.pdf'

    try:
        client = get_storage_client()
        bucket = client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)

        # ☁️ Upload
        blob.upload_from_file(
            file.file,
            content_type='application/pdf'
        )

        # ⬇️ Download para ETL
        file_bytes = blob.download_as_bytes()
        file_io = BytesIO(file_bytes)

        # 🔄 ETL Sertãozinho
        await etl_sertaozinho(
            company_id=company_id,
            user_id=user_id,
            data_hora_enviar=data_hora_enviar,
            data_hora_upload=upload_date,
            filename=filename,
            blob_file=file_io
        )

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                'message': f'Arquivo {destination_blob_name} enviado com sucesso'
            }
        )

    except Exception as e:
        if 'blob' in locals():
            blob.delete()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f'Erro durante processamento: {e}'
        )


@router.get('/download/{blob_name}')
@require_valid_token
async def download_file(permission_token: str, blob_name: str):
    client = get_storage_client()
    bucket = client.get_bucket(BUCKET_NAME)
    blob = bucket.get_blob(blob_name)

    if not blob:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail='Arquivo não encontrado'
        )

    file_io = BytesIO(blob.download_as_bytes())

    return StreamingResponse(
        file_io,
        media_type='application/pdf',
        headers={
            'Content-Disposition': f'attachment; filename={blob_name}'
        }
    )

import os
from functools import wraps

from dotenv import load_dotenv
from fastapi import HTTPException

load_dotenv()

PERMISSION_TOKEN = os.getenv("PERMISSION_TOKEN")


def require_valid_token(func):
    """
    Decorator para checar se o token é válido, comparando com o security token

    token: token de usuário gerado na api https://api.whats.mi4u.app/
    """

    @wraps(func)
    async def wrapper(*args, **kwargs):
        token = kwargs.get("permission_token")

        if not token or token != PERMISSION_TOKEN:
            raise HTTPException(status_code=401, detail="Invalid permission_token")

        return await func(*args, **kwargs)

    return wrapper

import os

import asyncpg
from asyncpg import Connection
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
database = os.getenv("DATABASE")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")


async def get_db_connection() -> Connection:
    try:
        conn = await asyncpg.connect(
            user=user,
            password=password,
            host=host,
            database=database,
            port=port,
        )
        print("Conexão com o banco de dados estabelecida!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar no banco de dados: {e}")


async def close_db_connection(conn):
    await conn.close()
    print("Conexão com o banco de dados fechada.")


if __name__ == "__main__":
    # print(DATABASE_URL)
    pass

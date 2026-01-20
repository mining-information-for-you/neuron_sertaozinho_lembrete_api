import os

from dotenv import load_dotenv
from fastapi.testclient import TestClient

from ...main import app

client = TestClient(app)

load_dotenv()

SECURITY_TOKEN = os.getenv('SECURITY_TOKEN')
USER_TEST_TOKEN = os.getenv('USER_TEST_TOKEN')


def test_get_schedule_success():
    response = client.get(
        '/schedule',
        params={
            'mi4u_access_token': SECURITY_TOKEN,
            'user_token': USER_TEST_TOKEN,
            'unidade_executante': 'Clinica Central',
            'profissional': 'Dr. Silva'
        }
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_get_schedule_invalid_user_token():
    response = client.get(
        '/schedule',
        params={
            'mi4u_access_token': SECURITY_TOKEN,
            'user_token': 'invalid_user_token',
            'id': 1
        }
    )
    assert response.status_code == 400


def test_get_schedule_invalid_access_token():
    response = client.get(
        '/schedule',
        params={
            'mi4u_access_token': 'invalid_token',
            'user_token': USER_TEST_TOKEN,
            'id': 1
        }
    )
    assert response.status_code == 401


def test_get_schedule_company_id():
    response = client.get(
        'schedule',
        params={
            'mi4u_access_token': SECURITY_TOKEN,
            'user_token': USER_TEST_TOKEN,
            'id': 1
        }
    )
    assert response.json()[0].get('empresa_id') == 1

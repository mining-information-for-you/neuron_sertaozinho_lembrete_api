from fastapi import FastAPI

from .api.endpoints.excluir import router as excluir_router
from .api.endpoints.files import router as file_router
from .api.endpoints.report import router as report_router
from .api.endpoints.schedules import router as schedules_router

app = FastAPI()

app.include_router(file_router)
app.include_router(schedules_router)
app.include_router(report_router)
app.include_router(excluir_router)

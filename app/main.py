from fastapi import FastAPI
from .api.endpoints.files import router as file_router
from .api.endpoints.schedules import router as schedules_router
from .api.endpoints.report import router as report_router

app = FastAPI()

app.include_router(file_router)
app.include_router(schedules_router)
app.include_router(report_router)

from fastapi import FastAPI
from .api.endpoints import router as api_router

app = FastAPI()

app.include_router(api_router)
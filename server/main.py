from fastapi import FastAPI
from .api.snapshot import router as snapshot_router

app = FastAPI()
app.include_router(snapshot_router, prefix="/api")


@app.get("/")
async def root():
    return {"message": "API is running"}

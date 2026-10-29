"""FastAPI application with config editor and pipeline endpoints."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config_editor_routes import router as config_editor_router
from api.pipeline_routes import router as pipeline_router

app = FastAPI(
    title="Market Data Pipeline Config Editor",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(config_editor_router)
app.include_router(pipeline_router)


@app.get("/")
async def root():
    return {
        "message": "Config Editor running",
        "editor": "/config-editor/",
        "pipeline": {
            "extraction": "POST /pipeline/run/extraction",
            "transformation": "POST /pipeline/run/transformation",
            "cloud_push": "POST /pipeline/run/cloud-push",
            "ingestion": "POST /pipeline/run/ingestion",
            "full": "POST /pipeline/run/full"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api.main:app", host="0.0.0.0", port=8080, reload=True)
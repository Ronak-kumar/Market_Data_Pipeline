"""FastAPI application with config editor and pipeline endpoints."""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.pipeline_routes import router as pipeline_router
from api.config_routes import router as config_router

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

app.include_router(pipeline_router)
app.include_router(config_router)


@app.get("/")
async def root():
    return {
        "message": "Config Editor running",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint for Docker."""
    return {"status": "healthy", "service": "market-data-pipeline"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8115, reload=False)
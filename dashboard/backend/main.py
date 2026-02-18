"""RetailNova Dashboard — FastAPI Backend."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import overview, kpis, customers, quality, pipeline

app = FastAPI(title="RetailNova Dashboard API", version="1.0.0")

# CORS — allow Vite dev server
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(overview.router)
app.include_router(kpis.router)
app.include_router(customers.router)
app.include_router(quality.router)
app.include_router(pipeline.router)


@app.get("/api/health")
def health():
    return {"status": "ok"}

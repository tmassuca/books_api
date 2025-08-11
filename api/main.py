import sys
import os
from pathlib import Path

# Adicionar o diretório raiz do projeto ao PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
import uvicorn
import logging

# Importar rotas
from api.routes import books, categories, health

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Criar app FastAPI
app = FastAPI(
    title="Books API",
    description="API para consulta de dados de livros - Sistema de recomendação",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir rotas
app.include_router(books.router)
app.include_router(categories.router)
app.include_router(health.router)

@app.get("/")
async def root():
    """Redireciona para a documentação"""
    return RedirectResponse(url="/docs")

@app.get("/api")
async def api_info():
    """Informações da API"""
    return {
        "name": "Books API",
        "version": "1.0.0",
        "description": "API para consulta de dados de livros",
        "endpoints": {
            "books": "/api/v1/books",
            "book_detail": "/api/v1/books/{id}",
            "search": "/api/v1/books/search",
            "categories": "/api/v1/categories",
            "health": "/api/v1/health"
        },
        "documentation": "/docs"
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    )
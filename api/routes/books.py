from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from api.models.schemas import BookDetail, BookSummary, APIResponse
from api.services.data_service import data_service

router = APIRouter(prefix="/api/v1", tags=["books"])

@router.get("/books", response_model=APIResponse)
async def get_all_books(
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limite de resultados"),
    offset: int = Query(0, ge=0, description="Offset para paginação")
):
    """
    Lista todos os livros disponíveis na base de dados
    """
    try:
        result = data_service.get_all_books(limit=limit, offset=offset)
        
        return APIResponse(
            success=True,
            message=f"Retornando {len(result['books'])} livros de {result['total']} total",
            data=result['books'],
            total=result['total']
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.get("/books/search", response_model=APIResponse)
async def search_books(
    title: Optional[str] = Query(None, description="Buscar por título"),
    category: Optional[str] = Query(None, description="Buscar por categoria"),
    min_price: Optional[float] = Query(None, ge=0, description="Preço mínimo"),
    max_price: Optional[float] = Query(None, ge=0, description="Preço máximo"),
    min_rating: Optional[int] = Query(None, ge=0, le=5, description="Rating mínimo"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Limite de resultados"),
    offset: int = Query(0, ge=0, description="Offset para paginação")
):
    """
    Busca livros por título e/ou categoria com filtros adicionais
    """
    try:
        result = data_service.search_books(
            title=title,
            category=category,
            min_price=min_price,
            max_price=max_price,
            min_rating=min_rating,
            limit=limit,
            offset=offset
        )
        
        return APIResponse(
            success=True,
            message=f"Encontrados {len(result['books'])} livros de {result['total']} total",
            data=result['books'],
            total=result['total']
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
    
@router.get("/books/{book_id}", response_model=APIResponse)
async def get_book_by_id(book_id: int):
    """
    Retorna detalhes completos de um livro específico pelo ID
    """
    try:
        book = data_service.get_book_by_id(book_id)
        
        if not book:
            raise HTTPException(status_code=404, detail=f"Livro com ID {book_id} não encontrado")
        
        return APIResponse(
            success=True,
            message=f"Livro encontrado",
            data=[book],
            total=1
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")


from fastapi import APIRouter, HTTPException
from api.models.schemas import Category, APIResponse
from api.services.data_service import data_service

router = APIRouter(prefix="/api/v1", tags=["categories"])

@router.get("/categories", response_model=APIResponse)
async def get_all_categories():
    """
    Lista todas as categorias de livros disponíveis
    """
    try:
        categories = data_service.get_all_categories()
        
        return APIResponse(
            success=True,
            message=f"Retornando {len(categories)} categorias",
            data=categories,
            total=len(categories)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")
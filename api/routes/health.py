from fastapi import APIRouter, HTTPException
from datetime import datetime
from api.models.schemas import HealthCheck
from api.services.data_service import data_service

router = APIRouter(prefix="/api/v1", tags=["health"])

@router.get("/health", response_model=HealthCheck)
async def health_check():
    """
    Verifica status da API e conectividade com os dados
    """
    try:
        # Verificar status dos dados
        data_stats = data_service.get_data_stats()
        
        if 'error' in data_stats:
            status = "unhealthy"
            message = f"Problema com os dados: {data_stats['error']}"
        else:
            status = "healthy"
            message = "API funcionando normalmente"
        
        return HealthCheck(
            status=status,
            message=message,
            timestamp=datetime.utcnow().isoformat(),
            data_status=data_stats
        )
    except Exception as e:
        raise HTTPException(
            status_code=503, 
            detail=f"Serviço indisponível: {str(e)}"
        )
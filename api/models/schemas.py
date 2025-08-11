from pydantic import BaseModel, Field
from typing import Optional, List
from enum import Enum

class PriceRange(str, Enum):
    budget = "Budget"
    mid_range = "Mid-range"
    premium = "Premium"
    luxury = "Luxury"

class BookBase(BaseModel):
    title: str = Field(..., description="Título do livro")
    price: float = Field(..., ge=0, description="Preço do livro")
    category: str = Field(..., description="Categoria do livro")
    rating: int = Field(..., ge=0, le=5, description="Rating do livro (0-5)")

class BookDetail(BookBase):
    id: int = Field(..., description="ID único do livro")
    availability: int = Field(..., ge=0, description="Quantidade em estoque")
    description: str = Field("", description="Descrição do livro")
    upc: str = Field("", description="Código UPC")
    product_type: str = Field("", description="Tipo do produto")
    tax: float = Field(0.0, ge=0, description="Taxa")
    num_reviews: int = Field(0, ge=0, description="Número de reviews")
    url: str = Field("", description="URL do livro")
    price_range: Optional[PriceRange] = Field(None, description="Faixa de preço")
    popularity_score: float = Field(0.0, ge=0, description="Score de popularidade")

class BookSummary(BookBase):
    id: int = Field(..., description="ID único do livro")
    availability: int = Field(..., ge=0, description="Quantidade em estoque")

class Category(BaseModel):
    category: str = Field(..., description="Nome da categoria")
    total_books: int = Field(..., ge=0, description="Total de livros na categoria")
    avg_price: float = Field(..., ge=0, description="Preço médio")
    min_price: float = Field(..., ge=0, description="Preço mínimo")
    max_price: float = Field(..., ge=0, description="Preço máximo")
    avg_rating: float = Field(..., ge=0, le=5, description="Rating médio")

class SearchFilters(BaseModel):
    title: Optional[str] = Field(None, description="Filtro por título")
    category: Optional[str] = Field(None, description="Filtro por categoria")
    min_price: Optional[float] = Field(None, ge=0, description="Preço mínimo")
    max_price: Optional[float] = Field(None, ge=0, description="Preço máximo")
    min_rating: Optional[int] = Field(None, ge=0, le=5, description="Rating mínimo")

class HealthCheck(BaseModel):
    status: str = Field(..., description="Status da API")
    message: str = Field(..., description="Mensagem de status")
    timestamp: str = Field(..., description="Timestamp da verificação")
    data_status: dict = Field(..., description="Status dos dados")

class APIResponse(BaseModel):
    success: bool = Field(..., description="Indicador de sucesso")
    message: str = Field(..., description="Mensagem da resposta")
    data: Optional[List] = Field(None, description="Dados da resposta")
    total: Optional[int] = Field(None, description="Total de registros")
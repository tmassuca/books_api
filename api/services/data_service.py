import pandas as pd
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
import os

logger = logging.getLogger(__name__)

class DataService:
    def __init__(self):
        self.books_path = '../data/processed/books_processed.csv'
        self.categories_path = '../data/processed/categories.csv'
        self._books_df = None
        self._categories_df = None
    
    def _load_books(self) -> pd.DataFrame:
        """Carrega dados dos livros"""
        if self._books_df is None:
            if not os.path.exists(self.books_path):
                raise FileNotFoundError(f"Arquivo de dados não encontrado: {self.books_path}")
            self._books_df = pd.read_csv(self.books_path)
        return self._books_df
    
    def _load_categories(self) -> pd.DataFrame:
        """Carrega dados das categorias"""
        if self._categories_df is None:
            if not os.path.exists(self.categories_path):
                raise FileNotFoundError(f"Arquivo de categorias não encontrado: {self.categories_path}")
            self._categories_df = pd.read_csv(self.categories_path)
        return self._categories_df
    
    def get_all_books(self, limit: Optional[int] = None, offset: int = 0) -> Dict[str, Any]:
        """Retorna todos os livros com paginação"""
        df = self._load_books()
        
        total = len(df)
        
        # Aplicar paginação
        start_idx = offset
        end_idx = offset + limit if limit else len(df)
        
        books = df.iloc[start_idx:end_idx].to_dict('records')
        
        return {
            'books': books,
            'total': total,
            'offset': offset,
            'limit': limit or total
        }
    
    def get_book_by_id(self, book_id: int) -> Optional[Dict[str, Any]]:
        """Retorna um livro específico pelo ID"""
        df = self._load_books()
        book = df[df['id'] == book_id]
        
        if book.empty:
            return None
        
        return book.iloc[0].to_dict()
    
    def search_books(self, title: Optional[str] = None, 
                    category: Optional[str] = None,
                    min_price: Optional[float] = None,
                    max_price: Optional[float] = None,
                    min_rating: Optional[int] = None,
                    limit: Optional[int] = None,
                    offset: int = 0) -> Dict[str, Any]:
        """Busca livros com filtros"""
        df = self._load_books()
        
        # Aplicar filtros
        if title:
            df = df[df['title'].str.contains(title, case=False, na=False)]
        
        if category:
            df = df[df['category'].str.contains(category, case=False, na=False)]
        
        if min_price is not None:
            df = df[df['price'] >= min_price]
        
        if max_price is not None:
            df = df[df['price'] <= max_price]
        
        if min_rating is not None:
            df = df[df['rating'] >= min_rating]
        
        total = len(df)
        
        # Aplicar paginação
        start_idx = offset
        end_idx = offset + limit if limit else len(df)
        
        books = df.iloc[start_idx:end_idx].to_dict('records')
        
        return {
            'books': books,
            'total': total,
            'offset': offset,
            'limit': limit or total,
            'filters_applied': {
                'title': title,
                'category': category,
                'min_price': min_price,
                'max_price': max_price,
                'min_rating': min_rating
            }
        }
    
    def get_all_categories(self) -> List[Dict[str, Any]]:
        """Retorna todas as categorias"""
        df = self._load_categories()
        return df.to_dict('records')
    
    def get_data_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas dos dados"""
        try:
            books_df = self._load_books()
            categories_df = self._load_categories()
            
            return {
                'total_books': len(books_df),
                'total_categories': len(categories_df),
                'price_range': {
                    'min': float(books_df['price'].min()),
                    'max': float(books_df['price'].max()),
                    'avg': float(books_df['price'].mean())
                },
                'rating_distribution': books_df['rating'].value_counts().to_dict(),
                'books_by_category': books_df['category'].value_counts().head(10).to_dict(),
                'data_freshness': {
                    'books_file_exists': os.path.exists(self.books_path),
                    'categories_file_exists': os.path.exists(self.categories_path)
                }
            }
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {
                'error': str(e),
                'data_available': False
            }

# Instância global do serviço
data_service = DataService()
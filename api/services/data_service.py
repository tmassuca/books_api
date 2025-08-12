import pandas as pd
import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
import os
import numpy as np

logger = logging.getLogger(__name__)

class DataService:
    def __init__(self):
        # Testar múltiplos caminhos possíveis
        possible_paths = [
            'data/processed/',           # Raiz do projeto
            '../data/processed/',        # Um nível acima
            './data/processed/',         # Diretório atual
            '../../data/processed/',     # Dois níveis acima
            '/opt/render/project/src/data/processed/',  # Caminho absoluto Render
        ]
        
        self.books_path = None
        self.categories_path = None
        
        # Encontrar caminho que funciona
        for base_path in possible_paths:
            books_file = base_path + 'books_processed.csv'
            categories_file = base_path + 'categories.csv'
            
            if os.path.exists(books_file) and os.path.exists(categories_file):
                self.books_path = books_file
                self.categories_path = categories_file
                logger.info(f"✅ Arquivos encontrados em: {base_path}")
                break
        
        if not self.books_path:
            # Log dos caminhos testados para debug
            logger.error("❌ Arquivos não encontrados. Caminhos testados:")
            for path in possible_paths:
                logger.error(f"   - {path} (existe: {os.path.exists(path)})")
            logger.error(f"   - Diretório atual: {os.getcwd()}")
            
            # Usar caminhos padrão como fallback
            self.books_path = '../data/processed/books_processed.csv'
            self.categories_path = '../data/processed/categories.csv'
        
        self._books_df = None
        self._categories_df = None
    
    def _clean_for_json(self, data) -> Any:
        """Limpa dados para serem seguros para JSON"""
        if isinstance(data, dict):
            return {k: self._clean_for_json(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._clean_for_json(item) for item in data]
        elif isinstance(data, (np.floating, float)):
            if np.isnan(data) or np.isinf(data):
                return None
            return float(data)
        elif isinstance(data, (np.integer, int)):
            return int(data)
        elif isinstance(data, np.ndarray):
            return data.tolist()
        elif pd.isna(data):
            return None
        else:
            return data
    
    def _load_books(self) -> pd.DataFrame:
        """Carrega dados dos livros"""
        if self._books_df is None:
            if not os.path.exists(self.books_path):
                raise FileNotFoundError(f"Arquivo de dados não encontrado: {self.books_path}. Pasta atual: {os.getcwd()}")
            
            self._books_df = pd.read_csv(self.books_path)
            
            # Limpar dados problemáticos
            # Substituir NaN por valores padrão
            numeric_columns = ['price', 'availability', 'rating', 'tax', 'num_reviews', 'popularity_score']
            for col in numeric_columns:
                if col in self._books_df.columns:
                    self._books_df[col] = pd.to_numeric(self._books_df[col], errors='coerce')
                    self._books_df[col] = self._books_df[col].fillna(0)
            
            # Substituir strings NaN
            string_columns = ['title', 'description', 'category', 'upc', 'product_type', 'url', 'price_range']
            for col in string_columns:
                if col in self._books_df.columns:
                    self._books_df[col] = self._books_df[col].fillna('')
            
        return self._books_df
    
    def _load_categories(self) -> pd.DataFrame:
        """Carrega dados das categorias"""
        if self._categories_df is None:
            if not os.path.exists(self.categories_path):
                raise FileNotFoundError(f"Arquivo de categorias não encontrado: {self.categories_path}")
            
            self._categories_df = pd.read_csv(self.categories_path)
            
            # Limpar dados numéricos
            numeric_columns = ['total_books', 'avg_price', 'min_price', 'max_price', 'avg_rating']
            for col in numeric_columns:
                if col in self._categories_df.columns:
                    self._categories_df[col] = pd.to_numeric(self._categories_df[col], errors='coerce')
                    self._categories_df[col] = self._categories_df[col].fillna(0)
            
            # Limpar strings
            if 'category' in self._categories_df.columns:
                self._categories_df['category'] = self._categories_df['category'].fillna('')
                
        return self._categories_df
    
    def get_all_books(self, limit: Optional[int] = None, offset: int = 0) -> Dict[str, Any]:
        """Retorna todos os livros com paginação"""
        df = self._load_books()
        
        total = len(df)
        
        # Aplicar paginação
        start_idx = offset
        end_idx = offset + limit if limit else len(df)
        
        books = df.iloc[start_idx:end_idx].to_dict('records')
        
        # Limpar dados para JSON
        books_clean = self._clean_for_json(books)
        
        return {
            'books': books_clean,
            'total': int(total),
            'offset': int(offset),
            'limit': int(limit or total)
        }
    
    def get_book_by_id(self, book_id: int) -> Optional[Dict[str, Any]]:
        """Retorna um livro específico pelo ID"""
        df = self._load_books()
        book = df[df['id'] == book_id]
        
        if book.empty:
            return None
        
        book_data = book.iloc[0].to_dict()
        return self._clean_for_json(book_data)
    
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
        
        # Limpar dados para JSON
        books_clean = self._clean_for_json(books)
        filters_clean = self._clean_for_json({
            'title': title,
            'category': category,
            'min_price': min_price,
            'max_price': max_price,
            'min_rating': min_rating
        })
        
        return {
            'books': books_clean,
            'total': int(total),
            'offset': int(offset),
            'limit': int(limit or total),
            'filters_applied': filters_clean
        }
    
    def get_all_categories(self) -> List[Dict[str, Any]]:
        """Retorna todas as categorias"""
        df = self._load_categories()
        categories = df.to_dict('records')
        return self._clean_for_json(categories)
    
    def get_data_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas dos dados"""
        try:
            books_df = self._load_books()
            categories_df = self._load_categories()
            
            # Calcular estatísticas de forma segura
            price_stats = {
                'min': float(books_df['price'].min()) if not books_df['price'].empty else 0.0,
                'max': float(books_df['price'].max()) if not books_df['price'].empty else 0.0,
                'avg': float(books_df['price'].mean()) if not books_df['price'].empty else 0.0
            }
            
            # Verificar se há valores inválidos
            for key, value in price_stats.items():
                if np.isnan(value) or np.isinf(value):
                    price_stats[key] = 0.0
            
            # Rating distribution de forma segura
            rating_dist = books_df['rating'].value_counts().to_dict()
            rating_dist_clean = {str(k): int(v) for k, v in rating_dist.items() if not (np.isnan(k) or np.isinf(k))}
            
            # Category distribution de forma segura
            category_dist = books_df['category'].value_counts().head(10).to_dict()
            category_dist_clean = {str(k): int(v) for k, v in category_dist.items() if pd.notna(k)}
            
            stats = {
                'total_books': int(len(books_df)),
                'total_categories': int(len(categories_df)),
                'price_range': price_stats,
                'rating_distribution': rating_dist_clean,
                'books_by_category': category_dist_clean,
                'data_freshness': {
                    'books_file_exists': bool(os.path.exists(self.books_path)),
                    'categories_file_exists': bool(os.path.exists(self.categories_path))
                }
            }
            
            return self._clean_for_json(stats)
            
        except Exception as e:
            logger.error(f"Erro ao obter estatísticas: {e}")
            return {
                'error': str(e),
                'data_available': False
            }

# Instância global do serviço
data_service = DataService()
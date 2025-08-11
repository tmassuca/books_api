import pandas as pd
import logging
from typing import Dict, List
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, raw_data_path: str = 'data/raw/books_data.csv'):
        self.raw_data_path = raw_data_path
    
    def load_raw_data(self) -> pd.DataFrame:
        """Carrega dados brutos"""
        logger.info(f"Carregando dados de {self.raw_data_path}")
        return pd.read_csv(self.raw_data_path)
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Limpa e normaliza os dados"""
        logger.info("Iniciando limpeza dos dados...")
        
        # Remover duplicatas
        df = df.drop_duplicates(subset=['title', 'upc'])
        
        # Limpar campos de texto
        df['title'] = df['title'].str.strip()
        df['description'] = df['description'].fillna('').str.strip()
        df['category'] = df['category'].str.strip()
        
        # Normalizar categorias
        df['category'] = df['category'].str.title()
        
        # Garantir tipos corretos
        df['price'] = pd.to_numeric(df['price'], errors='coerce')
        df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
        df['availability'] = pd.to_numeric(df['availability'], errors='coerce')
        df['num_reviews'] = pd.to_numeric(df['num_reviews'], errors='coerce')
        
        # Preencher valores nulos
        df['rating'] = df['rating'].fillna(0)
        df['availability'] = df['availability'].fillna(0)
        df['num_reviews'] = df['num_reviews'].fillna(0)
        df['description'] = df['description'].fillna('')
        
        # Adicionar campos calculados
        df['price_range'] = pd.cut(df['price'], 
                                 bins=[0, 20, 40, 60, float('inf')], 
                                 labels=['Budget', 'Mid-range', 'Premium', 'Luxury'])
        
        df['popularity_score'] = (df['rating'] * 2 + df['num_reviews'] * 0.1).round(2)
        
        logger.info(f"Limpeza concluída. {len(df)} registros limpos.")
        return df
    
    def create_categories_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cria dataset de categorias"""
        categories = df.groupby('category').agg({
            'id': 'count',
            'price': ['mean', 'min', 'max'],
            'rating': 'mean'
        }).round(2)
        
        categories.columns = ['total_books', 'avg_price', 'min_price', 'max_price', 'avg_rating']
        categories = categories.reset_index()
        
        return categories
    
    def save_processed_data(self, df: pd.DataFrame, categories_df: pd.DataFrame):
        """Salva dados processados"""
        os.makedirs('data/processed', exist_ok=True)
        
        # Salvar livros processados
        books_path = 'data/processed/books_processed.csv'
        df.to_csv(books_path, index=False)
        logger.info(f"Dados de livros salvos em {books_path}")
        
        # Salvar categorias
        categories_path = 'data/processed/categories.csv'
        categories_df.to_csv(categories_path, index=False)
        logger.info(f"Dados de categorias salvos em {categories_path}")
    
    def process(self):
        """Método principal de processamento"""
        try:
            # Carregar dados brutos
            raw_df = self.load_raw_data()
            
            # Limpar dados
            clean_df = self.clean_data(raw_df)
            
            # Criar dados de categorias
            categories_df = self.create_categories_data(clean_df)
            
            # Salvar dados processados
            self.save_processed_data(clean_df, categories_df)
            
            # Estatísticas finais
            logger.info("=== ESTATÍSTICAS FINAIS ===")
            logger.info(f"Total de livros: {len(clean_df)}")
            logger.info(f"Total de categorias: {len(categories_df)}")
            logger.info(f"Preço médio: £{clean_df['price'].mean():.2f}")
            logger.info(f"Rating médio: {clean_df['rating'].mean():.2f}")
            
        except Exception as e:
            logger.error(f"Erro durante processamento: {e}")
            raise

def main():
    processor = DataProcessor()
    processor.process()

if __name__ == "__main__":
    main()
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import logging
from urllib.parse import urljoin, urlparse
import os
from typing import List, Dict, Optional
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BooksScraper:
    def __init__(self, base_url: str = "https://books.toscrape.com/"):
        self.base_url = base_url
        self.session = requests.Session()
        self.books_data = []
        
    def get_page(self, url: str) -> Optional[BeautifulSoup]:
        """Faz requisição para uma página e retorna BeautifulSoup object"""
        try:
            response = self.session.get(url)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Erro ao acessar {url}: {e}")
            return None
    
    def extract_rating(self, rating_class: str) -> int:
        """Converte rating de texto para número"""
        rating_map = {
            'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5
        }
        for word, num in rating_map.items():
            if word in rating_class:
                return num
        return 0
    
    def clean_price(self, price_text: str) -> float:
        """Limpa e converte preço para float"""
        return float(re.sub(r'[£,]', '', price_text))
    
    def scrape_book_details(self, book_url: str) -> Dict:
        """Extrai detalhes completos de um livro"""
        soup = self.get_page(book_url)
        if not soup:
            return {}
        
        try:
            # Informações básicas
            title = soup.select_one('h1').text.strip()
            
            # Preço
            price = soup.select_one('p.price_color').text.strip()
            price_value = self.clean_price(price)
            
            # Disponibilidade
            availability = soup.select_one('p.availability').text.strip()
            in_stock = int(re.search(r'\d+', availability).group()) if re.search(r'\d+', availability) else 0
            
            # Rating
            rating_element = soup.select_one('p.star-rating')
            rating = self.extract_rating(rating_element.get('class')[1]) if rating_element else 0
            
            # Descrição
            description_element = soup.select_one('#product_description + p')
            description = description_element.text.strip() if description_element else ""
            
            # Informações da tabela
            table_rows = soup.select('table.table-striped tr')
            product_info = {}
            for row in table_rows:
                key = row.select_one('td').text.strip()
                value = row.select_one('td:nth-child(2)').text.strip()
                product_info[key] = value
            
            # Categoria (breadcrumb)
            breadcrumb = soup.select('ul.breadcrumb li')
            category = breadcrumb[2].text.strip() if len(breadcrumb) > 2 else "Unknown"
            
            return {
                'title': title,
                'price': price_value,
                'availability': in_stock,
                'rating': rating,
                'description': description,
                'category': category,
                'upc': product_info.get('UPC', ''),
                'product_type': product_info.get('Product Type', ''),
                'tax': self.clean_price(product_info.get('Tax', '£0.00')),
                'num_reviews': int(product_info.get('Number of reviews', '0')),
                'url': book_url
            }
        except Exception as e:
            logger.error(f"Erro ao processar detalhes do livro {book_url}: {e}")
            return {}
    
    def scrape_catalogue_page(self, page_url: str) -> List[str]:
        """Extrai URLs dos livros de uma página do catálogo"""
        soup = self.get_page(page_url)
        if not soup:
            return []
        
        book_links = []
        articles = soup.select('article.product_pod')
        
        for article in articles:
            link_element = article.select_one('h3 a')
            if link_element:
                relative_url = link_element.get('href', '')
                logger.debug(f"URL relativa encontrada: {relative_url}")
                
                # Corrigir a construção da URL
                if relative_url.startswith('../../../'):
                    # Remove os '../../../' e adiciona 'catalogue/'
                    clean_url = relative_url.replace('../../../', '')
                    book_url = urljoin(self.base_url, f'catalogue/{clean_url}')
                elif relative_url.startswith('catalogue/'):
                    # Já está no formato correto
                    book_url = urljoin(self.base_url, relative_url)
                else:
                    # Adiciona catalogue/ se não estiver presente
                    book_url = urljoin(self.base_url, f'catalogue/{relative_url}')
                
                logger.debug(f"URL final construída: {book_url}")
                book_links.append(book_url)
        logger.info(f"URLs processadas: {len(book_links)}")
        return book_links
    
    def get_all_catalogue_pages(self) -> List[str]:
        """Obtém URLs de todas as páginas do catálogo"""
        pages = [self.base_url]
        current_page = 1
        
        while True:
        #while current_page<=3: # Limite para teste - apagar em produção
            page_url = f"{self.base_url}catalogue/page-{current_page + 1}.html"
            soup = self.get_page(page_url)
            
            if not soup or not soup.select('article.product_pod'):
                break
                
            pages.append(page_url)
            current_page += 1
            logger.info(f"Encontrada página {current_page}")
        
        logger.info(f"Total de páginas encontradas: {len(pages)}")
        return pages
    
    def scrape_all_books(self) -> pd.DataFrame:
        """Método principal para fazer scraping de todos os livros"""
        logger.info("Iniciando scraping de todos os livros...")
        
        # Obter todas as páginas do catálogo
        catalogue_pages = self.get_all_catalogue_pages()
        
        all_book_urls = []
        for page_url in catalogue_pages:
            logger.info(f"Processando página: {page_url}")
            book_urls = self.scrape_catalogue_page(page_url)
            all_book_urls.extend(book_urls)
            time.sleep(1)  # Rate limiting
        
        logger.info(f"Total de livros encontrados: {len(all_book_urls)}")
        
        # Processar cada livro
        books_data = []
        for i, book_url in enumerate(all_book_urls, 1):
            logger.info(f"Processando livro {i}/{len(all_book_urls)}: {book_url}")
            book_data = self.scrape_book_details(book_url)
            
            if book_data:
                book_data['id'] = i
                books_data.append(book_data)
            
            time.sleep(0.5)  # Rate limiting
            
            # Checkpoint a cada 50 livros
            if i % 50 == 0:
                df_checkpoint = pd.DataFrame(books_data)
                df_checkpoint.to_csv(f'data/checkpoint_{i}.csv', index=False)
                logger.info(f"Checkpoint salvo: {i} livros processados")
        
        df = pd.DataFrame(books_data)
        logger.info(f"Scraping concluído! {len(df)} livros coletados.")
        return df
    
    def save_data(self, df: pd.DataFrame, filename: str = 'books_data.csv'):
        """Salva os dados em CSV"""
        os.makedirs('data/raw', exist_ok=True)
        filepath = f'data/raw/{filename}'
        #df.to_csv(filepath, index=False)
        df.to_csv(os.path.join('data', 'raw', filename), index=False)
        logger.info(f"Dados salvos em {filepath}")

def main():
    scraper = BooksScraper()
    
    try:
        # Fazer scraping de todos os livros
        books_df = scraper.scrape_all_books()
        
        # Salvar dados
        scraper.save_data(books_df)
        
        # Estatísticas básicas
        logger.info(f"Estatísticas:")
        logger.info(f"Total de livros: {len(books_df)}")
        logger.info(f"Categorias únicas: {books_df['category'].nunique()}")
        logger.info(f"Preço médio: £{books_df['price'].mean():.2f}")
        logger.info(f"Rating médio: {books_df['rating'].mean():.2f}")
        
    except Exception as e:
        logger.error(f"Erro durante o scraping: {e}")
        raise

if __name__ == "__main__":
    main()
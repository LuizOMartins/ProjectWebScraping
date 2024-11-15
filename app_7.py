import requests
from bs4 import BeautifulSoup
import time
import sqlite3
import pandas as pd
import asyncio
from telegram import Bot
import os
from dotenv import load_dotenv
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do bot do Telegram
TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

if not TOKEN or not CHAT_ID:
    raise ValueError("TOKEN ou CHAT_ID não configurados no arquivo .env")

bot = Bot(token=TOKEN)


def fetch_page():
    url = 'https://www.mercadolivre.com.br/apple-iphone-16-pro-1-tb-titnio-preto-distribuidor-autorizado/p/MLB1040287851#polycard_client=search-nordic&wid=MLB5054621110&sid=search&searchVariation=MLB1040287851&position=6&search_layout=stack&type=product&tracking_id=92c2ddf6-f70e-475b-b41e-fe2742459774'
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Erro ao buscar página: {e}")
        return None


def parse_page(html):
    try:
        soup = BeautifulSoup(html, 'html.parser')
        product_name = soup.find('h1', class_='ui-pdp-title').get_text(strip=True)
        prices = soup.find_all('span', class_='andes-money-amount__fraction')
        old_price = int(prices[0].get_text(strip=True).replace('.', ''))
        new_price = int(prices[1].get_text(strip=True).replace('.', ''))
        installment_price = int(prices[2].get_text(strip=True).replace('.', ''))

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

        return {
            'product_name': product_name,
            'old_price': old_price,
            'new_price': new_price,
            'installment_price': installment_price,
            'timestamp': timestamp
        }
    except (AttributeError, IndexError, ValueError) as e:
        logging.error(f"Erro ao parsear a página: {e}")
        return None


def create_connection(db_name='iphone_prices.db'):
    """Cria uma conexão com o banco de dados SQLite."""
    return sqlite3.connect(db_name)


def setup_database(conn):
    """Cria a tabela de preços se ela não existir."""
    with conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_name TEXT,
                old_price INTEGER,
                new_price INTEGER,
                installment_price INTEGER,
                timestamp TEXT
            )
        ''')


def save_to_database(conn, data):
    """Salva uma linha de dados no banco de dados SQLite usando pandas."""
    if data:
        df = pd.DataFrame([data])  # Converte o dicionário em um DataFrame de uma linha
        df.to_sql('prices', conn, if_exists='append', index=False)  # Salva no banco de dados


def get_max_price(conn):
    """Consulta o maior preço registrado até o momento."""
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(new_price), timestamp FROM prices")
    result = cursor.fetchone()
    if result and result[0] is not None:
        return result[0], result[1]
    return None, None


async def send_telegram_message(text):
    """Envia uma mensagem para o Telegram."""
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text)
    except Exception as e:
        logging.error(f"Erro ao enviar mensagem para o Telegram: {e}")


async def main():
    conn = create_connection()
    setup_database(conn)

    try:
        while True:
            # Faz a requisição e parseia a página
            page_content = fetch_page()
            if not page_content:
                await asyncio.sleep(60)
                continue

            product_info = parse_page(page_content)
            if not product_info:
                await asyncio.sleep(60)
                continue

            current_price = product_info['new_price']
            
            # Obtém o maior preço já salvo
            max_price, max_price_timestamp = get_max_price(conn)
            
            # Comparação de preços
            if max_price is None or current_price > max_price:
                message = f"Novo preço maior detectado: {current_price}"
                logging.info(message)
                await send_telegram_message(message)
            else:
                message = f"O maior preço registrado é {max_price} em {max_price_timestamp}"
                logging.info(message)
                await send_telegram_message(message)

            # Salva os dados no banco de dados SQLite
            save_to_database(conn, product_info)
            logging.info(f"Dados salvos no banco: {product_info}")
            
            # Aguarda 60 segundos antes da próxima execução
            await asyncio.sleep(60)

    except KeyboardInterrupt:
        logging.info("Parando a execução...")
    finally:
        conn.close()


# Executa o loop assíncrono
if __name__ == "__main__":
    asyncio.run(main())

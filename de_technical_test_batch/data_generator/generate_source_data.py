import csv
import logging
import random
from datetime import datetime, timedelta
from faker import Faker
from pathlib import Path
from typing import List, Dict, Any, Tuple, TypedDict

SCRIPT_PATH = Path(__file__).resolve()
GENERATOR_DIR = SCRIPT_PATH.parent
ROOT_DIR = GENERATOR_DIR.parent
OUTPUT_DIR = ROOT_DIR / "data"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

NUM_CUSTOMERS = 1000
NUM_PRODUCTS = 200
NUM_CAMPAIGNS = 50
NUM_TRANSACTIONS = 1500
AVG_ITEMS_PER_TX = 3
FAKER_LOCALE = 'id_ID'

fake = Faker(FAKER_LOCALE)

class ProductData(TypedDict):
    product_id: int
    name: str
    category: str
    price: int

def ensure_output_dir(directory: Path):
    try:
        directory.mkdir(parents=True, exist_ok=True)
        logging.info(f"Output directory '{directory}' is ready.")
    except OSError as e:
        logging.error(f"Failed to create directory {directory}: {e}")
        raise

def write_to_csv(filepath: Path, header: List[str], data: List[List[Any]]):
    logging.info(f"Writing {len(data)} records to {filepath.name}...")
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(data)
    except IOError as e:
        logging.error(f"Failed to write to {filepath}: {e}")
        raise

def generate_customers(num_records: int) -> List[int]:
    header = ['customer_id', 'name', 'email', 'city', 'signup_date']
    data = []
    customer_ids = []
    
    for i in range(1, num_records + 1):
        customer_ids.append(i)
        data.append([
            i,
            fake.name(),
            fake.email(),
            fake.city(),
            fake.date_time_between(start_date='-3y', end_date='now')
        ])
        
    write_to_csv(OUTPUT_DIR / "customers.csv", header, data)
    return customer_ids

def generate_products(num_records: int) -> Dict[int, ProductData]:
    header = ['product_id', 'product_name', 'category', 'price']
    data = []
    product_map: Dict[int, ProductData] = {}
    categories = ['Elektronik', 'Fashion', 'Perabotan', 'Makanan', 'Olahraga']

    for i in range(1, num_records + 1):
        price = random.randint(5000, 2000000) * 10
        category = random.choice(categories)
        
        product_info: ProductData = {
            'product_id': i,
            'name': f"{fake.word().capitalize()} {category} {fake.word()}",
            'category': category,
            'price': price
        }
        
        product_map[i] = product_info
        data.append([i, product_info['name'], category, price])
        
    write_to_csv(OUTPUT_DIR / "products.csv", header, data)
    return product_map

def generate_marketing_campaigns(num_records: int):
    header = ['campaign_id', 'campaign_name', 'start_date', 'end_date', 'channel']
    data = []
    channels = ['Facebook Ads', 'Google Ads', 'TikTok', 'Email', 'SEO']

    for i in range(1, num_records + 1):
        start_date = fake.date_time_between(start_date='-1y', end_date='now')
        end_date = start_date + timedelta(days=random.randint(14, 60))
        data.append([
            i,
            f"{fake.word().capitalize()} Sale {start_date.year}",
            start_date,
            end_date,
            random.choice(channels)
        ])
        
    write_to_csv(OUTPUT_DIR / "marketing_campaigns.csv", header, data)

def generate_transactions_and_items(
    num_transactions: int,
    customer_ids: List[int],
    product_map: Dict[int, ProductData]
):
    tx_header = ['transaction_id', 'customer_id', 'transaction_date', 'total_amount']
    item_header = ['transaction_item_id', 'transaction_id', 'product_id', 'quantity', 'price']
    
    tx_data = []
    item_data = []
    
    product_ids = list(product_map.keys())
    transaction_item_id_counter = 1

    for tx_id in range(1, num_transactions + 1):
        
        customer_id = random.choice(customer_ids)
        transaction_date = fake.date_time_between(start_date='-2y', end_date='now')
        
        num_items_in_this_tx = max(1, random.randint(1, AVG_ITEMS_PER_TX * 2))
        current_transaction_total = 0
        
        for _ in range(num_items_in_this_tx):
            product_id = random.choice(product_ids)
            product_info = product_map[product_id]
            
            quantity = random.randint(1, 3)
            price_at_sale = product_info['price'] 
            
            line_total = quantity * price_at_sale
            current_transaction_total += line_total
            
            item_data.append([
                transaction_item_id_counter,
                tx_id,
                product_id,
                quantity,
                price_at_sale
            ])
            transaction_item_id_counter += 1
            
        tx_data.append([
            tx_id,
            customer_id,
            transaction_date,
            current_transaction_total
        ])

    write_to_csv(OUTPUT_DIR / "transactions.csv", tx_header, tx_data)
    write_to_csv(OUTPUT_DIR / "transaction_items.csv", item_header, item_data)

def main():
    logging.info("--- Starting Data Generation Process ---")
    
    ensure_output_dir(OUTPUT_DIR)
    
    logging.info("Step 1: Generating parent tables...")
    customer_ids = generate_customers(NUM_CUSTOMERS)
    product_map = generate_products(NUM_PRODUCTS)
    generate_marketing_campaigns(NUM_CAMPAIGNS)
    
    logging.info("Step 2: Generating child tables (transactions and items)...")
    generate_transactions_and_items(
        NUM_TRANSACTIONS,
        customer_ids,
        product_map
    )
    
    logging.info("--- Data Generation Complete ---")
    logging.info(f"All files saved in '{OUTPUT_DIR.relative_to(ROOT_DIR)}' directory.")

if __name__ == "__main__":
    main()
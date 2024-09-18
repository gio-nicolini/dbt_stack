import pandas as pd
import random
import datetime
from sqlalchemy import create_engine
from faker import Faker
from dotenv import load_dotenv
from os import getenv

from pandas import DataFrame

# Init Faker
fake = Faker()

# Generate fake client data
def generate_customers(n: int) -> DataFrame:
    customers: list[dict] = []
    
    for _ in range(n):
        customer: dict = {
            'customer_id': fake.uuid4(),
            'name': fake.name(),
            'email': fake.email(),
            'address': fake.address(),
            'city': fake.city(),
            'country': fake.country(),
            'phone_number': fake.phone_number(),
            'birthdate': fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')
        }
        
        customers.append(customer)
    
    return pd.DataFrame(customers)

def generate_products(n: int) -> DataFrame:
    
    categories: list[str] = ['Electronics', 'Clothing', 'Home', 'Sports', 'Books']
    products: list[dict] = []
    
    for _ in range(n):
        product: dict = {
            'product_id': fake.uuid4(),
            'product_name': fake.word().capitalize(),
            'category': random.choice(categories),
            'price': round(random.uniform(10.0, 10000.0), 2),
            'stock_quantity': random.randint(1 ,1000)
        }
        products.append(product)
    
    return pd.DataFrame(products)

def generate_purchases(customers_df, products_df, num_purchases):
    purchases = []
    start_date = datetime.date(datetime.datetime.now().year, 1, 1)
    end_date = datetime.date(datetime.datetime.now().year, 12, 31)
    delta = end_date - start_date
    for _ in range(num_purchases):
        purchase = {
            'purchase_id': fake.uuid4(),
            'customer_id': random.choice(customers_df['customer_id']),
            'product_id': random.choice(products_df['product_id']),
            'purchase_date': start_date + datetime.timedelta(days=random.randint(0, delta.days)),
            'quantity': random.randint(1, 5),
            'total_amount': round(random.uniform(10.0, 500.0), 2)
        }
        purchases.append(purchase)
    return pd.DataFrame(purchases)

if __name__ == '__main__':
    
    
    load_dotenv()
    
    # Load db credentials
    DATABASE_TYPE = getenv('DATABASE_TYPE')
    DBAPI = getenv('DBAPI')
    USERNAME = getenv('USER')
    PASSWORD = getenv('PASSWORD')
    HOST = getenv('HOST')
    PORT = getenv('PORT')
    DATABASE = getenv('DATABASE')
    
    # Conn url
    DATABASE_URL = f'{DATABASE_TYPE}+{DBAPI}://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}'
    
    # Create conn
    engine = create_engine(DATABASE_URL)
    
    # Generate data
    n_customers: int = 2_000
    n_products: int = 20
    n_purchases: int = 20_000
    
    customers_df: DataFrame = generate_customers(n_customers)
    products_df: DataFrame = generate_products(n_products)
    purchases_df: DataFrame = generate_purchases(customers_df, products_df, n_purchases)
    # Insert data
    try:
        with engine.connect() as conn:
            
            customers_df.to_sql('customers_raw', conn, index=False, if_exists='append')
            products_df.to_sql('products_raw', conn, index=False, if_exists='append')
            purchases_df.to_sql('purchases_raw', conn, index=False, if_exists='append')
            
        conn.close()
        
    except Exception as e:
        print(f"Conn failed. {e}")
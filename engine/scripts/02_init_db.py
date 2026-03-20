import os
import pandas as pd
import sqlalchemy as sql
from sqlalchemy import Column, VARCHAR, Float, DateTime, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'my_postgres_db' 
DB_PORT = '5432'
DB_NAME = os.getenv('POSTGRES_DB')

print(f"Connecting to Database: {DB_NAME}")
connection_str = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sql.create_engine(connection_str)

base = declarative_base()

class Geolocation(base):
    __tablename__ = 'geolocation'

    geolocation_zip_code_prefix = Column(VARCHAR(10), primary_key=True)
    geolocation_lat = Column(Float)
    geolocation_lng = Column(Float)
    geolocation_city = Column(VARCHAR(50))
    geolocation_state = Column(VARCHAR(10))
    zipcode = relationship("Customers", back_populates="loc")
    zipcode_seller = relationship("Sellers", back_populates="localization")

class Customers(base):
    __tablename__ = 'customers'

    customer_id = Column(VARCHAR(50), primary_key = True)
    customer_unique_id = Column(VARCHAR(50))
    customer_zip_code_prefix = Column(VARCHAR(10), ForeignKey('geolocation.geolocation_zip_code_prefix'))
    customer_city = Column(VARCHAR(50))
    customer_state = Column(VARCHAR(10))
    orders = relationship("Orders", back_populates="owner")
    loc = relationship("Geolocation", back_populates = "geolocation_zip_code_prefix")


class Orders(base):
    __tablename__ = 'orders'

    order_id = Column(VARCHAR(50), primary_key = True)
    customer_id = Column(VARCHAR(50), ForeignKey('customers.customer_id'))
    order_status = Column(VARCHAR(20))
    order_purchase_timestamp = Column(DateTime)
    order_approved_at = Column(DateTime)
    order_delivered_carrier_date = Column(DateTime)
    order_delivered_customer_date = Column(DateTime)
    order_estimated_delivery_date = Column(DateTime)
    owner = relationship("Customers", back_populates="orders")
    orderitems = relationship("Order_Items", back_populates="order_id_items")
    order_payments = relationship("Order_Payments", back_populates="orderid")
    orderreviews = relationship("Order_Reviews", back_populates="order_review")

class Sellers(base):
    __tablename__ = 'sellers'

    seller_id = Column(VARCHAR(50), primary_key = True)
    seller_zip_code_prefix = Column(VARCHAR(10), ForeignKey('geolocation.geolocation_zip_code_prefix'))
    seller_city = Column(VARCHAR(50))
    seller_state = Column(VARCHAR(10))
    localization = relationship("Geolocation", back_populates="zipcode_seller")
    sellerid = relationship("Order_Items", back_populates="seller_item")

class Prodcategory(base):
    __tablename__ = 'prodcategory'

    product_category_name = Column(VARCHAR(100), primary_key = True)
    product_category_name_english = Column(VARCHAR(100))
    prodcat = relationship("Products", back_populates="prodcategory")

class Products(base):
    __tablename__ = 'products'

    product_id = Column(VARCHAR(50), primary_key = True)
    product_category_name = Column(VARCHAR(100), ForeignKey('prodcategory.product_category_name'))
    product_name_lenght = Column(Integer)
    product_description_lenght = Column(Integer)
    product_photos_qty = Column(Integer)
    product_weight_g = Column(Float)
    product_length_cm = Column(Float)
    product_height_cm = Column(Float)
    product_width_cm = Column(Float)
    prod_id = relationship("Order_Items", back_populates="product")
    prodcategory = relationship("Prodcategory", back_populates="prodcat")

class Order_Items(base):
    __tablename__ = 'order_items'

    order_id = Column(VARCHAR(50), ForeignKey('orders.order_id'), primary_key=True)
    order_item_id = Column(Integer, primary_key=True)
    product_id = Column(VARCHAR(50), ForeignKey('products.product_id'), primary_key=True)
    seller_id = Column(VARCHAR(50), ForeignKey('sellers.seller_id'), primary_key=True)
    shipping_limit_date = Column(DateTime, primary_key=True)
    price = Column(Float, primary_key=True)
    freight_value = Column(Float, primary_key=True)
    product = relationship("Products", back_populates="prod_id")
    order_id_items = relationship("Orders", back_populates="orderitems")
    seller_item = relationship("Sellers", back_populates="sellerid")


class Order_payments(base):
    __tablename__ = 'order_payments'

    order_id = Column(VARCHAR(50), ForeignKey('orders.order_id'), primary_key=True)
    payment_sequential = Column(Integer, primary_key=True)
    payment_type = Column(VARCHAR(20), primary_key=True)
    payment_installments = Column(Integer, primary_key=True)
    payment_value = Column(Float, primary_key=True)
    orderid = relationship("Orders", back_populates="order_payments")

class Order_reviews(base):
    __tablename__ = 'order_reviews'

    review_id = Column(VARCHAR(50), primary_key = True)
    order_id = Column(VARCHAR(50), ForeignKey('orders.order_id'), primary_key=True)
    review_score = Column(Integer)
    review_comment_title = Column(VARCHAR(100))
    review_comment_message = Column(VARCHAR(500))
    review_creation_date = Column(DateTime)
    review_answer_timestamp = Column(DateTime)
    order_review = relationship("Orders", back_populates="orderreviews")

if __name__ == "__main__":

    base.metadata.create_all(engine)
    DATA_DIR = 'Dataset_Raw'
    geolocation_df = pd.read_csv(f'{DATA_DIR}/olist_geolocation_dataset.csv')
    geolocation_clean = geolocation_df.drop_duplicates(subset=['geolocation_zip_code_prefix']).copy()
    
    sellers_df = pd.read_csv(f'{DATA_DIR}/olist_sellers_dataset.csv')
    customers_df = pd.read_csv(f'{DATA_DIR}/olist_customers_dataset.csv')

    missing_sellers_zips = sellers_df[~sellers_df['seller_zip_code_prefix'].isin(geolocation_clean['geolocation_zip_code_prefix'])].copy()

    if not missing_sellers_zips.empty:
        missing_sellers_zips = missing_sellers_zips[['seller_zip_code_prefix', 'seller_city', 'seller_state']].drop_duplicates()
        missing_sellers_zips.columns = ['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state']
        missing_sellers_zips['geolocation_lat'] = 0.0
        missing_sellers_zips['geolocation_lng'] = 0.0
        geolocation_clean = pd.concat([geolocation_clean, missing_sellers_zips], ignore_index=True)

    missing_customers_zips = customers_df[~customers_df['customer_zip_code_prefix'].isin(geolocation_clean['geolocation_zip_code_prefix'])].copy()
    if not missing_customers_zips.empty:
        missing_customers_zips = missing_customers_zips[['customer_zip_code_prefix', 'customer_city', 'customer_state']].drop_duplicates()
        missing_customers_zips.columns = ['geolocation_zip_code_prefix', 'geolocation_city', 'geolocation_state']
        missing_customers_zips['geolocation_lat'] = 0.0
        missing_customers_zips['geolocation_lng'] = 0.0
        geolocation_clean = pd.concat([geolocation_clean, missing_customers_zips], ignore_index=True)
    
    geolocation_clean = geolocation_clean.drop_duplicates(subset=['geolocation_zip_code_prefix'])

    prod_cat = pd.read_csv(f'{DATA_DIR}/product_category_name_translation.csv')
    products_df = pd.read_csv(f'{DATA_DIR}/olist_products_dataset.csv')
    
    missing_cats = products_df[~products_df['product_category_name'].isin(prod_cat['product_category_name']) & products_df['product_category_name'].notnull()].copy()
    if not missing_cats.empty:
        missing_cats = missing_cats[['product_category_name']].drop_duplicates()
        missing_cats['product_category_name_english'] = 'Unknown'
        prod_cat = pd.concat([prod_cat, missing_cats], ignore_index=True)

    prod_cat = prod_cat.drop_duplicates(subset=['product_category_name']).dropna()
    try:
        geolocation_clean.to_sql('geolocation', engine, if_exists='append', index=False)
        print("✅ Geolocalization loaded!")
    except Exception as e:
        print(f"Error occurred while loading geolocation data: {e}")
    try:
        customers_df.to_sql('customers', engine, if_exists='append', index=False)
        print("✅ Customers loaded!")
    except Exception as e:
        print(f"Error occurred while loading customers data: {e}")
    try:
        prod_cat.to_sql('prodcategory', engine, if_exists='append', index=False)
        print("✅ Prodcategory loaded!")
    except Exception as e:
        print(f"Error occurred while loading product category data: {e}")
    try:
        products_df.to_sql('products', engine, if_exists='append', index=False)
        print("✅ Products loaded!")
    except Exception as e:
        print(f"Error occurred while loading products data: {e}")
    
    orders_df = pd.read_csv(f'{DATA_DIR}/olist_orders_dataset.csv')
    try:
        orders_df.to_sql('orders', engine, if_exists='append', index=False)
        print("✅ Orders loaded!")
    except Exception as e:
        print(f"Error occurred while loading orders data: {e}")

    sellers_df = pd.read_csv(f'{DATA_DIR}/olist_sellers_dataset.csv')
    try:
        sellers_df.to_sql('sellers', engine, if_exists='append', index=False)
        print("✅ Sellers loaded!")
    except Exception as e:
        print(f"Error occurred while loading sellers data: {e}")

    order_items_df = pd.read_csv(f'{DATA_DIR}/olist_order_items_dataset.csv')
    try:
        order_items_df.to_sql('order_items', engine, if_exists='append', index=False)
        print("✅ Order Items loaded!")
    except Exception as e:
        print(f"Error occurred while loading order items data: {e}")

    order_payments_df = pd.read_csv(f'{DATA_DIR}/olist_order_payments_dataset.csv')
    try:
        order_payments_df.to_sql('order_payments', engine, if_exists='append', index=False)
        print("✅ Order Payments loaded!")
    except Exception as e:
        print(f"Error occurred while loading order payments data: {e}")

    order_reviews_df = pd.read_csv(f'{DATA_DIR}/olist_order_reviews_dataset.csv')
    try:
        order_reviews_df.to_sql('order_reviews', engine, if_exists='append', index=False)
        print("✅ Order Reviews loaded!")
    except Exception as e:
        print(f"Error occurred while loading order reviews data: {e}")

    print("Database initialization completed successfully!")
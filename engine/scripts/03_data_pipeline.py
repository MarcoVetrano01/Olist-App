import pandas as pd
import sqlalchemy as sql
import os
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'my_postgres_db' 
DB_PORT = '5432'
DB_NAME = os.getenv('POSTGRES_DB')
connection_str = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sql.create_engine(connection_str)

EXPORT_DIR = '/app/exports'

QUERIES = {
    "best_sellers.csv": """
        SELECT A.SELLER_ID, B.seller_zip_code_prefix as zip_code, B.seller_state as state, B.seller_city as city, 
               E.geolocation_lat, E.geolocation_lng,
               COUNT(A.ORDER_ID) as N_ORDERS,
               AVG(D.review_score) as Average_Score,
               SUM(CASE WHEN D.review_score < 3 THEN 1 ELSE 0 END) * 100.0 / COUNT(A.ORDER_ID) as perc_negative_reviews,
               SUM(CASE WHEN C.ORDER_DELIVERED_CUSTOMER_DATE > C.ORDER_ESTIMATED_DELIVERY_DATE THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(DISTINCT A.order_id), 0) as perc_delays,
               AVG(CASE WHEN C.ORDER_DELIVERED_CUSTOMER_DATE > C.ORDER_ESTIMATED_DELIVERY_DATE 
                   THEN EXTRACT(DAY FROM (C.ORDER_DELIVERED_CUSTOMER_DATE - C.ORDER_ESTIMATED_DELIVERY_DATE)) 
                   ELSE NULL END) as average_delay_days 
        FROM order_items A 
        JOIN sellers B ON A.SELLER_ID = B.SELLER_ID 
        LEFT JOIN geolocation E ON E.geolocation_zip_code_prefix = B.seller_zip_code_prefix 
        JOIN orders C ON A.ORDER_ID = C.ORDER_ID 
        LEFT JOIN order_reviews D ON D.order_id = A.order_id 
        GROUP BY A.seller_id, B.SELLER_ZIP_CODE_PREFIX, B.seller_state, B.seller_city, E.geolocation_lat, E.geolocation_lng 
        ORDER BY N_orders DESC, average_score DESC;
    """,
    
    "reviews.csv": """
        SELECT review_comment_message, review_score, 
               CASE WHEN review_score >=3 THEN 1 ELSE 0 END as is_positive 
        FROM order_reviews 
        WHERE review_comment_message IS NOT NULL;
    """,
    
    "customers_distribution.csv": """
        SELECT A.customer_unique_id, A.customer_zip_code_prefix, A.customer_city, A.customer_state, 
               B.geolocation_lat as lat, B.geolocation_lng as lng, 
               SUM(C.price + C.freight_value) as spesa_totale 
        FROM customers A 
        JOIN geolocation B ON A.customer_zip_code_prefix = B.geolocation_zip_code_prefix  
        JOIN orders D ON A.customer_id = D.customer_id 
        JOIN order_items C ON C.order_id = D.order_id 
        GROUP BY customer_unique_id, A.customer_zip_code_prefix, A.customer_city, A.customer_state, B.geolocation_lat, B.geolocation_lng  
        ORDER BY spesa_totale DESC;
    """,
    
    "delays_by_state.csv": """
        SELECT A.seller_state, 
               SUM(CASE WHEN C.order_estimated_delivery_date < C.order_delivered_customer_date THEN 1 ELSE 0 END) as number_of_delays, 
               COUNT(B.order_id) as number_of_orders, 
               ROUND(SUM(CASE WHEN C.order_estimated_delivery_date < C.order_delivered_customer_date THEN 1 ELSE 0 END) * 100.0 / COUNT(B.order_id), 2) as percentage_of_delays 
        FROM sellers A 
        JOIN order_items B ON A.seller_id = B.seller_id  
        JOIN orders C ON C.order_id = B.order_id 
        GROUP BY A.seller_state 
        ORDER BY number_of_delays DESC;
    """,
    
    "sales_by_category_month.csv": """
        WITH OrderMonths AS (
            SELECT B.product_category_name, 
                   EXTRACT(MONTH FROM C.order_purchase_timestamp) AS order_month, 
                   A.order_id 
            FROM order_items A 
            JOIN products B ON A.product_id = B.product_id  
            JOIN orders C ON A.order_id = C.order_id
        ) 
        SELECT D.product_category_name_english AS Category, 
               COUNT(*) FILTER (WHERE order_month = 1) AS Sold_in_Jan, 
               COUNT(*) FILTER (WHERE order_month = 2) AS Sold_in_Feb, 
               COUNT(*) FILTER (WHERE order_month = 3) AS Sold_in_Mar, 
               COUNT(*) FILTER (WHERE order_month = 4) AS Sold_in_Apr, 
               COUNT(*) FILTER (WHERE order_month = 5) AS Sold_in_May, 
               COUNT(*) FILTER (WHERE order_month = 6) AS Sold_in_Jun, 
               COUNT(*) FILTER (WHERE order_month = 7) AS Sold_in_Jul, 
               COUNT(*) FILTER (WHERE order_month = 8) AS Sold_in_Aug, 
               COUNT(*) FILTER (WHERE order_month = 9) AS Sold_in_Sep, 
               COUNT(*) FILTER (WHERE order_month = 10) AS Sold_in_Oct, 
               COUNT(*) FILTER (WHERE order_month = 11) AS Sold_in_Nov, 
               COUNT(*) FILTER (WHERE order_month = 12) AS Sold_in_Dec, 
               COUNT(order_id) AS total_sold 
        FROM OrderMonths OM 
        JOIN prodcategory D ON OM.product_category_name = D.product_category_name 
        GROUP BY 1;
    """,
    
    "delay_stats.csv": """
        SELECT B.seller_id, 
               COUNT(A.order_id) FILTER (WHERE A.order_estimated_delivery_date < A.order_delivered_customer_date) as number_of_delays, 
               COUNT(A.order_id) as total_orders, 
               AVG(A.order_approved_at - A.order_purchase_timestamp) as avg_seller_response_time, 
               AVG(A.order_delivered_carrier_date - A.order_approved_at) as carrier_delivery_time, 
               AVG(A.order_delivered_customer_date - A.order_delivered_carrier_date) as customer_delivery_time 
        FROM orders A 
        JOIN order_items B ON A.order_id = B.order_id 
        WHERE EXTRACT(DAY FROM A.order_delivered_customer_date) > EXTRACT(DAY FROM A.order_estimated_delivery_date) 
        GROUP BY B.seller_id 
        ORDER BY number_of_delays DESC;
    """,
    
    "network_delay.csv": """
        WITH customer_coordinates AS (
            SELECT A.seller_id, B.customer_id, C.seller_city, C.seller_state, 
                   D.customer_city, D.customer_state, E.geolocation_lat as customer_lat, E.geolocation_lng as customer_lng 
            FROM order_items A 
            JOIN orders B ON A.order_id = B.order_id  
            JOIN sellers C ON A.seller_id = C.seller_id 
            JOIN customers D ON D.customer_id = B.customer_id  
            JOIN geolocation E ON D.customer_zip_code_prefix = E.geolocation_zip_code_prefix 
            WHERE EXTRACT(DAY FROM B.order_estimated_delivery_date) < EXTRACT(DAY FROM B.order_delivered_customer_date)
        ), 
        seller_coordinates AS (
            SELECT A.seller_id, B.customer_id, C.seller_city, C.seller_state, D.customer_city, D.customer_state, 
                   E.geolocation_lat as seller_lat, E.geolocation_lng as seller_lng 
            FROM order_items A 
            JOIN orders B ON A.order_id = B.order_id  
            JOIN sellers C ON A.seller_id = C.seller_id 
            JOIN customers D ON D.customer_id = B.customer_id  
            JOIN geolocation E ON C.seller_zip_code_prefix = E.geolocation_zip_code_prefix 
            WHERE EXTRACT(DAY FROM B.order_estimated_delivery_date) < EXTRACT(DAY FROM B.order_delivered_customer_date)
        ) 
        SELECT A.seller_id, A.seller_city, A.seller_state, A.seller_lat, A.seller_lng,  
               B.customer_id, B.customer_city, B.customer_state, B.customer_lat, B.customer_lng 
        FROM seller_coordinates A 
        JOIN customer_coordinates B ON (A.seller_id = B.seller_id AND A.customer_id = B.customer_id);
    """,
    
    "delay_review_correlation.csv": """
        SELECT A.order_id, 
               EXTRACT(DAY FROM A.order_delivered_customer_date - A.order_estimated_delivery_date) as delivery_delay, 
               B.review_score 
        FROM orders A 
        JOIN order_reviews B ON A.order_id = B.order_id 
        WHERE B.review_score IS NOT NULL 
          AND EXTRACT(DAY FROM A.order_delivered_customer_date) - EXTRACT(DAY FROM A.order_estimated_delivery_date) IS NOT NULL 
        ORDER BY delivery_delay DESC, B.review_score DESC;
    """,
    
    "negative_reviews.csv": """
        SELECT review_comment_message, review_score 
        FROM order_reviews 
        WHERE review_score IS NOT NULL 
          AND review_comment_message IS NOT NULL 
          AND review_score <= 2;
    """,
    
    "product_reviews.csv": """
        SELECT A.product_id, D.product_category_name_english, AVG(B.review_score) as avg_score 
        FROM order_items A 
        JOIN order_reviews B ON A.order_id = B.order_id 
        JOIN products C ON A.product_id = C.product_id 
        JOIN prodcategory D ON D.product_category_name = C.product_category_name 
        WHERE B.review_score IS NOT NULL 
        GROUP BY A.product_id, D.product_category_name_english 
        ORDER BY avg_score DESC;
    """,
    
    "RMF_per_client.csv": """
        WITH controlclients AS (
            SELECT A.customer_unique_id, MAX(B.order_purchase_timestamp) as ultimo_ordine 
            FROM customers A 
            JOIN orders B ON A.customer_id = B.customer_id 
            WHERE B.order_purchase_timestamp < '2018-07-01' 
            GROUP BY A.customer_unique_id 
        ) 
        SELECT A.customer_unique_id, 
               EXTRACT(DAY FROM '2018-01-01'::TIMESTAMP - MAX(B.order_purchase_timestamp)) as recency, 
               COUNT(B.order_id) as frequency, 
               SUM(D.payment_value) as monetary, 
               AVG(E.review_score) as average_score, 
               AVG(CASE WHEN B.ORDER_DELIVERED_CUSTOMER_DATE > B.ORDER_ESTIMATED_DELIVERY_DATE THEN EXTRACT(DAY FROM (B.ORDER_DELIVERED_CUSTOMER_DATE - B.ORDER_ESTIMATED_DELIVERY_DATE)) ELSE 0 END) as average_delay_days, 
               CASE WHEN MAX(B.order_purchase_timestamp) = C.ultimo_ordine THEN 0 ELSE 1 END as churned 
        FROM customers A 
        JOIN orders B ON A.customer_id = B.customer_id  
        JOIN controlclients C ON A.customer_unique_id = C.customer_unique_id  
        JOIN order_payments D ON B.order_id = D.order_id 
        JOIN order_reviews E ON E.order_id = B.order_id 
        WHERE B.order_purchase_timestamp <= '2018-01-01' 
        GROUP BY A.customer_unique_id, C.ultimo_ordine  
        ORDER BY churned DESC;
    """,
    
    "cohort.csv": """
        WITH Cohort_Base AS (
            SELECT B.customer_unique_id, 
                   DATE_TRUNC('month', MIN(A.order_purchase_timestamp))::date AS purchase_month 
            FROM orders A 
            JOIN customers B ON A.customer_id = B.customer_id 
            GROUP BY B.customer_unique_id
        ),
        Date_Spine AS (
            SELECT generate_series(
                (SELECT MIN(purchase_month) FROM Cohort_Base), 
                (SELECT MAX(purchase_month) FROM Cohort_Base), 
                '1 month'::interval
            )::date AS calendar_month
        ), 
        User_Activities AS (
            SELECT DISTINCT B.customer_unique_id, 
                   DATE_TRUNC('month', A.order_purchase_timestamp)::date AS activity_month 
            FROM orders A 
            JOIN customers B ON A.customer_id = B.customer_id
        ) 
        SELECT ds.calendar_month AS purchase_month, 
               COUNT(DISTINCT cb.customer_unique_id) AS new_clients, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '1 month') AS still_active_1M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '2 month') AS still_active_2M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '3 month') AS still_active_3M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '4 month') AS still_active_4M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '5 month') AS still_active_5M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '6 month') AS still_active_6M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '7 month') AS still_active_7M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '8 month') AS still_active_8M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '9 month') AS still_active_9M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '10 month') AS still_active_10M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '11 month') AS still_active_11M, 
               COUNT(DISTINCT ua.customer_unique_id) FILTER (WHERE ua.activity_month = ds.calendar_month + INTERVAL '12 month') AS still_active_12M 
        FROM Date_Spine ds 
        LEFT JOIN Cohort_Base cb ON ds.calendar_month = cb.purchase_month 
        LEFT JOIN User_Activities ua ON cb.customer_unique_id = ua.customer_unique_id 
        GROUP BY ds.calendar_month  
        ORDER BY ds.calendar_month;
    """,
    
    "first_order_analysis.csv": """
        WITH RankedOrders AS (
            SELECT A.customer_unique_id, B.order_purchase_timestamp, 
                   ROW_NUMBER() OVER(PARTITION BY A.customer_unique_id ORDER BY B.order_purchase_timestamp) as order_sequence, 
                   LEAD(B.order_purchase_timestamp) OVER(PARTITION BY A.customer_unique_id ORDER BY B.order_purchase_timestamp) as next_order_timestamp, 
                   CASE WHEN B.order_estimated_delivery_date < B.order_delivered_customer_date THEN EXTRACT(DAY FROM (B.order_delivered_customer_date - B.order_estimated_delivery_date)) ELSE 0 END as delay, 
                   C.review_score, E.product_category_name, 
                   F.freight_value / NULLIF(F.price, 0) as delivery_cost_percentage, 
                   H.payment_installments, 
                   CASE WHEN A.customer_state != D.seller_state THEN 1 ELSE 0 END as interstate_shipping 
            FROM customers A 
            JOIN orders B ON A.customer_id = B.customer_id  
            LEFT JOIN order_reviews C ON C.order_id = B.order_id 
            LEFT JOIN order_items F ON F.order_id = B.order_id 
            LEFT JOIN products E ON E.product_id = F.product_id 
            LEFT JOIN sellers D ON D.seller_id = F.seller_id 
            LEFT JOIN order_payments H ON H.order_id = B.order_id 
        ) 
        SELECT customer_unique_id, delay, review_score, product_category_name, 
               delivery_cost_percentage, payment_installments, interstate_shipping, 
               CASE WHEN next_order_timestamp < order_purchase_timestamp + INTERVAL '90 days' THEN 1 ELSE 0 END as came_back_in_3_months 
        FROM RankedOrders  
        WHERE order_sequence = 1 
        ORDER BY customer_unique_id;
    """
}

if __name__ == "__main__":
    print(f"Creating export directory: {EXPORT_DIR}")
    os.makedirs(EXPORT_DIR, exist_ok=True)
    
    # Esegue le query iterando il dizionario in modo pulito e sicuro
    for filename, query in QUERIES.items():
        print(f"Executing query for {filename}...")
        try:
            # Dropna lo usiamo solo su best_sellers per evitare di perdere dati vitali negli altri
            df = pd.read_sql_query(query, engine)
            
            if filename == "best_sellers.csv":
                df = df.dropna()
                
            export_path = os.path.join(EXPORT_DIR, filename)
            df.to_csv(export_path, index=False)
            print(f"Success! {len(df)} rows exported to {filename}")
            
        except Exception as e:
            print(f"ERROR during export of {filename}: {e}")

    print("All exports completed!")
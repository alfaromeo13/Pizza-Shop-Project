import os
import json
import uuid
import math
import random
import time
from kafka import KafkaProducer
from mysql.connector import connect, Error
from datetime import date, timedelta, datetime  

usersLimit         = 1000
orderInterval      = 100
mysqlHost          = os.environ.get("MYSQL_SERVER", "localhost")
mysqlPort          = '3306'
mysqlUser          = 'mysqluser'
mysqlPass          = 'mysqlpw'
debeziumHostPort   = 'debezium:8083'
kafkaHostPort      = f"{os.environ.get('KAFKA_BROKER_HOSTNAME', 'localhost')}:{os.environ.get('KAFKA_BROKER_PORT', '29092')}"

# print(f"Kafka broker: {kafkaHostPort}")

def random_time_on_day(day: date) -> str:
    random_seconds = random.randint(0, 86399)
    full_datetime = datetime.combine(day, datetime.min.time()) + timedelta(seconds=random_seconds)
    # Format with fixed 3 decimal places for milliseconds:
    return full_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')

def daterange_weekdays(start_date, end_date):
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:  # 0 = Monday, 6 = Sunday
            yield current
        current += timedelta(days=1)

def generate_items(product_prices, num_items):
    items = []
    for _ in range(num_items):
        product = random.choice(product_prices)
        quantity = random.randint(1, 5)
        items.append({
            "productId": str(product[0]),
            "quantity": quantity,
            "price": product[1]
        })
    return items

def calculate_total_price(items):
    return round(math.fsum(item["quantity"] * item["price"] for item in items), 2)

def create_event(user, items, status, timestamp):
    return {
        "id": str(uuid.uuid4()),
        "createdAt": timestamp,
        "userId": user,
        "status": status,
        "price": calculate_total_price(items),
        "items": items
    }

producer = KafkaProducer(bootstrap_servers=kafkaHostPort, api_version=(7, 1, 0), 
  value_serializer=lambda m: json.dumps(m).encode('utf-8'))

events_processed = 0

try:
    with connect(
        host=mysqlHost,
        user=mysqlUser,
        password=mysqlPass,
    ) as connection:
        with connection.cursor() as cursor:

            print("Getting products for the products topic")
            cursor.execute("SELECT id, name, description, category, price, image FROM pizzashop.products")
            
            products = [{
                "id": str(row[0]),
                "name": row[1],
                "description": row[2],
                "category": row[3],
                "price": row[4],
                "image": row[5]
                }
                for row in cursor
            ]

            for product in products:
                producer.send('products', product, product["id"].encode("UTF-8"))
            
            producer.flush()

            cursor.execute("SELECT id FROM pizzashop.users")
            users = [row[0] for row in cursor]  # we are only interested in user IDs

            print("Getting product ID and PRICE as tuples...")
            cursor.execute("SELECT id, price FROM pizzashop.products")
            
            product_prices = [(row[0], row[1]) for row in cursor] # product along with price 
            
            # Historical data generation from 2022 to today
            start_date = date(2022, 1, 1)
            end_date = date.today()

            for single_date in daterange_weekdays(start_date, end_date):
                for _ in range(random.randint(40, 101)):
                    user = random.choice(users)
                    items = generate_items(product_prices, random.randint(1, 10))
                    event = create_event(user, items, "PAYMENT_CONFIRMED", random_time_on_day(single_date))
                    producer.send('orders', event, bytes(event["id"].encode("UTF-8")))
                    events_processed += 1

                    if events_processed % 100 == 0:
                        producer.flush()
            
            producer.flush()
            
            print("Added dummy records, and started live ingestion.")

            events_processed = 0
            
            # After this we will constantly simulate real orders comming up.
            while True:

                date(2022, 1, 1)

                user = random.choice(users)
                items = generate_items(product_prices, random.randint(1, 10))
                event = create_event(user, items, "PLACED_ORDER", datetime.now().isoformat())
                producer.send('orders', event, bytes(event["id"].encode("UTF-8")))
                events_processed += 1

                if events_processed % 100 == 0:
                    producer.flush()

                time.sleep(random.randint(orderInterval/5, orderInterval)/1000)

    connection.close()

except Error as e:
    print(e)
import os
import json
import uuid
import math
import random
import time
from kafka import KafkaProducer
from mysql.connector import connect, Error
from datetime import date, timedelta, datetime  

# Configuration
usersLimit = 1000
orderInterval = 3000
mysqlHost = os.environ.get("MYSQL_SERVER", "localhost")
mysqlPort = '3306'
mysqlUser = 'mysqluser'
mysqlPass = 'mysqlpw'
debeziumHostPort = 'debezium:8083'
kafkaHostPort = f"{os.environ.get('KAFKA_BROKER_HOSTNAME', 'localhost')}:{os.environ.get('KAFKA_BROKER_PORT', '29092')}"

def random_time_on_day(day: date) -> str:
    """Generate a random ISO 8601 timestamp on a given day, up to now if today"""
    if day == datetime.now().date():
        # Limit to current time
        now = datetime.now()
        start_of_day = datetime.combine(now.date(), datetime.min.time())

        # Get timedelta
        elapsed = now - start_of_day

        # Convert to milliseconds
        milliseconds_since_start_of_day = int(elapsed.total_seconds() * 1000)

        random_milliseconds = random.randint(0, milliseconds_since_start_of_day) 
        full_datetime = datetime.combine(day, datetime.min.time()) + timedelta(milliseconds=random_milliseconds)
        return full_datetime.isoformat()
    else:
        # Use full day range for past dates
        random_milliseconds = random.randint(0, 86399999)  # full 24h in ms
        full_datetime = datetime.combine(day, datetime.min.time()) + timedelta(milliseconds=random_milliseconds)
        return full_datetime.isoformat()

def daterange_weekdays(start_date, end_date):
    """Generate dates between start_date and end_date"""
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)

def generate_items(product_prices, num_items):
    """Generate random order items"""
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


def random_time_in_range(start_datetime, end_datetime):
    """Generate a random datetime between two datetime objects"""
    delta = end_datetime - start_datetime
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start_datetime + timedelta(seconds=random_seconds)

def calculate_total_price(items):
    """Calculate total price of order items"""
    return round(math.fsum(item["quantity"] * item["price"] for item in items), 2)

def create_event(user, items, status, timestamp):
    """Create order event with consistent structure"""
    return {
        "id": str(uuid.uuid4()),
        "createdAt": timestamp,
        "userId": user,
        "status": status,
        "price": calculate_total_price(items),
        "items": items
    }

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=kafkaHostPort, 
    api_version=(7, 1, 0), 
    value_serializer=lambda m: json.dumps(m).encode('utf-8')
)

events_processed = 0

try:
    with connect(
        host=mysqlHost,
        user=mysqlUser,
        password=mysqlPass,
    ) as connection:
        with connection.cursor() as cursor:
            # Load products
            print("Loading products...")
            cursor.execute("SELECT id, name, description, category, price, image FROM pizzashop.products")
            products = [{
                "id": str(row[0]),
                "name": row[1],
                "description": row[2],
                "category": row[3],
                "price": row[4],
                "image": row[5]
            } for row in cursor]

            for product in products:
                producer.send('products', product, product["id"].encode("UTF-8"))
            producer.flush()

            # Load users
            cursor.execute("SELECT id FROM pizzashop.users")
            users = [row[0] for row in cursor]

            # Load product prices
            print("Loading product prices...")
            cursor.execute("SELECT id, price FROM pizzashop.products")
            product_prices = [(row[0], row[1]) for row in cursor]

            # Generate historical data for full year of 2024
            start_time = time.time()
            start_date = date(2023, 1, 1)
            end_date = datetime.now().date()
            days_total = (end_date - start_date).days + 1

            for single_day in daterange_weekdays(start_date, end_date):
                num_orders_today = random.randint(120, 250)  # realistic daily volume

                for _ in range(num_orders_today):
                    user = random.choice(users)
                    items = generate_items(product_prices, random.randint(1, 6))

                    timestamp_str = random_time_on_day(single_day)  # ISO format
                    event = create_event(user, items, "PLACED_ORDER", timestamp_str)

                    producer.send('orders', event, bytes(event["id"].encode("UTF-8")))
                    events_processed += 1

                    if events_processed % 100    == 0:
                        print(f"Generated {events_processed:,} events so far...")
                        producer.flush()

            producer.flush()

            print(f"Finished generating {events_processed:,} orders across {days_total} days in {time.time() - start_time:.2f} seconds.")

            # Live order simulation
            events_processed = 0

            while True:
                user = random.choice(users)
                items = generate_items(product_prices, random.randint(1, 6))
                event = create_event(user, items, "PLACED_ORDER", datetime.now().isoformat())
                producer.send('orders', event, bytes(event["id"].encode("UTF-8")))
                events_processed += 1

                if events_processed % 10 == 0: 
                    producer.flush()

                time.sleep(random.randint(orderInterval/5, orderInterval)/1000)


    connection.close()
except Error as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.flush()
    producer.close()
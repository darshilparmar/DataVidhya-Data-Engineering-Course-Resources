
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Kafka broker settings
BROKER_URL = 'localhost:29092'  # Use the externally mapped port from docker-compose
TOPIC_NAME = 'purchases_2'

# Sample data for generating random purchases
USERS = ['john', 'alice', 'bob', 'emma', 'michael', 'sophia', 'william', 'olivia', 'james', 'ava']
CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'INR', 'BRL']
CATEGORIES = ['electronics', 'clothing', 'groceries', 'books', 'home', 'sports', 'beauty', 'toys', 'jewelry', 'office']
LOCATIONS = ['new_york', 'london', 'tokyo', 'paris', 'berlin', 'sydney', 'toronto', 'mumbai', 'rio', 'dubai']

def create_producer():
    """Create and return a Kafka producer instance."""
    return KafkaProducer(
        bootstrap_servers=BROKER_URL,
        key_serializer=lambda k: k.encode('utf-8'),
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_purchase():
    """Generate a random purchase record."""
    username = random.choice(USERS)
    
    # Generate a timestamp in ISO format
    now = datetime.now()
    timestamp = now.isoformat()
    
    return {
        'key': username,
        'value': {
            'username': username,
            'currency': random.choice(CURRENCIES),
            'amount': round(random.uniform(10.0, 500.0), 2),
            'timestamp': timestamp,
            'category': random.choice(CATEGORIES),
            'location': random.choice(LOCATIONS),
            'items': random.randint(1, 10)
        }
    }

def print_message_info(purchase, success=True):
    """Print information about the produced message."""
    status = "✓" if success else "✗"
    print(f"{status} [{purchase['value']['timestamp']}] {purchase['key']}: {purchase['value']['currency']} {purchase['value']['amount']:.2f} - {purchase['value']['category']} ({purchase['value']['location']})")

def main():
    """Main function to run the producer."""
    producer = create_producer()
    
    print("Starting purchases producer...")
    print(f"Sending messages to topic: {TOPIC_NAME}")
    print("Press Ctrl+C to stop")
    print("-" * 80)
    
    message_count = 0
    try:
        while True:
            # Generate a purchase
            purchase = generate_purchase()
            
            # Send to Kafka
            future = producer.send(
                TOPIC_NAME,
                key=purchase['key'],
                value=purchase['value']
            )
            
            # Print message info
            try:
                future.get(timeout=10)  # Wait for send to complete
                print_message_info(purchase)
                message_count += 1
                
                # Every 10 messages, print a summary
                if message_count % 10 == 0:
                    print(f"Total messages sent: {message_count}")
                    print("-" * 80)
                    
            except Exception as e:
                print_message_info(purchase, success=False)
                print(f"Error: {e}")
            
            # Random delay between 1 and 3 seconds
            time.sleep(random.uniform(1, 3))
            
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    finally:
        producer.flush()  # Make sure all messages are sent
        producer.close()
        print(f"Producer stopped. Total messages sent: {message_count}")

if __name__ == "__main__":
    main()
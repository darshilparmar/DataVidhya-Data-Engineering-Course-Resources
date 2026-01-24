
import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Dict, List

from confluent_kafka import Producer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092'
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_REALTIME')


# Define stocks with initial prices
STOCKS = {
    'AAPL': 180.0,   # Apple
    'MSFT': 350.0,   # Microsoft
    'GOOGL': 130.0,  # Alphabet (Google)
    'AMZN': 130.0,   # Amazon
    'META': 300.0,   # Meta (Facebook)
    'TSLA': 200.0,   # Tesla
    'NVDA': 400.0,   # NVIDIA
    'INTC': 35.0,    # Intel
}

class StreamDataColelctor:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic=KAFKA_TOPIC, interval=30):
          
        self.logger = logger
        self.topic = topic
        self.interval = interval

        self.current_stocks = STOCKS.copy()

        self.producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'continuous-stock-data-producer'
        }

        try:
            self.producer = Producer(self.producer_conf)
            self.logger.info(f"Producer initialized. Sending to: {bootstrap_servers}, Topic: {topic}")
        except Exception as e:
            self.logger.error(f"Failed to create Kafka Producer: {e}")
            raise

    def delivery_report(self, err, msg):
        if err is not None: 
            self.logger.error(f"Delivery failed for message: {msg}")
        else:
            self.logger.info(f"Message delivered successfully to topic {msg.topic} [{msg.partition()}]")

    def generate_stock_data(self, symbol):

        current_price = self.current_stocks[symbol]

        market_factor = random.uniform(-0.005, 0.005) #market wide movemenet
        stock_factor =  random.uniform(-0.005, 0.005) # stock specific movement 

        change_pct = market_factor + stock_factor

        if random.random() < 0.05:
            stock_factor += random.uniform(-0.02, 0.02)
        
        new_price = round(current_price * (1+change_pct), 2)

        self.current_stocks[symbol] = new_price

        #calcuate percent change
        price_change = round(new_price - current_price, 2)
        percent_change = round((price_change / current_price) * 100, 2)

        volume = random.randint(1000,100000)

        stock_data = {
            'symbol': symbol,
            'price': new_price,
            'change': price_change,
            'percent_change': percent_change,
            'volume': volume,
            "timestamp": datetime.now().isoformat()

        }

        return stock_data


    
    def produce_stock_data(self):
        """Main function to continuously produce stock data."""
        self.logger.info(f"Starting continuous stock data production")
        
        try:
            while True:
                # Track successful and failed symbols
                successful_symbols = 0
                failed_symbols = 0
                
                # Fetch and produce data for each stock
                for symbol in self.current_stocks.keys():
                    try:
                        # Generate stock data
                        stock_data = self.generate_stock_data(symbol)
                        
                        if stock_data:
                            # Convert to JSON string
                            message = json.dumps(stock_data)
                            
                            # Publish to Kafka topic
                            self.producer.produce(
                                self.topic, 
                                key=symbol, 
                                value=message,
                                callback=self.delivery_report
                            )
                            
                            # Trigger message delivery
                            self.producer.poll(0)
                            
                            successful_symbols += 1
                        else:
                            failed_symbols += 1
                    except Exception as e:
                        self.logger.error(f"Error processing {symbol}: {e}")
                        failed_symbols += 1
                
                # Log summary of data generation
                self.logger.info(f"Data Generation Summary: Successful: {successful_symbols}, Failed: {failed_symbols}")
                
                # Wait before next iteration
                self.logger.info(f"Waiting {self.interval} seconds before next data generation...")
                time.sleep(self.interval)
                
        except KeyboardInterrupt:
            self.logger.info("Producer stopped by user")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            self.logger.info("Producer shutting down")
            self.producer.flush()



def main():
    """Main function to run the producer."""
    try:
        # Create producer instance
        producer = StreamDataColelctor(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC,
            interval=2  # Generate data every 30 seconds
        )
        
        # Start producing stock data
        producer.produce_stock_data()
        
    except Exception as e:
        logger.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
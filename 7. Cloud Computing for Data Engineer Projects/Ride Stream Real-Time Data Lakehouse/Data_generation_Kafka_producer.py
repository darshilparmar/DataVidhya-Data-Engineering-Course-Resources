from kafka import KafkaProducer 
from kafka.sasl.oauth import AbstractTokenProvider
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import json
import random
from datetime import datetime, timedelta

topicname = 'realtimeridedata'
BROKERS = 'boot-azgxfx6o.c1.kafka-serverless.us-east-1.amazonaws.com:9098'
region = 'us-east-1'

class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self):
        super().__init__()
    
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token(region)
        return token
    
    def extend_token(self):
        return self.token()
    
    def principal(self):
        return "msk-iam-user"

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers=BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retry_backoff_ms=500,
    request_timeout_ms=20000,
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
)

# NYC city and locations
city = ['Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island']
manhattan_locations = ['Upper East Side', 'Upper West Side', 'Midtown', 'Downtown', 'Chelsea', 'Harlem']
brooklyn_locations = ['Williamsburg', 'Park Slope', 'DUMBO', 'Brooklyn Heights', 'Bushwick']
queens_locations = ['Astoria', 'Long Island City', 'Flushing', 'Jackson Heights']
bronx_locations = ['Yankee Stadium', 'Fordham', 'Pelham Bay']
staten_island_locations = ['St. George', 'Tottenville', 'New Dorp']

# Payment types
payment_types = ['Credit card', 'Cash', 'No charge', 'Dispute', 'Unknown']

# Rate codes
rate_codes = ['Standard', 'JFK', 'Newark', 'Nassau/Westchester', 'Negotiated', 'Group ride']

# Trip types
trip_types = ['Street-hail', 'Dispatch']

def get_pickup_loc(city):
    if city == 'Manhattan':
        return random.choice(manhattan_locations);
    elif city == 'Brooklyn':
        return random.choice(brooklyn_locations)
    elif city == 'Queens':
        return random.choice(queens_locations)
    elif city == 'Bronx':
        return random.choice(bronx_locations)
    else:
        return random.choice(staten_island_locations)

def get_dropoff_loc(pickup_city):
    # 70% chance dropoff in same city, 30% chance in different city
    if random.random() < 0.7:
        return get_pickup_loc(pickup_city)
    else:
        return get_pickup_loc(random.choice([b for b in city if b != pickup_city]))

def generate_green_taxi_data(trip_id):
    # Base timestamp - random date in the last 30 days
    base_time = datetime.now() - timedelta(days=random.randint(1, 30))
    
    # Pickup details
    pickup_city = random.choice(city)
    pickup_loc = get_pickup_loc(pickup_city)
    
    # Dropoff details
    dropoff_city = pickup_city if random.random() < 0.7 else random.choice([b for b in city if b != pickup_city])
    dropoff_loc = get_dropoff_loc(dropoff_city)
    
    # Trip metrics
    trip_dst = round(random.uniform(0.5, 25.0), 2)
    fare_amt = round(trip_dst * 2.5 + random.uniform(2.0, 10.0), 2)
    fare_amt = round(fare_amt * random.uniform(0.1, 0.25), 2) if random.random() < 0.8 else 0.0
    tolls_amt = round(random.uniform(0.0, 10.0), 2) if random.random() < 0.3 else 0.0
    total_amt = fare_amt + fare_amt + tolls_amt
    
    # Generate timestamps
    pickup_time = base_time
    dropoff_time = pickup_time + timedelta(minutes=random.randint(5, 90))
    
    taxi_data = {
        "trip_id": f"trip_{trip_id:04d}",
        "vendor_id": random.randint(1, 2),
        "pickup_dt": pickup_time.strftime("%Y-%m-%d %H:%M:%S"),
        "dropoff_dt": dropoff_time.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_cnt": random.randint(1, 6),
        "trip_dst": trip_dst,
        "pickup_city": pickup_city,
        "pickup_loc": pickup_loc,
        "dropoff_city": dropoff_city,
        "dropoff_loc": dropoff_loc,
        "fare_amt": fare_amt,
        "fare_amt": fare_amt,
        "tolls_amt": tolls_amt,
        "total_amt": total_amt,
        "payment_typ": random.choice(payment_types),
        "rate_cd": random.choice(rate_codes),
        "trip_typ": random.choice(trip_types),
        "congestion_surcharge": round(random.uniform(0.0, 2.5), 2) if random.random() < 0.4 else 0.0,
        "airport_fee": round(random.uniform(0.0, 1.25), 2) if random.random() < 0.2 else 0.0,
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    return taxi_data

# Send exactly 100 records
total_records = 100
sent_count = 0

print(f"Starting to send {total_records} NYC Green Taxi records...")

for i in range(total_records):
    data = generate_green_taxi_data(i + 1)
    
    try:
        future = producer.send(topicname, value=data)
        producer.flush()
        record_metadata = future.get(timeout=10)
        
        sent_count += 1
        print(f"✅ Sent record {sent_count}/{total_records}: Trip ID {data['trip_id']} - ${data['total_amt']} - {data['pickup_city']} to {data['dropoff_city']}")
        
    except Exception as e:
        print(f"❌ Error sending message: {e}")
        break

print(f"\nFinished! Successfully sent {sent_count} out of {total_records} records.")
producer.close()
import json
import time
import random
from datetime import datetime
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData
from faker import Faker

fake = Faker("en_IN")   

CONNECTION_STR = "Primary Key Connection"

EVENT_HUB_NAME = "loan_events"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

def random_date(start_year=2000, end_year=2015):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)

    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    input_ts=(start_date + timedelta(days=random_days)).isoformat()  
    dt = datetime.strptime(input_ts, "%Y-%m-%dT%H:%M:%S")
    output = dt.strftime("%d/%m/%Y, %H:%M:%S.%f")[:-3] + " +00:00"
    return output

def random_date_birth(start_year=1950, end_year=1980):
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)

    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    input_ts=(start_date + timedelta(days=random_days)).isoformat()  
    dt = datetime.strptime(input_ts, "%Y-%m-%dT%H:%M:%S")
    output = dt.strftime("%d/%m/%Y, %H:%M:%S.%f")[:-3] + " +00:00"
    return output

STATE_CITY_MAP = {
    "UP": ["Lucknow", "Kanpur", "Varanasi", "Agra"],
    "MH": ["Mumbai", "Pune", "Nagpur"],
    "DL": ["New Delhi"],
    "KA": ["Bengaluru", "Mysuru"],
    "TN": ["Chennai", "Coimbatore"],
    "GJ": ["Ahmedabad", "Surat"],
    "WB": ["Kolkata"],
    "RJ": ["Jaipur", "Udaipur"]
}

CITY_PINCODE_MAP = {
    "Lucknow": ["226001", "226010", "226020"],
    "Kanpur": ["208001", "208002", "208003"],
    "Varanasi": ["221001", "221002", "221003"],
    "Agra": ["282001", "282002", "282003"],

    "Mumbai": ["400001", "400002", "400003"],
    "Pune": ["411001", "411002", "411003"],
    "Nagpur": ["440001", "440002", "440003"],

    "New Delhi": ["110001", "110002", "110003"],

    "Bengaluru": ["560001", "560002", "560003"],
    "Mysuru": ["570001", "570002", "570003"],

    "Chennai": ["600001", "600002", "600003"],
    "Coimbatore": ["641001", "641002", "641003"],

    "Ahmedabad": ["380001", "380002", "380003"],
    "Surat": ["395001", "395002", "395003"],

    "Kolkata": ["700001", "700002", "700003"],

    "Jaipur": ["302001", "302002", "302003"],
    "Udaipur": ["313001", "313002", "313003"]
}





def generate_sample_event(i):
    return {
        "event_id": f"EVT{i}",
        "event_ts": datetime.utcnow().isoformat(),

        "customer": {
            "customer_id": f"CUST{i}",
            "full_name": fake.name(),  
            "date_of_birth": random_date_birth(1950,1980),
            "gender": random.choice(["M", "F"]),
            "city": city,
            "state": state,
            "pincode": pincode,
            "employment_type": random.choice(["SALARIED", "SELF_EMPLOYED"]),
            "annual_income": random.randint(300000, 1500000),
            "ACCOUNT_OPEN_DATE": random_date(2000,2020),
            "kyc_status": random.choice(["VERIFIED", "PENDING"]),
            "customer_status": random.choice(["ACTIVE", "INACTIVE"])
        },

        "application": {
            "application_id": f"APP{i}",
            "channel": random.choice(["MOBILE", "WEB", "BRANCH"]),
            "loan_product": loan_product,
            "purpose" : purpose,
            "requested_amount": random.randint(1000, 1000000),
            "tenure_months": random.choice([12, 24, 36]),
            "interest_rate": round(random.uniform(10.5, 15.5), 2),
            "branch_cd": random.choice(['01','34','98','76','89']),
            "application_status": "SUBMITTED"
        },

        "device": {
            "device_id": f"DEV{i}",
            "device_type": random.choice(["ANDROID", "IOS", "WEB"]),
            "os_version": random.choice(["13", "14", "15"]),
            "browser": random.choice(["CHROME", "BINGE", "SAFARI","BRAVE"]),
            "ip_address": fake.ipv4(),
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6)
        }
    }

try:
    for i in range(1, 150000):
        state = random.choice(list(STATE_CITY_MAP.keys()))
        city = random.choice(STATE_CITY_MAP[state])
        pincode=random.choice(CITY_PINCODE_MAP[city])

        loan_products=["PERSONAL", "HOME", "CAR","BUSINESS","Education"]
        loan_product=random.choice(loan_products)
        PRICES_MAP={
            "PERSONAL" : ["Medical","DEBT"],
            "HOME" : ["LAND","HOME","Rennovation"],
            "BUSINESS" : ["Small","MSME","MNC"],
            "Education":["Undergradution","PostGradution","Diploma"],
            "CAR":["Hatchback","Sedan","SUV","Compact-SUV"]
            }

        purpose=random.choice(PRICES_MAP[loan_product])
        batch = producer.create_batch()
        event = generate_sample_event(i)
        batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)
        print(event)
        print(f"Sent event {i}")
        time.sleep(1)

finally:
    producer.close()
    print("Completed")
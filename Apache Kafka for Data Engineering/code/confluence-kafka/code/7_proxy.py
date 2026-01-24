import json 
import requests


REST_PROXY_URL = "http://localhost:8084"

def get_topics():
    resp = requests.get(f"{REST_PROXY_URL}/topics")

    try:
        resp.raise_for_status()
    except:
        print("failed to list topics")
        exit(1)

    print(resp.json())

    return resp.json()

def get_topic(topic_name):
    resp = requests.get(f"{REST_PROXY_URL}/topics/{topic_name}")

    try:
        resp.raise_for_status()
    except:
        print("failed to list topics")
        exit(1)

    print("Fetching topic metadata...")
    print(resp.json())

    return json.dumps(resp.json())



def get_brokers():
    resp = requests.get(f"{REST_PROXY_URL}/brokers")

    try:
        resp.raise_for_status()
    except:
        print("failed to get roker details")
        exit(1)
    print(resp.json())
   
    return json.dumps(resp.json())

if __name__ == "__main__":
    topics = get_topics()
    get_topic(topics[0])
    get_brokers()
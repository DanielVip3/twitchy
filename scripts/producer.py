import requests
import json
from confluent_kafka import Producer

config = {
  'bootstrap.servers': 'kafka:29092',
  'client.id': 'dotturin-bike-producer',
  'message.max.bytes': 10485760
}

producer = Producer(config)

API_URL = "https://gbfs.api.ridedott.com/public/v2/turin/free_bike_status.json"
TOPIC_NAME = "dotturin-bike-status"

def producer_callback(err, msg):
  if err is not None:
    print(f"[-] Delivery error: {err}")
  else:
    print(f"[+] Message sent to {msg.topic()}")

def fetch_free_bikes(url):
  response = requests.get(url)

  if response.status_code == 200:
    data = response.json()

    return data
  else:
    print(f"[-] API error! Status code: {response.status_code}")

    return None

def main():
  try:
    print(f"\n[*] API request: {API_URL}")
      
    data = fetch_free_bikes(API_URL)
      
    if data is not None:
      payload = json.dumps(data)
        
      producer.produce(
        topic=TOPIC_NAME, 
        value=payload.encode('utf-8'), 
        callback=producer_callback
      )
        
      # TEMPORARY for testing [TODO: remove]
      producer.flush()
   
  except Exception as e:
    print(f"[-] Runtime error: {e}")

if __name__ == "__main__":
    print("[*] Starting Kafka producer...")
    main()
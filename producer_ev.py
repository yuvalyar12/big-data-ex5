import csv
import time
import json
from kafka import KafkaProducer

# Make sure this matches the filename you downloaded from Kaggle
CSV_FILE_PATH = 'input/Electric_Vehicle_Population_Data.csv' 
TOPIC_NAME = 'electric-cars'
BOOTSTRAP_SERVERS = ['localhost:9092']

def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"--- Starting Producer for Topic: {TOPIC_NAME} ---")
    
    try:
        with open(CSV_FILE_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            count = 0
            for row in reader:
                producer.send(TOPIC_NAME, value=row)
              
                count += 1
                if count % 100 == 0:
                    print(f"Sent {count} vehicles... (Last: {row.get('Make')} in {row.get('City')})")
                
                time.sleep(0.01) 
                
        producer.flush()
        print(f"--- Finished! Total records sent: {count} ---")

    except FileNotFoundError:
        print(f"ERROR: The file '{CSV_FILE_PATH}' was not found.")
        print("Make sure the CSV file is in the same folder as this script.")
        
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    finally:
        producer.close()

if __name__ == "__main__":
    run_producer()

import json
import time
from kafka import KafkaProducer
from datetime import datetime
import random
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

CITIES = {
    1: 'Surabaya',
    2: 'Sidoarjo',
    3: 'Gresik',
    4: 'Malang',
    5: 'Pasuruan'
}

DEVICES_PER_CITY = {
    1: ['SKWH-SBY-001', 'SKWH-SBY-002', 'SKWH-SBY-003'],
    2: ['SKWH-SDA-001', 'SKWH-SDA-002'],
    3: ['SKWH-GSK-001'],
    4: ['SKWH-MLG-001', 'SKWH-MLG-002'],
    5: ['SKWH-PAS-001']
}

def generate_sensor_data(anomaly_probability=0.05):
    city_id = random.randint(1, 5)
    city_name = CITIES[city_id]
    device_name = random.choice(DEVICES_PER_CITY[city_id])
    
    generate_anomaly = random.uniform(0, 1) < anomaly_probability
    if generate_anomaly:
        watt_value = random.choice([random.randint(200, 250), random.randint(300, 350)])
    else:
        watt_value = random.randint(250, 300)

    voltage_value = random.randint(100, 200)
    electric_current = round((watt_value / voltage_value), 2)

    data_point = {
        'date_time': datetime.now().isoformat(),
        'city_id': city_id,
        'city_name': city_name,
        'device_name': device_name,
        'P_watt': watt_value,
        'V_volt': voltage_value,
        'I_amphere': electric_current
    }
    return data_point

def send_data_to_kafka(producer, topic_name):
    log.info("Memulai streaming producer sensor... (Tekan Ctrl+C untuk berhenti)")
    while True:
        data_point = generate_sensor_data()
        
        try:
            producer.send(topic_name, value=data_point)
            log.info(f"Data diproduksi: {data_point}")
            time.sleep(1)
            
        except Exception as e:
            log.warning(f"Gagal mengirim data: {e}. Mencoba lagi...")
            time.sleep(5)

if __name__ == "__main__":
    topic_name = 'sensor_topic'
    bootstrap_servers = ['localhost:9092']

    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )
        log.info(f"Koneksi Kafka Producer ke {bootstrap_servers} berhasil.")
        
        send_data_to_kafka(producer, topic_name)
        
    except Exception as e:
        log.error(f"Gagal terhubung ke Kafka di {bootstrap_servers}.")
        log.error("Pastikan Anda sudah menjalankan: 'docker-compose -f docker-compose.streaming.yaml up -d'")
        log.error(f"Error: {e}")
"""
Telemetry Preprocessor Service

Consumes raw telemetry, enriches with derived metrics, and produces enriched data.
"""
import os
import sys
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer, Producer
import redis

sys.path.insert(0, '/app/shared')
from schemas.telemetry_schema import RawTelemetry, EnrichedTelemetry, DerivedMetrics, AnomalyFlags, NormalizedData, ProcessingMetadata

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class PreprocessorService:
    def __init__(self):
        self.kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.input_topic = os.getenv('KAFKA_INPUT_TOPIC', 'car-telemetry-raw')
        self.output_topic = os.getenv('KAFKA_OUTPUT_TOPIC', 'car-telemetry-enriched')
        self.run_duration = int(os.getenv('RUN_DURATION', '270'))

        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_bootstrap,
            'group.id': 'telemetry-preprocessor',
            'auto.offset.reset': 'earliest'
        })
        self.producer = Producer({'bootstrap.servers': self.kafka_bootstrap})
        self.consumer.subscribe([self.input_topic])

        # Redis for vehicle state tracking
        self.redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'redis'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            decode_responses=True
        )
        logger.info("Preprocessor service initialized")

    def calculate_derived_metrics(self, telemetry: dict, prev_data: dict) -> DerivedMetrics:
        """Calculate derived metrics from raw sensor data"""
        sensor = telemetry['sensor_data']

        # Calculate acceleration
        acceleration = 0.0
        if prev_data and 'speed_kmh' in prev_data:
            time_delta = 1.0  # Assuming 1 second intervals
            speed_diff = (sensor['speed_kmh'] - prev_data['speed_kmh']) / 3.6  # Convert to m/s
            acceleration = speed_diff / time_delta

        # Calculate engine load
        engine_load = (sensor['rpm'] / 8000) * (sensor['throttle_position_percent'] / 100) * 100

        # Calculate brake intensity
        brake_intensity = min(sensor['brake_pressure_bar'] / 10.0, 1.0)

        # Determine if idling
        is_idling = sensor['rpm'] > 800 and sensor['speed_kmh'] < 5

        # Determine if aggressive driving
        is_aggressive = abs(acceleration) > 3.0 or brake_intensity > 0.7

        return DerivedMetrics(
            acceleration_ms2=acceleration,
            fuel_efficiency_kmpl=None,  # Placeholder
            engine_load_percent=engine_load,
            brake_intensity=brake_intensity,
            is_idling=is_idling,
            is_aggressive_driving=is_aggressive
        )

    def detect_anomalies(self, sensor_data: dict) -> AnomalyFlags:
        """Detect anomalies using rule-based logic"""
        return AnomalyFlags(
            overheating=sensor_data['engine_temp_celsius'] > 115,
            low_oil_pressure=sensor_data['oil_pressure_bar'] < 2.5 and sensor_data['rpm'] > 2000,
            battery_issue=sensor_data['battery_voltage'] < 11.5 or sensor_data['battery_voltage'] > 15,
            sudden_deceleration=False,  # Calculated from acceleration
            fuel_anomaly=False  # Requires historical comparison
        )

    def normalize_data(self, sensor_data: dict) -> NormalizedData:
        """Normalize sensor values to 0-1 scale"""
        return NormalizedData(
            speed_normalized=min(sensor_data['speed_kmh'] / 300, 1.0),
            rpm_normalized=min(sensor_data['rpm'] / 8000, 1.0),
            temp_normalized=min(sensor_data['engine_temp_celsius'] / 150, 1.0)
        )

    def process_message(self, msg_value: dict) -> dict:
        """Process a single telemetry message"""
        start_time = datetime.utcnow()
        vehicle_id = msg_value['vehicle_id']

        # Get previous data from Redis
        prev_key = f"vehicle:{vehicle_id}:last"
        prev_data_str = self.redis_client.get(prev_key)
        prev_data = json.loads(prev_data_str) if prev_data_str else {}

        # Calculate enrichments
        derived_metrics = self.calculate_derived_metrics(msg_value, prev_data)
        anomaly_flags = self.detect_anomalies(msg_value['sensor_data'])
        normalized_data = self.normalize_data(msg_value['sensor_data'])

        # Store current data for next iteration
        self.redis_client.setex(prev_key, 3600, json.dumps(msg_value['sensor_data']))

        # Create enriched telemetry
        enriched = {
            **msg_value,
            'derived_metrics': derived_metrics.model_dump(),
            'anomaly_flags': anomaly_flags.model_dump(),
            'normalization': normalized_data.model_dump(),
            'processing_metadata': {
                'processed_at': datetime.utcnow().isoformat(),
                'processor_version': 'v1.0',
                'enrichment_latency_ms': (datetime.utcnow() - start_time).total_seconds() * 1000
            }
        }

        return enriched

    def run(self):
        """Main processing loop"""
        logger.info(f"Starting preprocessor (run duration: {self.run_duration}s)...")
        import time
        start_time = time.time()

        try:
            while True:
                if time.time() - start_time >= self.run_duration:
                    break

                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    raw_data = json.loads(msg.value().decode('utf-8'))
                    enriched_data = self.process_message(raw_data)

                    self.producer.produce(
                        self.output_topic,
                        value=json.dumps(enriched_data, default=str).encode('utf-8'),
                        key=msg.key()
                    )
                    self.producer.poll(0)

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        finally:
            self.producer.flush()
            self.consumer.close()
            logger.info("Preprocessor shutdown complete")


if __name__ == "__main__":
    service = PreprocessorService()
    service.run()

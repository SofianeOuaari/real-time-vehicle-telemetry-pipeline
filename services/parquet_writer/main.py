"""
Parquet Writer Service

Consumes enriched telemetry and writes to partitioned Parquet files.
"""
import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from confluent_kafka import Consumer
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ParquetWriterService:
    def __init__(self):
        self.kafka_bootstrap = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.input_topic = os.getenv('KAFKA_TOPIC', 'car-telemetry-enriched')
        self.output_path = Path(os.getenv('PARQUET_OUTPUT_PATH', '/data/parquet'))
        self.batch_size = int(os.getenv('BATCH_SIZE', '1000'))
        self.run_duration = int(os.getenv('RUN_DURATION', '300'))

        self.consumer = Consumer({
            'bootstrap.servers': self.kafka_bootstrap,
            'group.id': 'parquet-writer',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([self.input_topic])

        self.batch = []
        logger.info(f"Parquet writer initialized (output: {self.output_path})")

    def flatten_record(self, record: dict) -> dict:
        """Flatten nested JSON for Parquet schema"""
        flat = {
            'vehicle_id': record['vehicle_id'],
            'timestamp': record['timestamp'],
            'trace_id': record['trace_id'],
            'speed_kmh': record['sensor_data']['speed_kmh'],
            'rpm': record['sensor_data']['rpm'],
            'engine_temp': record['sensor_data']['engine_temp_celsius'],
            'fuel_level': record['sensor_data']['fuel_level_percent'],
            'battery_voltage': record['sensor_data']['battery_voltage'],
            'acceleration': record['derived_metrics']['acceleration_ms2'],
            'engine_load': record['derived_metrics']['engine_load_percent'],
            'is_idling': record['derived_metrics']['is_idling'],
            'is_aggressive': record['derived_metrics']['is_aggressive_driving'],
            'overheating': record['anomaly_flags']['overheating'],
            'low_oil_pressure': record['anomaly_flags']['low_oil_pressure'],
            'battery_issue': record['anomaly_flags']['battery_issue'],
        }
        return flat

    def write_batch(self):
        """Write accumulated batch to Parquet file"""
        if not self.batch:
            return

        # Flatten records
        flat_records = [self.flatten_record(r) for r in self.batch]

        # Create PyArrow table
        table = pa.Table.from_pylist(flat_records)

        # Generate partitioned path
        now = datetime.utcnow()
        partition_path = self.output_path / f"year={now.year}" / f"month={now.month:02d}" / f"day={now.day:02d}" / f"hour={now.hour:02d}"
        partition_path.mkdir(parents=True, exist_ok=True)

        # Write Parquet file
        output_file = partition_path / f"data_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
        pq.write_table(table, output_file, compression='snappy')

        logger.info(f"Wrote {len(self.batch)} records to {output_file}")
        self.batch = []

    def run(self):
        """Main processing loop"""
        logger.info(f"Starting parquet writer (run duration: {self.run_duration}s)...")
        import time
        start_time = time.time()

        try:
            while True:
                if time.time() - start_time >= self.run_duration:
                    break

                msg = self.consumer.poll(1.0)
                if msg is None:
                    if self.batch:
                        self.write_batch()
                    continue
                if msg.error():
                    continue

                try:
                    record = json.loads(msg.value().decode('utf-8'))
                    self.batch.append(record)

                    if len(self.batch) >= self.batch_size:
                        self.write_batch()

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        finally:
            if self.batch:
                self.write_batch()
            self.consumer.close()
            logger.info("Parquet writer shutdown complete")


if __name__ == "__main__":
    service = ParquetWriterService()
    service.run()

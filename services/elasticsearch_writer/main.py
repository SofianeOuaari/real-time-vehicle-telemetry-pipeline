"""
Elasticsearch Writer Service

Consumes enriched telemetry from Kafka and indexes flattened documents
into daily Elasticsearch indices for Kibana visualisation.
"""
import os
import json
import logging
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from confluent_kafka import Consumer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'car-telemetry-enriched')
ES_HOST = os.getenv('ELASTICSEARCH_HOST', 'http://elasticsearch:9200')
ES_INDEX_PREFIX = os.getenv('ES_INDEX_PREFIX', 'telemetry')
ES_BUFFER_SIZE = int(os.getenv('ES_BUFFER_SIZE', '100'))


def flatten(raw: dict) -> dict:
    """Flatten nested enriched-telemetry message into a single-level document."""
    sensor = raw.get('sensor_data', {})
    loc = raw.get('location', {})
    derived = raw.get('derived_metrics', {})
    anomaly = raw.get('anomaly_flags', {})
    norm = raw.get('normalization', {})
    proc = raw.get('processing_metadata', {})

    doc = {
        '@timestamp': raw.get('timestamp'),
        'vehicle_id': raw.get('vehicle_id'),
        'trace_id': raw.get('trace_id'),
        'span_id': raw.get('span_id'),

        # sensor readings
        'speed_kmh': sensor.get('speed_kmh'),
        'rpm': sensor.get('rpm'),
        'engine_temp_celsius': sensor.get('engine_temp_celsius'),
        'fuel_level_percent': sensor.get('fuel_level_percent'),
        'throttle_position_percent': sensor.get('throttle_position_percent'),
        'brake_pressure_bar': sensor.get('brake_pressure_bar'),
        'coolant_temp_celsius': sensor.get('coolant_temp_celsius'),
        'oil_pressure_bar': sensor.get('oil_pressure_bar'),
        'battery_voltage': sensor.get('battery_voltage'),
        'odometer_km': sensor.get('odometer_km'),

        # geo_point — Kibana Maps compatible
        'location': {
            'lat': loc.get('latitude'),
            'lon': loc.get('longitude'),
        } if loc.get('latitude') is not None else None,
        'altitude_m': loc.get('altitude_m'),

        # derived metrics
        'acceleration_ms2': derived.get('acceleration_ms2'),
        'fuel_efficiency_kmpl': derived.get('fuel_efficiency_kmpl'),
        'engine_load_percent': derived.get('engine_load_percent'),
        'brake_intensity': derived.get('brake_intensity'),
        'is_idling': derived.get('is_idling'),
        'is_aggressive_driving': derived.get('is_aggressive_driving'),

        # anomaly flags
        'overheating': anomaly.get('overheating', False),
        'low_oil_pressure': anomaly.get('low_oil_pressure', False),
        'battery_issue': anomaly.get('battery_issue', False),
        'sudden_deceleration': anomaly.get('sudden_deceleration', False),
        'fuel_anomaly': anomaly.get('fuel_anomaly', False),
        # convenience roll-up for dashboards
        'anomaly_detected': any(anomaly.values()) if anomaly else False,

        # normalised values
        'speed_normalized': norm.get('speed_normalized'),
        'rpm_normalized': norm.get('rpm_normalized'),
        'temp_normalized': norm.get('temp_normalized'),

        # processing metadata
        'processed_at': proc.get('processed_at'),
        'enrichment_latency_ms': proc.get('enrichment_latency_ms'),
    }

    # drop None-valued keys so ES mapping stays clean
    return {k: v for k, v in doc.items() if v is not None}


INDEX_TEMPLATE = {
    "index_patterns": ["telemetry-*"],
    "template": {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        },
        "mappings": {
            "properties": {
                "@timestamp":           {"type": "date"},
                "vehicle_id":           {"type": "keyword"},
                "trace_id":             {"type": "keyword"},
                "span_id":              {"type": "keyword"},
                "speed_kmh":            {"type": "float"},
                "rpm":                  {"type": "integer"},
                "engine_temp_celsius":  {"type": "float"},
                "fuel_level_percent":   {"type": "float"},
                "throttle_position_percent": {"type": "float"},
                "brake_pressure_bar":   {"type": "float"},
                "coolant_temp_celsius": {"type": "float"},
                "oil_pressure_bar":     {"type": "float"},
                "battery_voltage":      {"type": "float"},
                "odometer_km":          {"type": "float"},
                "location":             {"type": "geo_point"},
                "altitude_m":           {"type": "float"},
                "acceleration_ms2":     {"type": "float"},
                "fuel_efficiency_kmpl": {"type": "float"},
                "engine_load_percent":  {"type": "float"},
                "brake_intensity":      {"type": "float"},
                "is_idling":            {"type": "boolean"},
                "is_aggressive_driving":{"type": "boolean"},
                "overheating":          {"type": "boolean"},
                "low_oil_pressure":     {"type": "boolean"},
                "battery_issue":        {"type": "boolean"},
                "sudden_deceleration":  {"type": "boolean"},
                "fuel_anomaly":         {"type": "boolean"},
                "anomaly_detected":     {"type": "boolean"},
                "speed_normalized":     {"type": "float"},
                "rpm_normalized":       {"type": "float"},
                "temp_normalized":      {"type": "float"},
                "processed_at":         {"type": "date"},
                "enrichment_latency_ms":{"type": "float"},
            }
        },
    },
}


class ElasticsearchWriterService:
    def __init__(self):
        self.es = Elasticsearch(ES_HOST)
        self.consumer = Consumer({
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': 'elasticsearch-writer',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
        })
        self.buffer: list[dict] = []

    def _wait_for_es(self):
        # Use a raw HTTP check — same as the Docker healthcheck curl —
        # so we see the real error instead of the client swallowing it.
        health_url = f"{ES_HOST}/_cluster/health"
        for attempt in range(1, 61):
            try:
                with urllib.request.urlopen(health_url, timeout=5) as resp:
                    if resp.status == 200:
                        logger.info("Elasticsearch is ready")
                        return
            except urllib.error.URLError as e:
                logger.info(f"Waiting for Elasticsearch ({attempt}/60): {e.reason}")
            except Exception as e:
                logger.info(f"Waiting for Elasticsearch ({attempt}/60): {e}")
            time.sleep(5)
        raise RuntimeError("Elasticsearch did not become available after 5 minutes")

    def _ensure_template(self):
        self.es.indices.put_index_template(
            name=f"{ES_INDEX_PREFIX}-template",
            body=INDEX_TEMPLATE,
        )
        logger.info("Index template applied")

    def _index_name(self) -> str:
        return f"{ES_INDEX_PREFIX}-{datetime.now(timezone.utc).strftime('%Y.%m.%d')}"

    def _flush(self):
        if not self.buffer:
            return
        try:
            ok, errors = bulk(self.es, self.buffer, raise_on_error=False)
            if errors:
                logger.warning(f"Bulk index: {ok} ok, {len(errors)} errors")
            else:
                logger.info(f"Indexed {ok} documents → {self._index_name()}")
        except Exception as e:
            logger.error(f"Bulk index failed: {e}")
        self.buffer.clear()

    def _handle(self, raw: dict):
        doc = flatten(raw)
        self.buffer.append({"_index": self._index_name(), "_source": doc})
        if len(self.buffer) >= ES_BUFFER_SIZE:
            self._flush()

    def run(self):
        self._wait_for_es()
        self._ensure_template()
        self.consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Consuming from '{KAFKA_TOPIC}', indexing to Elasticsearch")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    self._flush()
                    continue
                if msg.error():
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
                try:
                    self._handle(json.loads(msg.value().decode('utf-8')))
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
        except KeyboardInterrupt:
            pass
        finally:
            self._flush()
            self.consumer.close()
            logger.info("Elasticsearch writer shut down")


if __name__ == '__main__':
    ElasticsearchWriterService().run()

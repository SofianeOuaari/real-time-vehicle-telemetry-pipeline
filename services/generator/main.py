"""
Car Sensor Telemetry Generator

Main entry point for generating and streaming car sensor data to Kafka.
"""
import os
import sys
import time
import logging
import uuid
from datetime import datetime
from typing import Dict

# Add shared directory to path
sys.path.insert(0, '/app/shared')

from generators.sensor_generator import CarSensorGenerator
from generators.driving_patterns import FleetPatternCoordinator
from generators.anomaly_injector import AnomalyInjector
from kafka_producer.producer import TelemetryProducer
from schemas.telemetry_schema import RawTelemetry, SensorData, LocationData


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TelemetryGeneratorService:
    """Main service for generating car sensor telemetry"""

    def __init__(self):
        # Load configuration from environment
        self.kafka_bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'car-telemetry-raw')
        self.num_vehicles = int(os.getenv('GENERATOR_NUM_VEHICLES', '50'))
        self.interval_seconds = float(os.getenv('GENERATION_INTERVAL', '1'))
        self.anomaly_probability = float(os.getenv('GENERATOR_ANOMALY_PROBABILITY', '0.08'))
        self.run_duration = int(os.getenv('RUN_DURATION', '240'))  # Default 4 minutes

        logger.info(f"Initializing Telemetry Generator Service")
        logger.info(f"  Kafka: {self.kafka_bootstrap_servers}")
        logger.info(f"  Topic: {self.kafka_topic}")
        logger.info(f"  Vehicles: {self.num_vehicles}")
        logger.info(f"  Interval: {self.interval_seconds}s")
        logger.info(f"  Anomaly probability: {self.anomaly_probability}")
        logger.info(f"  Run duration: {self.run_duration}s")

        # Initialize components
        self.producer = TelemetryProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            topic=self.kafka_topic
        )
        self.fleet_coordinator = FleetPatternCoordinator(self.num_vehicles)
        self.anomaly_injector = AnomalyInjector(anomaly_probability=self.anomaly_probability)
        self.vehicle_generators = {}

        # Initialize sensor generators for each vehicle
        for i in range(self.num_vehicles):
            vehicle_id = f"VEH{i:04d}"
            self.vehicle_generators[vehicle_id] = CarSensorGenerator(vehicle_id, seed=i)

        logger.info(f"Initialized {self.num_vehicles} vehicle generators")

    def generate_telemetry_message(self, vehicle_id: str) -> Dict:
        """Generate a complete telemetry message for a vehicle"""
        # Get sensor generator for vehicle
        generator = self.vehicle_generators[vehicle_id]

        # Get current driving mode
        driving_mode = self.fleet_coordinator.get_vehicle_mode(vehicle_id)

        # Generate sensor reading
        sensor_reading = generator.generate_sensor_reading(driving_mode)

        # Inject anomaly if applicable
        if self.anomaly_injector.should_inject_anomaly(vehicle_id):
            sensor_reading = self.anomaly_injector.inject_anomaly(vehicle_id, sensor_reading)
            anomaly_info = self.anomaly_injector.get_active_anomaly_info(vehicle_id)
            if anomaly_info:
                logger.debug(
                    f"Injected {anomaly_info['type'].value} anomaly for {vehicle_id} "
                    f"(severity: {anomaly_info['severity']:.2f})"
                )

        # Generate location
        location = generator.generate_location()

        # Create OpenTelemetry trace context
        trace_id = str(uuid.uuid4())
        span_id = str(uuid.uuid4())[:16]

        # Create Pydantic models
        sensor_data = SensorData(**sensor_reading)
        location_data = LocationData(**location)

        # Create raw telemetry message
        telemetry = RawTelemetry(
            vehicle_id=vehicle_id,
            timestamp=datetime.utcnow(),
            trace_id=trace_id,
            span_id=span_id,
            sensor_data=sensor_data,
            location=location_data,
            metadata={
                "sensor_version": "v1.0",
                "data_quality_score": 0.95,
                "driving_mode": driving_mode
            }
        )

        return telemetry.model_dump(mode='json')

    def run(self):
        """Main run loop"""
        logger.info("Starting telemetry generation...")

        start_time = time.time()
        iteration = 0

        try:
            while True:
                # Check if we should stop based on run duration
                elapsed_time = time.time() - start_time
                if elapsed_time >= self.run_duration:
                    logger.info(f"Run duration ({self.run_duration}s) reached. Stopping...")
                    break

                iteration += 1
                batch_start = time.time()

                # Generate telemetry for all vehicles
                for vehicle_id in self.vehicle_generators.keys():
                    try:
                        # Generate telemetry message
                        message = self.generate_telemetry_message(vehicle_id)

                        # Send to Kafka with vehicle_id as key for partitioning
                        self.producer.send(
                            value=message,
                            key=vehicle_id,
                            headers={
                                "trace_id": message["trace_id"],
                                "span_id": message["span_id"]
                            }
                        )

                    except Exception as e:
                        logger.error(f"Error generating telemetry for {vehicle_id}: {e}")

                # Log progress
                if iteration % 10 == 0:
                    stats = self.producer.get_stats()
                    logger.info(
                        f"Iteration {iteration}: Generated {stats['messages_sent']} messages "
                        f"(Success rate: {stats['success_rate']:.2%})"
                    )

                # Sleep to maintain desired interval
                batch_duration = time.time() - batch_start
                sleep_time = max(0, self.interval_seconds - batch_duration)
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, stopping...")
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
        finally:
            self.shutdown()

    def shutdown(self):
        """Graceful shutdown"""
        logger.info("Shutting down telemetry generator...")
        self.producer.close()
        logger.info("Shutdown complete")


def main():
    """Entry point"""
    service = TelemetryGeneratorService()
    service.run()


if __name__ == "__main__":
    main()

"""
Kafka Producer Wrapper

Handles Kafka message production with error handling and retry logic.
"""
import json
import logging
from typing import Dict, Optional
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


logger = logging.getLogger(__name__)


class TelemetryProducer:
    """Kafka producer for telemetry data"""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        batch_size: int = 16384,
        linger_ms: int = 100
    ):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Topic to produce to
            batch_size: Batch size for producer
            linger_ms: Linger time in milliseconds
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers

        # Producer configuration
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'telemetry-generator',
            'compression.type': 'snappy',
            'batch.size': batch_size,
            'linger.ms': linger_ms,
            'acks': 'all',  # Wait for all replicas
            'retries': 3,
        }

        self.producer = Producer(self.config)
        self.message_count = 0
        self.error_count = 0

        logger.info(f"Initialized Kafka producer for topic: {topic}")

        # Create topic if it doesn't exist
        self._ensure_topic_exists()

    def _ensure_topic_exists(self) -> None:
        """Create topic if it doesn't exist"""
        try:
            admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})

            # Get existing topics
            metadata = admin_client.list_topics(timeout=10)

            if self.topic not in metadata.topics:
                logger.info(f"Creating topic: {self.topic}")
                topic_config = NewTopic(
                    topic=self.topic,
                    num_partitions=6,
                    replication_factor=1,
                    config={
                        'compression.type': 'snappy',
                        'retention.ms': str(7 * 24 * 60 * 60 * 1000)  # 7 days
                    }
                )
                admin_client.create_topics([topic_config])
                logger.info(f"Topic {self.topic} created successfully")
        except Exception as e:
            logger.warning(f"Could not create topic {self.topic}: {e}")

    def send(
        self,
        value: Dict,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Send message to Kafka.

        Args:
            value: Message value (will be JSON serialized)
            key: Message key for partitioning
            headers: Optional message headers

        Returns:
            True if message queued successfully, False otherwise
        """
        try:
            # Serialize value to JSON
            value_bytes = json.dumps(value, default=str).encode('utf-8')

            # Encode key if provided
            key_bytes = key.encode('utf-8') if key else None

            # Convert headers to list of tuples
            kafka_headers = None
            if headers:
                kafka_headers = [(k, v.encode('utf-8')) for k, v in headers.items()]

            # Send message
            self.producer.produce(
                topic=self.topic,
                value=value_bytes,
                key=key_bytes,
                headers=kafka_headers,
                on_delivery=self._delivery_callback
            )

            # Trigger callbacks for delivered messages
            self.producer.poll(0)

            return True

        except BufferError as e:
            logger.error(f"Producer queue full: {e}")
            self.error_count += 1
            # Wait for queue to drain
            self.producer.flush(timeout=5)
            return False
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            self.error_count += 1
            return False

    def _delivery_callback(self, err, msg):
        """Callback for message delivery confirmation"""
        if err:
            logger.error(f"Message delivery failed: {err}")
            self.error_count += 1
        else:
            self.message_count += 1
            if self.message_count % 100 == 0:
                logger.info(
                    f"Delivered {self.message_count} messages "
                    f"(partition: {msg.partition()}, offset: {msg.offset()})"
                )

    def flush(self, timeout: int = 10) -> int:
        """
        Flush pending messages.

        Args:
            timeout: Timeout in seconds

        Returns:
            Number of messages still in queue
        """
        remaining = self.producer.flush(timeout=timeout)
        if remaining > 0:
            logger.warning(f"{remaining} messages still in queue after flush")
        else:
            logger.info(f"All messages flushed successfully ({self.message_count} total)")
        return remaining

    def get_stats(self) -> Dict:
        """Get producer statistics"""
        return {
            "messages_sent": self.message_count,
            "errors": self.error_count,
            "success_rate": self.message_count / (self.message_count + self.error_count)
            if (self.message_count + self.error_count) > 0 else 0
        }

    def close(self):
        """Close producer and flush pending messages"""
        logger.info("Closing Kafka producer...")
        self.flush()
        logger.info(f"Producer stats: {self.get_stats()}")

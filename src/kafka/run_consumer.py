"""
Kafka Consumer entry point

Khởi tạo Kafka consumers cho server events, bao gồm:
- Khởi tạo các state managers (singleton)
- Khởi tạo Kafka infrastructure (config, factories, DLQ publisher)
- Inject managers vào ServerRegistry
- Start consumers (blocking)
"""
from src.kafka.consumer.infrastructure.KafkaConfig import KafkaConfig
from src.kafka.consumer.infrastructure.KafkaConsumerFactory import KafkaConsumerFactory
from src.kafka.consumer.infrastructure.KafkaProducerFactory import KafkaProducerFactory
from src.kafka.consumer.infrastructure.DeadLetterPublisher import DeadLetterPublisher
from src.kafka.consumer.server.ServerRegistry import ServerRegistry
from src.kafka.shared.base.JsonSerializer import JsonSerializer

from src.socketio.socketio_server.main.manager.ReceiverManager import ReceiverManager
from src.socketio.socketio_server.main.manager.SenderManager import SenderManager
from src.socketio.socketio_server.main.manager.PairManager import PairManager
from src.socketio.socketio_server.main.manager.JobManager import JobManager
from src.socketio.socketio_server.main.manager.WorkerManager import WorkerManager


def main():
    """
    Entry point cho Kafka consumer process.

    Flow:
        1. Khởi tạo state managers
        2. Khởi tạo Kafka infrastructure
        3. Tạo ServerRegistry với injected managers
        4. Start consumers (blocks until shutdown signal)
    """
    # Initialize State Managers
    receiver_manager = ReceiverManager()
    sender_manager = SenderManager()
    pair_manager = PairManager()
    worker_manager = WorkerManager()
    job_manager = JobManager()

    print("[run_consumer] State managers initialized")

    # Initialize Kafka Infrastructure
    config = KafkaConfig()

    # Consumer factory
    consumer_factory = KafkaConsumerFactory(config)

    # Producer for DLQ publishing
    producer_factory = KafkaProducerFactory(config)
    producer = producer_factory.get_instance()

    # DLQ publisher
    serializer = JsonSerializer()
    dlq_publisher = DeadLetterPublisher(producer, serializer)

    print("[run_consumer] Kafka infrastructure initialized")

    # Create Registry with Injected Managers
    registry = ServerRegistry(
        consumer_factory=consumer_factory,
        dlq_publisher=dlq_publisher,
        receiver_manager=receiver_manager,
        sender_manager=sender_manager,
        pair_manager=pair_manager,
        job_manager=job_manager,
        worker_manager=worker_manager,
    )

    # Start Consumers (Blocking)
    print("[run_consumer] Starting consumers...")
    print("[run_consumer] Press Ctrl+C to shutdown")

    try:
        registry.register_all()
    finally:
        dlq_publisher.close()
        producer_factory.close_instance()
        print("[run_consumer] Shutdown complete")


if __name__ == "__main__":
    main()

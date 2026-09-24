from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_transactions: str = "payments.transactions"
    kafka_topic_dlq: str = "payments.dlq"

    bronze_path: str = "./data/bronze"
    silver_path: str = "./data/silver"
    gold_path: str = "./data/gold"
    checkpoint_path: str = "./checkpoints"
    
    producer_max_events: int = 0
    producer_min_delay_ms: int = 200
    producer_max_delay_ms: int = 2000
    
    fault_duplicate_rate: float = 0.05
    fault_late_rate: float = 0.05
    fault_corrupt_rate: float = 0.03

    env: str = "local"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"

settings = Settings()
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

    env: str = "local"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"

settings = Settings()
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic_transactions: str = "payments.transactions"
    kafka_topic_dlq: str = "payments.dlq"

    bronze_path: str = "./data/bronze"
    silver_path: str = "./data/silver"
    gold_path: str = "./data/gold"
    checkpoint_path: str = "./checkpoints"

    env: str = "local"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"

settings = Settings()
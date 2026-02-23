from pydantic import BaseSettings, PostgresDsn, RedisDsn

class Settings(BaseSettings):
    # Pydantic will look for these names in your .env file (all uppercase)
    database_url: PostgresDsn 
    redis_url: RedisDsn
    
    class Config:
        env_file = ".env"

settings = Settings()
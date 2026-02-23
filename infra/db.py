import asyncpg
from typing import Optional 
from .config import settings


class Database:
    def __init__(self):
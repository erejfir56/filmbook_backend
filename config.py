# config.py
# App config and DB connection
import os

class Config:
    SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL', 'mysql+pymysql://user:password@localhost/filmbook')
    SQLALCHEMY_TRACK_MODIFICATIONS = False

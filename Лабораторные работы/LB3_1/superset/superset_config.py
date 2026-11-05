from pathlib import Path
SECRET_KEY = Path('/run/lab/superset_key').read_text().strip()
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/superset"
WTF_CSRF_ENABLED = True
CACHE_CONFIG = {"CACHE_TYPE": "RedisCache", "CACHE_REDIS_URL": "redis://redis:6379/0"}
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": True}

SESSION_COOKIE_NAME = "titanic17_superset_session"

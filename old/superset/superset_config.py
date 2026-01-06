# Superset config override to force postgres metadata DB

SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
SQLALCHEMY_TRACK_MODIFICATIONS = False

SECRET_KEY = "mysecret"

# Disable SQLite + fix session rollback issues
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True
}

# Allow CORS (optional)
ENABLE_CORS = True

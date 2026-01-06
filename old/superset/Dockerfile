FROM apache/superset:3.0.0

USER root

# Install working versions compatible with Superset 3.0.0
RUN pip install --no-cache-dir \
    "snowflake-connector-python==3.2.0" \
    "snowflake-sqlalchemy==1.4.7" \
    "cryptography==39.0.1" \
    "sqlalchemy==1.4.52"

USER superset

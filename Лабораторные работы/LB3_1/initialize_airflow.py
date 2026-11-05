"""Keep a visible Airflow Connection in the metadata DB as well as env configuration."""
import os
from airflow.models.connection import Connection
from airflow.utils.session import create_session
with create_session() as session:
    if not session.query(Connection).filter_by(conn_id='analytics_postgres').first():
        session.add(Connection(conn_id='analytics_postgres',uri=os.environ['AIRFLOW_CONN_ANALYTICS_POSTGRES']))

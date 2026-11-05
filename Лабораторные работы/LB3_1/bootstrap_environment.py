"""Persistent local keys and writable data volume, created on the first compose up."""
import os
import secrets
from pathlib import Path
root=Path('/run/lab')
root.mkdir(exist_ok=True)
for filename,variable in [('airflow_key','AIRFLOW_SECRET_KEY'),('superset_key','SUPERSET_SECRET_KEY')]:
    path=root/filename
    if not path.exists():
        path.write_text(os.environ.get(variable) or secrets.token_hex(32))
    path.chmod(0o644)
data=Path('/data')
data.mkdir(exist_ok=True)
os.chown(data,50000,0)
data.chmod(0o775)

from pathlib import Path
import nbformat
from nbclient import NotebookClient
import traceback, sys
p = Path(sys.argv[1]).resolve()
nb = nbformat.read(p, as_version=4)
try:
    client = NotebookClient(nb, timeout=240, kernel_name='python3', resources={'metadata': {'path': str(p.parent)}})
    client.execute()
    print('EXECUTION_OK')
except Exception:
    traceback.print_exc()
    print('EXECUTION_FAILED')
finally:
    nbformat.write(nb, p)
    print(f'SAVED {p}')

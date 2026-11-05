"""Convert the supplied JSON export to the native Superset ZIP format.
Usage: python3 superset/json_to_zip.py superset/dashboard_export.json /tmp/dashboard.zip
JSON is valid YAML, so each exported object is stored without losing its structure.
"""
import json
import sys
import zipfile
from pathlib import PurePosixPath


def convert(source, destination):
    with open(source, encoding='utf-8') as file:
        payload = json.load(file)
    if not isinstance(payload, dict) or not payload:
        raise ValueError('Expected a nonempty export object')
    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, obj in payload.items():
            path = PurePosixPath(name)
            if path.is_absolute() or '..' in path.parts or path.suffix != '.yaml':
                raise ValueError('Invalid export path')
            archive.writestr(name, json.dumps(obj, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        raise SystemExit('Usage: json_to_zip.py SOURCE.json DESTINATION.zip')
    convert(sys.argv[1], sys.argv[2])

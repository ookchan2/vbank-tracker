# scripts/generate_site.py
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import database

logger = logging.getLogger(__name__)

DATA_JSON_PATH = str(Path(__file__).parent.parent / "docs" / "data.json")


def generate_site():
    database.init_db()
    database.export_to_json(DATA_JSON_PATH)
    logger.info("✅ docs/data.json generated")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_site()
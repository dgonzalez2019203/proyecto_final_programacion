# -*- coding: utf-8 -*-
"""Configuración común de pytest: rutas de import y conexión a la base SQLite del prototipo."""
import sqlite3
import sys
from pathlib import Path

import pytest

BASE = Path(__file__).resolve().parents[1]
for carpeta in ("pipeline", "api"):
    sys.path.insert(0, str(BASE / carpeta))

from config import DB_PATH  # noqa: E402


@pytest.fixture(scope="session")
def con():
    if not DB_PATH.exists():
        pytest.skip("No existe data/novabank.db: ejecute primero python pipeline/ejecutar_pipeline.py")
    c = sqlite3.connect(DB_PATH)
    yield c
    c.close()

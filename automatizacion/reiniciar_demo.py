# -*- coding: utf-8 -*-
"""Deja la base lista para la demo: borra las acciones ejecutadas y vuelve todas las recomendaciones a 'pendiente'.

Así, al correr la automatización en vivo, el tablero muestra cómo pasan de pendiente a enviada.
Ejecución:  python automatizacion/reiniciar_demo.py
"""
import sqlite3
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
from config import DB_PATH  # noqa: E402

con = sqlite3.connect(DB_PATH)
borradas = con.execute("DELETE FROM gold_acciones_enviadas").rowcount
con.execute("UPDATE gold_siguiente_mejor_accion SET estado = 'pendiente'")
con.commit()
con.close()
print(f"Listo: {borradas} acciones borradas y todas las recomendaciones en 'pendiente'.")

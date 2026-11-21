# -*- coding: utf-8 -*-
"""Orquestador del pipeline NovaBank MVP: corre los seis pasos de principio a fin.

  1. generar_datos   fuentes sintéticas -> Bronze (archivos)
  2. validar_datos   rangos, nulos, duplicados y distribuciones -> reporte
  3. ingesta         Bronze -> SQLite bronze_* y silver_* (+ Parquet)
  4. transformar     Silver -> gold_features_cliente
  5. modelos         entrena NBO, abandono y fraude -> modelos/ y gold_metricas_modelos
  6. predecir        predicciones, motor de decisión, trazabilidad y KPI -> gold_*

Ejecución:  python pipeline/ejecutar_pipeline.py [--semilla N] [--clientes N] [--sin-generar]

Con otra semilla (o más clientes) los datos sintéticos cambian, y con ellos los modelos, las predicciones y
el tablero. Sin argumentos se usa la semilla 2026 y 1,200 clientes. Cada corrida queda en registro_ejecuciones.
"""
import argparse
import os
import sqlite3
import sys
import time
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8")

_ap = argparse.ArgumentParser(description="Pipeline NovaBank MVP de principio a fin")
_ap.add_argument("--semilla", type=int, help="semilla de los datos sintéticos (por defecto 2026)")
_ap.add_argument("--clientes", type=int, help="número de clientes, mínimo 1000 (por defecto 1200)")
_ap.add_argument("--sin-generar", action="store_true", help="reutiliza los archivos Bronze existentes")
ARGS = _ap.parse_args()
if ARGS.clientes is not None and ARGS.clientes < 1000:
    _ap.error("--clientes debe ser al menos 1000 (lo exige el reto y la validación)")
# config.py lee estas variables al importarse, por eso se fijan antes de importar los pasos
if ARGS.semilla is not None:
    os.environ["NOVABANK_SEMILLA"] = str(ARGS.semilla)
if ARGS.clientes is not None:
    os.environ["NOVABANK_CLIENTES"] = str(ARGS.clientes)

import generar_datos
import validar_datos
import ingesta
import transformar
import modelos
import predecir
from config import DB_PATH, N_CLIENTES, SEED, VERSION_NBO

PASOS = [("Generar datos sintéticos", generar_datos.main), ("Validar datos", validar_datos.main), ("Ingesta Bronze → Silver (SQLite)", ingesta.main),
         ("Transformar → Gold features", transformar.main), ("Entrenar modelos", modelos.main), ("Predecir y decidir", predecir.main)]


def main(sin_generar=False):
    inicio = time.time()
    for i, (nombre, fn) in enumerate(PASOS, 1):
        if sin_generar and i == 1:
            continue
        print(f"\n=== Paso {i}/{len(PASOS)}: {nombre} ===")
        t = time.time()
        codigo = fn()
        if codigo:
            print(f"El paso {i} terminó con errores (código {codigo}). Pipeline detenido.")
            return codigo
        print(f"    ({time.time() - t:.1f} s)")
    duracion = time.time() - inicio
    registrar_ejecucion(duracion)
    print(f"\nPipeline completo en {duracion:.1f} s (semilla {SEED}, {N_CLIENTES:,} clientes).")
    return 0


def registrar_ejecucion(duracion):
    """Deja la corrida en registro_ejecuciones: el tablero la muestra y así detecta que hay datos nuevos."""
    con = sqlite3.connect(DB_PATH)
    con.execute("CREATE TABLE IF NOT EXISTS registro_ejecuciones (ts TEXT, semilla INTEGER, clientes INTEGER, duracion_s REAL, modelo_nbo TEXT)")
    con.execute("INSERT INTO registro_ejecuciones VALUES (?,?,?,?,?)",
                (datetime.now().isoformat(timespec="seconds"), SEED, N_CLIENTES, round(duracion, 1), VERSION_NBO))
    con.commit()
    con.close()


if __name__ == "__main__":
    sys.exit(main(ARGS.sin_generar))

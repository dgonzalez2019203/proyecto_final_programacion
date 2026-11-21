# -*- coding: utf-8 -*-
"""Configuración compartida del pipeline NovaBank MVP (Miniproyecto #3).

Todo el prototipo trabaja con datos SINTÉTICOS. Las rutas y constantes de aquí
las usan el generador, la ingesta, el modelado, la API y las pruebas.
"""
import os
from datetime import date, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DATA = BASE / "data"
BRONZE = DATA / "bronze"          # datos crudos tal como llegan de cada fuente (CSV, JSON, JSONL)
SILVER = DATA / "silver"          # copia en Parquet (formato abierto) de las tablas limpias
DB_PATH = DATA / "novabank.db"    # base SQLite con las tres capas: bronze_*, silver_*, gold_*
MODELOS = BASE / "modelos"        # modelos entrenados (.joblib) y métricas
REPORTES = DATA / "reportes"      # reporte de validación de datos

# Se pueden cambiar sin tocar el código (lo usa ejecutar_pipeline.py --semilla N --clientes N)
SEED = int(os.environ.get("NOVABANK_SEMILLA", 2026))
N_CLIENTES = int(os.environ.get("NOVABANK_CLIENTES", 1200))
FECHA_FIN = date(2026, 11, 10)    # fecha de corte del prototipo
DIAS = 180
FECHA_INI = FECHA_FIN - timedelta(days=DIAS - 1)
FECHAS = [FECHA_INI + timedelta(days=i) for i in range(DIAS)]
PART_INGESTA = f"fecha_ingesta={FECHA_FIN.isoformat()}"

# Versiones de los modelos (trazabilidad)
VERSION_NBO = "nbo_hgb_v1.1.0"
VERSION_ABANDONO = "abandono_logreg_v1.1.0"
VERSION_FRAUDE = "fraude_iforest_v1.1.0"

PRODUCTOS = {
    # producto: (columna de tenencia, nombre visible)
    "tarjeta_credito": ("tiene_tarjeta_credito", "Tarjeta de crédito"),
    "prestamo_personal": ("tiene_prestamo", "Préstamo personal"),
    "cuenta_ahorro_meta": ("tiene_ahorro", "Cuenta de ahorro con meta"),
    "seguro_vida": ("tiene_seguro", "Seguro de vida"),
    "plazo_fijo": ("tiene_plazo_fijo", "Depósito a plazo fijo"),
    "credito_pyme": ("tiene_credito_pyme", "Crédito PYME"),
}
PRODUCTOS_CREDITO = ["tarjeta_credito", "prestamo_personal", "credito_pyme"]
CANALES_OFERTA = ["push_app", "email", "asesor"]
SEGMENTOS = ["Joven digital", "Profesional", "Emprendedor", "Familiar", "Senior"]

# Fuentes de Bronze: nombre lógico -> (carpeta, archivo). Las usan la ingesta y las pruebas.
FUENTES_BRONZE = {
    "clientes": ("core_bancario", "clientes.csv"),
    "saldos_mensuales": ("core_bancario", "saldos_mensuales.csv"),
    "transacciones": ("switch_tarjetas", "transacciones.csv"),
    "sesiones_diarias": ("app_movil", "sesiones_diarias.csv"),
    "eventos_app": ("app_movil", "eventos_app.jsonl"),
    "buro_credito": ("buro_credito", "respuestas_buro.json"),
    "campanas_historial": ("crm_campanas", "ofertas_historicas.csv"),
    "etiquetas_abandono": ("crm_campanas", "bajas_observadas.csv"),
    "contracargos": ("switch_tarjetas", "contracargos_confirmados.csv"),
    "interacciones": ("call_center", "interacciones.csv"),
}

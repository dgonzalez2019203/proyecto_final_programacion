# -*- coding: utf-8 -*-
"""Pruebas de ingesta: comprueban que los datos llegan completos y correctos de Bronze a SQLite y a Silver.

Cubre lo que pide el Miniproyecto #3: número de registros, columnas y tipos, nulos y duplicados.
"""
import pandas as pd
import pytest

from config import FUENTES_BRONZE, PRODUCTOS
from ingesta import PII, leer_bronze

LLAVES = {"clientes": ["cliente_id"], "transacciones": ["txn_id"], "campanas_historial": ["oferta_id"], "interacciones": ["interaccion_id"],
          "saldos_mensuales": ["cliente_id", "anio_mes"], "sesiones_diarias": ["cliente_id", "fecha"], "eventos_app": ["session_id"],
          "buro_credito": ["cliente_id"], "etiquetas_abandono": ["cliente_id"], "contracargos": ["txn_id"]}


def tabla(con, nombre):
    return pd.read_sql(f"SELECT * FROM {nombre}", con)


# --- 1. Número de registros: cada archivo de Bronze llega completo a SQLite --------------------
@pytest.mark.parametrize("fuente", list(FUENTES_BRONZE))
def test_conteo_bronze_igual_archivo(con, fuente):
    archivo = leer_bronze(fuente)
    n_db = con.execute(f"SELECT COUNT(*) FROM bronze_{fuente}").fetchone()[0]
    assert n_db == len(archivo), f"bronze_{fuente}: {n_db} filas en SQLite vs {len(archivo)} en el archivo"
    assert n_db > 0


def test_conteo_minimo_clientes(con):
    assert con.execute("SELECT COUNT(*) FROM bronze_clientes").fetchone()[0] >= 1000


# --- 2. Columnas y tipos ------------------------------------------------------------------------
@pytest.mark.parametrize("fuente", list(FUENTES_BRONZE))
def test_columnas_bronze(con, fuente):
    esperadas = set(leer_bronze(fuente).columns) | {"fecha_ingesta"}
    assert set(tabla(con, f"bronze_{fuente}").columns) == esperadas


def test_tipos_silver_transacciones(con):
    tx = tabla(con, "silver_transacciones")
    assert pd.api.types.is_float_dtype(tx.monto_gtq)
    assert pd.to_datetime(tx.ts, errors="coerce").notna().all(), "todas las fechas deben ser válidas"
    assert set(tx.tipo.unique()) == {"debito", "credito"}
    assert (tx.dist_km_hogar >= 0).all()
    assert set(tx.dispositivo_nuevo.unique()) <= {0, 1}


def test_tipos_silver_clientes(con):
    cli = tabla(con, "silver_clientes")
    for col in ["edad", "antiguedad_meses", "anio_nacimiento"]:
        assert pd.api.types.is_integer_dtype(cli[col]), col
    assert pd.api.types.is_float_dtype(cli.ingreso_mensual_gtq)
    for col, _ in PRODUCTOS.values():
        assert set(cli[col].unique()) <= {0, 1}, col


def test_silver_buro_aplanado_desde_json(con):
    buro = tabla(con, "silver_buro_credito")
    con_historial = buro[buro.sin_historial_buro == 0]
    assert con_historial.score_buro.between(300, 850).all()
    assert buro[buro.sin_historial_buro == 1].score_buro.isna().all()


# --- 3. Nulos ------------------------------------------------------------------------------------
@pytest.mark.parametrize("tabla_nombre,columnas", [
    ("silver_clientes", ["cliente_id", "segmento", "ingreso_mensual_gtq", "departamento"]),
    ("silver_transacciones", ["txn_id", "cliente_id", "ts", "monto_gtq", "categoria", "canal"]),
    ("silver_campanas_historial", ["oferta_id", "cliente_id", "producto", "canal", "aceptada"]),
    ("gold_features_cliente", ["cliente_id", "segmento", "gasto_90d", "n_sesiones_30d", "saldo_prom_3m"]),
])
def test_sin_nulos_en_columnas_obligatorias(con, tabla_nombre, columnas):
    df = tabla(con, tabla_nombre)
    nulos = df[columnas].isna().sum()
    assert int(nulos.sum()) == 0, nulos[nulos > 0].to_dict()


# --- 4. Duplicados --------------------------------------------------------------------------------
@pytest.mark.parametrize("fuente,llave", list(LLAVES.items()))
def test_sin_duplicados_llave(con, fuente, llave):
    df = tabla(con, f"bronze_{fuente}")
    assert int(df.duplicated(subset=llave).sum()) == 0


def test_un_cliente_por_fila_en_gold(con):
    feat = tabla(con, "gold_features_cliente")
    assert feat.cliente_id.is_unique
    assert len(feat) == con.execute("SELECT COUNT(*) FROM silver_clientes").fetchone()[0]


# --- 5. Integridad entre capas ---------------------------------------------------------------------
def test_silver_no_contiene_datos_personales(con):
    cols = set(tabla(con, "silver_clientes").columns)
    assert not (cols & set(PII)), "Silver no debe contener nombre, DPI, correo ni teléfono en claro"
    assert {f"token_{c}" for c in PII} <= cols


def test_bronze_a_silver_conserva_transacciones(con):
    n_bronze = con.execute("SELECT COUNT(*) FROM bronze_transacciones").fetchone()[0]
    n_silver = con.execute("SELECT COUNT(*) FROM silver_transacciones").fetchone()[0]
    assert n_bronze == n_silver


def test_integridad_referencial_cliente(con):
    huerfanos = con.execute("SELECT COUNT(*) FROM silver_transacciones t LEFT JOIN silver_clientes c USING (cliente_id) WHERE c.cliente_id IS NULL").fetchone()[0]
    assert huerfanos == 0

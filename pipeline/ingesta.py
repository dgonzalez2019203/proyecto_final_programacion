# -*- coding: utf-8 -*-
"""Paso 3 del pipeline: ingesta de la zona Bronze a SQLite y construcción de la zona Silver.

Bronze (bronze_*):  cada archivo crudo se carga tal cual, sin transformar, con una columna
                    fecha_ingesta. Equivale a Cloud Storage en el diseño del MP2.
Silver (silver_*):  tablas limpias y tipadas, con los datos personales seudonimizados (SHA-256)
                    y variables derivadas (distancia al hogar, dispositivo desconocido).
                    Equivale a BigLake + Iceberg. También se escribe una copia Parquet en
                    data/silver/ para conservar un formato abierto.

Ejecución:  python pipeline/ingesta.py
"""
import hashlib
import json
import sqlite3

import numpy as np
import pandas as pd

from config import BRONZE, DB_PATH, FECHA_FIN, FUENTES_BRONZE, PART_INGESTA, PRODUCTOS, SILVER

PII = ["nombre_completo", "dpi", "email", "telefono"]


def conectar():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def tokenizar(valor):
    """Seudonimización irreversible (en producción: Sensitive Data Protection + llave en Cloud KMS)."""
    return hashlib.sha256(f"novabank-salt::{valor}".encode("utf-8")).hexdigest()[:16]


def haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371 * 2 * np.arcsin(np.sqrt(a))


def leer_bronze(fuente):
    """Lee un archivo crudo de Bronze (CSV, JSONL o JSON anidado) como DataFrame plano."""
    carpeta, archivo = FUENTES_BRONZE[fuente]
    ruta = BRONZE / carpeta / PART_INGESTA / archivo
    if archivo.endswith(".csv"):
        return pd.read_csv(ruta)
    if archivo.endswith(".jsonl"):
        filas = [json.loads(l) for l in ruta.read_text(encoding="utf-8").splitlines() if l.strip()]
        # el JSON anidado se guarda como texto en Bronze; Silver lo aplana
        return pd.DataFrame([{"session_id": f["session_id"], "cliente_id": f["cliente_id"], "ts_inicio": f["ts_inicio"],
                              "dispositivo_id": f["dispositivo_id"], "plataforma": f["app"]["plataforma"],
                              "eventos_json": json.dumps(f["eventos"], ensure_ascii=False)} for f in filas])
    datos = json.loads(ruta.read_text(encoding="utf-8"))
    return pd.DataFrame([{"cliente_id": d["cliente_id"], "fecha_consulta": d["fecha_consulta"], "proveedor": d["proveedor"],
                          "respuesta_json": json.dumps(d["resultado"], ensure_ascii=False)} for d in datos])


def guardar(con, nombre, df, parquet=False):
    df.to_sql(nombre, con, if_exists="replace", index=False)
    if parquet:
        carpeta = SILVER / nombre.replace("silver_", "")
        carpeta.mkdir(parents=True, exist_ok=True)
        df.to_parquet(carpeta / "part-00000.parquet", index=False)


# ---------------------------------------------------------------------------
# Bronze -> SQLite
# ---------------------------------------------------------------------------
def cargar_bronze(con):
    conteos = {}
    for fuente in FUENTES_BRONZE:
        df = leer_bronze(fuente)
        df["fecha_ingesta"] = FECHA_FIN.isoformat()
        guardar(con, f"bronze_{fuente}", df)
        conteos[fuente] = len(df)
    return conteos


# ---------------------------------------------------------------------------
# Bronze -> Silver (limpio, tipado, seudonimizado)
# ---------------------------------------------------------------------------
def construir_silver(con):
    cli = pd.read_sql("SELECT * FROM bronze_clientes", con)
    s_cli = cli.drop(columns=PII + ["fecha_ingesta"]).copy()
    for col in PII:
        s_cli[f"token_{col}"] = cli[col].map(tokenizar)
    s_cli["anio_nacimiento"] = cli.fecha_nacimiento.str[:4].astype(int)
    s_cli = s_cli.drop(columns=["fecha_nacimiento", "genero"])      # género va a una tabla aparte de atributos sensibles
    for col in ["urbano", "empleo_formal", "recibe_remesas", "consentimiento_marketing"] + [c for c, _ in PRODUCTOS.values()]:
        s_cli[col] = s_cli[col].astype(int)
    guardar(con, "silver_clientes", s_cli, parquet=True)
    guardar(con, "silver_clientes_atributos_sensibles", cli[["cliente_id", "genero"]], parquet=True)

    tx = pd.read_sql("SELECT * FROM bronze_transacciones", con).drop(columns=["fecha_ingesta"])
    tx["ts"] = pd.to_datetime(tx.ts)
    tx["dispositivo_id"] = tx.dispositivo_id.fillna("")
    hogar = cli.set_index("cliente_id")[["lat_hogar", "lon_hogar"]].loc[tx.cliente_id]
    tx["dist_km_hogar"] = np.round(haversine_km(tx.lat.to_numpy(), tx.lon.to_numpy(), hogar.lat_hogar.to_numpy(), hogar.lon_hogar.to_numpy()), 1)
    conocidos = dict(zip(cli.cliente_id, zip(cli.dispositivo_principal, cli.dispositivo_secundario.fillna(""))))
    tx["dispositivo_nuevo"] = [int(bool(d) and d not in conocidos[c]) for c, d in zip(tx.cliente_id, tx.dispositivo_id)]
    tx["ts"] = tx.ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    guardar(con, "silver_transacciones", tx, parquet=True)

    guardar(con, "silver_etiquetas_fraude", pd.read_sql("SELECT txn_id, cliente_id, fuente FROM bronze_contracargos", con), parquet=True)
    guardar(con, "silver_saldos_mensuales", pd.read_sql("SELECT cliente_id, anio_mes, saldo_promedio_gtq FROM bronze_saldos_mensuales", con), parquet=True)
    guardar(con, "silver_sesiones_diarias", pd.read_sql("SELECT cliente_id, fecha, n_sesiones, hora_promedio FROM bronze_sesiones_diarias", con), parquet=True)

    ev = pd.read_sql("SELECT * FROM bronze_eventos_app", con)
    filas = []
    for r in ev.itertuples():
        evs = json.loads(r.eventos_json)
        oferta = next((x for x in evs if x["tipo"] == "ver_oferta"), None)
        filas.append({"session_id": r.session_id, "cliente_id": r.cliente_id, "ts_inicio": r.ts_inicio, "plataforma": r.plataforma,
                      "n_eventos": len(evs), "duracion_seg": evs[-1]["t"], "hizo_transferencia": int(any(x["tipo"] == "transferencia" for x in evs)),
                      "producto_ofertado": oferta["producto"] if oferta else None, "respuesta_oferta": oferta["respuesta"] if oferta else None})
    guardar(con, "silver_sesiones_app", pd.DataFrame(filas), parquet=True)

    buro = pd.read_sql("SELECT * FROM bronze_buro_credito", con)
    resp = buro.respuesta_json.map(json.loads)
    s_buro = pd.DataFrame({"cliente_id": buro.cliente_id, "fecha_consulta": buro.fecha_consulta,
                           "sin_historial_buro": resp.isna().astype(int),
                           "score_buro": resp.map(lambda d: d["score"] if d else None),
                           "max_dias_mora_12m": resp.map(lambda d: d["max_dias_mora_12m"] if d else None),
                           "deuda_total_gtq": resp.map(lambda d: d["deuda_total_gtq"] if d else None),
                           "consultas_6m": resp.map(lambda d: d["consultas_6m"] if d else None)})
    guardar(con, "silver_buro_credito", s_buro, parquet=True)

    guardar(con, "silver_campanas_historial", pd.read_sql("SELECT oferta_id, cliente_id, fecha, producto, canal, aceptada FROM bronze_campanas_historial", con), parquet=True)
    guardar(con, "silver_etiquetas_abandono", pd.read_sql("SELECT cliente_id, baja_60d, fuente FROM bronze_etiquetas_abandono", con), parquet=True)
    inter = pd.read_sql("SELECT interaccion_id, cliente_id, canal, ts, intencion, texto, sentimiento FROM bronze_interacciones", con)
    inter["modelo_nlp"] = "simulado (producción: Gemini / Speech-to-Text)"
    guardar(con, "silver_interacciones", inter, parquet=True)


def main():
    con = conectar()
    conteos = cargar_bronze(con)
    print("Bronze cargado a SQLite:", ", ".join(f"{k}={v:,}" for k, v in conteos.items()))
    construir_silver(con)
    tablas = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'silver_%'", con).name.tolist()
    print(f"Silver construido ({len(tablas)} tablas): {', '.join(tablas)}")
    con.close()


if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""Paso 4 del pipeline: transforma Silver en la tabla de variables (features) de la zona Gold.

Construye una fila por cliente con variables de perfil, buró, gasto por categoría, actividad
en la app, saldos y contactos con el banco. Es la tabla que alimenta los tres modelos y la
API. Equivale a Feature Store / tabla Gold de BigQuery en el diseño del MP2.

Ejecución:  python pipeline/transformar.py
"""
import pandas as pd

from config import DB_PATH, FECHA_FIN, PRODUCTOS
from ingesta import conectar

CATS_GASTO = ["viajes", "restaurantes", "ecommerce", "educacion", "servicios_basicos", "supermercado", "entretenimiento_streaming", "transporte_apps"]


def construir_features(con):
    cli = pd.read_sql("SELECT * FROM silver_clientes", con)
    buro = pd.read_sql("SELECT * FROM silver_buro_credito", con).drop(columns=["fecha_consulta"])
    tx = pd.read_sql("SELECT cliente_id, ts, tipo, categoria, monto_gtq, canal FROM silver_transacciones", con)
    ses = pd.read_sql("SELECT * FROM silver_sesiones_diarias", con)
    inter = pd.read_sql("SELECT cliente_id, ts, intencion, sentimiento FROM silver_interacciones", con)
    saldos = pd.read_sql("SELECT * FROM silver_saldos_mensuales", con)

    f = cli.merge(buro, on="cliente_id", how="left").set_index("cliente_id")
    f["ratio_deuda_ingreso"] = (f.deuda_total_gtq / (f.ingreso_mensual_gtq * 12)).round(3)
    f["n_productos"] = 2 + f[[c for c, _ in PRODUCTOS.values()]].sum(axis=1)

    fin = pd.Timestamp(FECHA_FIN) + pd.Timedelta(days=1)
    tx["ts"] = pd.to_datetime(tx.ts)
    deb = tx[tx.tipo == "debito"]
    edad_tx = (fin - deb.ts).dt.days
    g30, g90 = deb[edad_tx <= 30].groupby("cliente_id"), deb[edad_tx <= 90].groupby("cliente_id")
    prev = deb[(edad_tx > 30) & (edad_tx <= 90)].groupby("cliente_id").size()
    f["n_txn_30d"] = g30.size().reindex(f.index, fill_value=0)
    f["gasto_30d"] = g30.monto_gtq.sum().reindex(f.index, fill_value=0).round(2)
    f["gasto_90d"] = g90.monto_gtq.sum().reindex(f.index, fill_value=0).round(2)
    f["var_actividad"] = (f.n_txn_30d / (prev.reindex(f.index, fill_value=0) / 2 + 1)).round(3)
    piv = deb[edad_tx <= 90].pivot_table(index="cliente_id", columns="categoria", values="monto_gtq", aggfunc="sum")
    for cat in CATS_GASTO:
        f[f"gasto_{cat}_90d"] = piv.get(cat, pd.Series(dtype=float)).reindex(f.index).fillna(0).round(2)
    dig = deb[(edad_tx <= 90) & (deb.canal == "ecommerce")].groupby("cliente_id").monto_gtq.sum()
    f["pct_gasto_digital_90d"] = (dig.reindex(f.index, fill_value=0) / (f.gasto_90d + 1)).round(3)
    cred = tx[tx.tipo == "credito"]
    edad_cr = (fin - cred.ts).dt.days
    f["remesas_90d_gtq"] = cred[(cred.categoria == "remesa_recibida") & (edad_cr <= 90)].groupby("cliente_id").monto_gtq.sum().reindex(f.index, fill_value=0).round(2)
    planilla30 = cred[(cred.categoria == "planilla") & (edad_cr <= 30)].groupby("cliente_id").size().reindex(f.index, fill_value=0)
    f["planilla_interrumpida"] = ((f.empleo_formal == 1) & (planilla30 == 0)).astype(int)

    ses["fecha"] = pd.to_datetime(ses.fecha)
    ult = ses.groupby("cliente_id").fecha.max()
    f["dias_sin_login"] = (fin - ult).dt.days.reindex(f.index).fillna(180).astype(int)
    f["n_sesiones_30d"] = ses[(fin - ses.fecha).dt.days <= 30].groupby("cliente_id").n_sesiones.sum().reindex(f.index, fill_value=0).astype(int)
    f["hora_preferida"] = ses.groupby("cliente_id").hora_promedio.agg(lambda s: int(round(s.mean()))).reindex(f.index).fillna(20).astype(int)

    inter["ts"] = pd.to_datetime(inter.ts)
    inter = inter[(fin - inter.ts).dt.days <= 90]
    f["n_reclamos_90d"] = inter[inter.intencion.isin(["reclamo_cobro", "problema_app", "cancelar_cuenta"])].groupby("cliente_id").size().reindex(f.index, fill_value=0).astype(int)
    f["sentimiento_prom_90d"] = inter.groupby("cliente_id").sentimiento.mean().reindex(f.index).fillna(0).round(3)

    sal = saldos.pivot(index="cliente_id", columns="anio_mes", values="saldo_promedio_gtq")
    cols = sorted(sal.columns)
    f["saldo_prom_3m"] = sal[cols[-3:]].mean(axis=1).reindex(f.index).round(2)
    f["tendencia_saldo"] = ((sal[cols[-1]] - sal[cols[-4]]) / (sal[cols[-4]] + 1)).reindex(f.index).round(3)
    f["fecha_features"] = FECHA_FIN.isoformat()
    return f.reset_index()


def main():
    con = conectar()
    feat = construir_features(con)
    feat.to_sql("gold_features_cliente", con, if_exists="replace", index=False)
    print(f"gold_features_cliente: {len(feat):,} clientes x {feat.shape[1]} columnas")
    con.close()


if __name__ == "__main__":
    main()

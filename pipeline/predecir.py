# -*- coding: utf-8 -*-
"""Paso 6 del pipeline: genera la predicción de cada cliente y la acción recomendada en Gold.

1. NBO: puntúa cada cliente x producto que no tiene x canal, elige el mejor canal por producto
   y aplica las reglas R1 a R6 (pipeline/reglas.py).
2. Abandono: probabilidad a 60 días, nivel de riesgo y acción de retención.
3. Fraude: score de anomalía por transacción de los últimos 30 días y decisión (aprobar / 2FA / bloquear).
4. Motor de decisión: arbitra R7 > R8 > oferta > educación y escribe gold_siguiente_mejor_accion.
5. Trazabilidad: gold_registro_predicciones guarda modelo, versión, hash de variables, regla y decisión.

Ejecución:  python pipeline/predecir.py
"""
import hashlib
import json
from datetime import datetime

import joblib
import numpy as np
import pandas as pd

from config import (CANALES_OFERTA, FECHA_FIN, MODELOS, PRODUCTOS, VERSION_ABANDONO, VERSION_FRAUDE, VERSION_NBO)
from ingesta import conectar
from modelos import FEATS_ABANDONO, FEATS_NBO, TEXTO_ABANDONO, features_fraude, matriz_abandono, matriz_nbo
from reglas import DIAS_BLOQUEO_FRAUDE, DIAS_FATIGA, elegir_oferta, siguiente_mejor_accion


def predecir_nbo(feat, campanas, modelo):
    fx = feat.set_index("cliente_id")
    grid = [(c.cliente_id, p, can) for c in feat.itertuples() for p, (col, _) in PRODUCTOS.items() if not getattr(c, col) for can in CANALES_OFERTA]
    grid = pd.DataFrame(grid, columns=["cliente_id", "producto", "canal"])
    grid["prob"] = modelo.predict_proba(matriz_nbo(fx.loc[grid.cliente_id].reset_index(), grid.producto.to_numpy(), grid.canal.to_numpy()))[:, 1]
    mejor_canal = grid.loc[grid.groupby(["cliente_id", "producto"]).prob.idxmax()]
    recientes = set(campanas[pd.to_datetime(campanas.fecha) > pd.Timestamp(FECHA_FIN) - pd.Timedelta(days=DIAS_FATIGA)].cliente_id)
    filas = []
    for cid in feat.cliente_id:
        c = fx.loc[cid]
        g = mejor_canal[mejor_canal.cliente_id == cid].sort_values("prob", ascending=False)
        cliente = {"segmento": c.segmento, "edad": int(c.edad), "sin_historial_buro": bool(c.sin_historial_buro),
                   "score_buro": None if pd.isna(c.score_buro) else float(c.score_buro),
                   "max_dias_mora_12m": None if pd.isna(c.max_dias_mora_12m) else float(c.max_dias_mora_12m),
                   "consentimiento_marketing": bool(c.consentimiento_marketing)}
        candidatos = [{"producto": r.producto, "canal": r.canal, "prob": float(r.prob)} for r in g.itertuples()]
        of = elegir_oferta(candidatos, cliente, oferta_reciente=cid in recientes) if candidatos else \
            {"producto": None, "canal": None, "prob": None, "regla_aplicada": "Cartera completa", "productos_bloqueados": "", "ofrecer": False}
        top3 = "; ".join(f"{PRODUCTOS[r.producto][1]} {r.prob:.0%}" for r in g.head(3).itertuples())
        filas.append({"cliente_id": cid, "producto_recomendado": of["producto"],
                      "producto_nombre": PRODUCTOS[of["producto"]][1] if of["producto"] else None,
                      "prob_aceptacion": None if of["prob"] is None else round(of["prob"], 4), "canal_optimo": of["canal"],
                      "hora_optima": int(c.hora_preferida), "regla_aplicada": of["regla_aplicada"], "ofrecer": int(of["ofrecer"]),
                      "productos_bloqueados": of["productos_bloqueados"], "top3_productos": top3, "modelo_version": VERSION_NBO})
    return pd.DataFrame(filas)


def predecir_abandono(feat, artefacto):
    Xs = np.clip(artefacto["scaler"].transform(matriz_abandono(feat)), -3, 3)
    m = artefacto["modelo"]
    p = m.predict_proba(Xs)[:, 1]
    razon = [TEXTO_ABANDONO[FEATS_ABANDONO[j]] for j in (Xs * m.coef_[0]).argmax(axis=1)]
    valor_alto = ((feat.ingreso_mensual_gtq >= feat.ingreso_mensual_gtq.quantile(.6)) | (feat.saldo_prom_3m >= feat.saldo_prom_3m.quantile(.6))).to_numpy()
    accion = []
    for pi, r, va in zip(p, razon, valor_alto):
        if pi >= .6 and r in ("Reclamos recientes", "Sentimiento negativo en contactos"):
            accion.append("Resolver caso abierto con prioridad + llamada de seguimiento")
        elif pi >= .6 and va:
            accion.append("Llamada de asesor + beneficio de retención")
        elif pi >= .6:
            accion.append("Push personalizado + encuesta de satisfacción")
        elif pi >= .35:
            accion.append("Campaña de reactivación en la app")
        else:
            accion.append("Sin acción (monitorear)")
    return pd.DataFrame({"cliente_id": feat.cliente_id, "prob_abandono_60d": p.round(4),
                         "nivel_riesgo": np.select([p >= .6, p >= .35], ["Alto", "Medio"], "Bajo"), "razon_principal": razon,
                         "accion_recomendada": accion, "modelo_version": VERSION_ABANDONO})


def predecir_fraude(tx, artefacto):
    d, X = features_fraude(tx)
    raw = -artefacto["modelo"].score_samples(X)
    score = np.clip((raw - artefacto["min"]) / (artefacto["max"] - artefacto["min"]), 0, 1)
    d["score_anomalia"] = score.round(4)
    d["decision"] = np.select([score >= artefacto["umbral_bloqueo"], score >= artefacto["umbral_2fa"]], ["Bloquear y notificar", "Solicitar 2FA"], "Aprobar")
    alertas = d[d.decision != "Aprobar"].copy()
    motivos = []
    for r in alertas.itertuples():
        mm = []
        if r.dispositivo_nuevo: mm.append("dispositivo nuevo")
        if r.pais != "GT": mm.append(f"país {r.pais}")
        elif r.dist_km_hogar > 50: mm.append(f"{r.dist_km_hogar:,.0f} km de su zona habitual")
        if r.ts.hour < 5: mm.append("horario de madrugada")
        if r.n_txn_1h >= 3: mm.append(f"{int(r.n_txn_1h)} compras en 1 hora")
        if r.monto_gtq > 3 * (r.mediana_cliente + 1): mm.append("monto atípico vs. su historial")
        motivos.append(", ".join(mm) or "combinación inusual de variables")
    alertas["motivos"] = motivos
    cols = ["txn_id", "cliente_id", "ts", "monto_gtq", "categoria", "canal", "pais", "dist_km_hogar", "dispositivo_nuevo", "n_txn_1h", "score_anomalia", "decision", "motivos"]
    alertas = alertas[cols].assign(modelo_version=VERSION_FRAUDE).sort_values("ts", ascending=False)
    alertas["ts"] = alertas.ts.dt.strftime("%Y-%m-%d %H:%M:%S")
    return alertas


def decidir(feat, nbo, aband, alertas):
    ult = (pd.Timestamp(FECHA_FIN) - pd.Timedelta(days=DIAS_BLOQUEO_FRAUDE - 1)).strftime("%Y-%m-%d")
    bloqueados = set(alertas[(alertas.decision == "Bloquear y notificar") & (alertas.ts >= ult)].cliente_id)
    nb, ab = nbo.set_index("cliente_id"), aband.set_index("cliente_id")
    filas = []
    for cid in feat.cliente_id:
        o = nb.loc[cid]
        oferta = {"producto": o.producto_recomendado, "canal": o.canal_optimo, "prob": o.prob_aceptacion,
                  "regla_aplicada": o.regla_aplicada, "ofrecer": bool(o.ofrecer)} if pd.notna(o.producto_recomendado) else None
        tipo, desc, regla = siguiente_mejor_accion(oferta, float(ab.loc[cid, "prob_abandono_60d"]), ab.loc[cid, "accion_recomendada"],
                                                   cid in bloqueados, o.hora_optima)
        filas.append({"cliente_id": cid, "tipo_accion": tipo, "siguiente_mejor_accion": desc, "regla_decision": regla,
                      "producto": o.producto_nombre if tipo == "Oferta" else None, "canal": o.canal_optimo if tipo == "Oferta" else None,
                      "prob_aceptacion": o.prob_aceptacion if tipo == "Oferta" else None, "prob_abandono_60d": float(ab.loc[cid, "prob_abandono_60d"]),
                      "estado": "pendiente", "fecha_decision": FECHA_FIN.isoformat()})
    return pd.DataFrame(filas)


def registro_predicciones(feat, nbo, aband, alertas):
    ts_batch = f"{FECHA_FIN.isoformat()}T02:00:00"
    fx = feat.set_index("cliente_id")
    hashf = {cid: hashlib.sha256(fx.loc[cid, FEATS_NBO].to_json().encode()).hexdigest()[:12] for cid in fx.index}
    reg = []
    for r in nbo.itertuples():
        if r.producto_recomendado:
            reg.append((r.cliente_id, "nbo", r.modelo_version, ts_batch, json.dumps({"producto": r.producto_recomendado, "prob": r.prob_aceptacion, "canal": r.canal_optimo}),
                        r.regla_aplicada, "oferta_programada" if r.ofrecer else "sin_oferta"))
    for r in aband.itertuples():
        reg.append((r.cliente_id, "abandono", r.modelo_version, ts_batch, json.dumps({"prob": r.prob_abandono_60d, "nivel": r.nivel_riesgo}),
                    f"{r.razon_principal}; umbrales 0.35 / 0.60", r.accion_recomendada))
    for r in alertas.itertuples():
        reg.append((r.cliente_id, "fraude", r.modelo_version, r.ts, json.dumps({"txn_id": r.txn_id, "score": r.score_anomalia}), r.motivos, r.decision))
    out = pd.DataFrame(reg, columns=["cliente_id", "modelo", "version", "ts_prediccion", "salida", "explicacion_o_regla", "decision"])
    out.insert(0, "prediccion_id", [f"PRED-{i + 1:07d}" for i in range(len(out))])
    out["hash_features"] = out.cliente_id.map(hashf)
    out["dataset_version"] = f"gold_features_cliente@{FECHA_FIN.isoformat()}"
    return out


def kpi_diarios(tx, campanas, alertas):
    deb = tx[tx.tipo == "debito"].assign(fecha=tx.ts.dt.strftime("%Y-%m-%d"))
    kpi = deb.groupby("fecha").agg(clientes_activos=("cliente_id", "nunique"), transacciones=("txn_id", "count"), monto_total_gtq=("monto_gtq", "sum")).round(2)
    camp = campanas.groupby("fecha").agg(ofertas=("oferta_id", "count"), aceptadas=("aceptada", "sum"))
    fr = alertas.assign(fecha=alertas.ts.str[:10]).groupby("fecha").size().rename("alertas_fraude")
    return kpi.join(camp).join(fr).fillna(0).reset_index()


def main():
    con = conectar()
    feat = pd.read_sql("SELECT * FROM gold_features_cliente", con)
    campanas = pd.read_sql("SELECT * FROM silver_campanas_historial", con)
    tx = pd.read_sql("SELECT * FROM silver_transacciones", con)
    tx["ts"] = pd.to_datetime(tx.ts)
    nbo = predecir_nbo(feat, campanas, joblib.load(MODELOS / "nbo.joblib"))
    aband = predecir_abandono(feat, joblib.load(MODELOS / "abandono.joblib"))
    alertas = predecir_fraude(tx[tx.ts >= pd.Timestamp(FECHA_FIN) - pd.Timedelta(days=30)], joblib.load(MODELOS / "fraude.joblib"))
    nba = decidir(feat, nbo, aband, alertas)
    registro = registro_predicciones(feat, nbo, aband, alertas)
    kpi = kpi_diarios(tx, campanas, alertas)
    for nombre, df in [("gold_pred_nbo", nbo), ("gold_pred_abandono", aband), ("gold_alertas_fraude", alertas),
                       ("gold_siguiente_mejor_accion", nba), ("gold_registro_predicciones", registro), ("gold_kpi_diarios", kpi)]:
        df.to_sql(nombre, con, if_exists="replace", index=False)
    con.execute("CREATE TABLE IF NOT EXISTS gold_acciones_enviadas (accion_id TEXT, cliente_id TEXT, tipo_accion TEXT, descripcion TEXT, canal TEXT, "
                "estado TEXT, ts_envio TEXT, disparador TEXT, detalle TEXT)")
    con.commit()
    con.close()
    print(f"NBO: {int(nbo.ofrecer.sum()):,} ofertas activas de {len(nbo):,} clientes | Reglas: {nbo.regla_aplicada.value_counts().to_dict()}")
    print(f"Abandono: {aband.nivel_riesgo.value_counts().to_dict()} | Alertas de fraude (30 días): {len(alertas):,}")
    print(f"Siguiente mejor acción: {nba.tipo_accion.value_counts().to_dict()} | Registro de trazabilidad: {len(registro):,} predicciones")


if __name__ == "__main__":
    main()

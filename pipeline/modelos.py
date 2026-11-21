# -*- coding: utf-8 -*-
"""Paso 5 del pipeline: entrena los modelos y guarda sus métricas.

Modelo principal (servicio elegido): siguiente mejor oferta (NBO).
  Gradient boosting que estima la probabilidad de que un cliente acepte un producto por un
  canal, entrenado con las ofertas históricas del CRM (silver_campanas_historial).
Modelos de apoyo para el motor de decisión:
  Abandono a 60 días (regresión logística) y anomalías transaccionales (Isolation Forest).

Salidas: modelos/*.joblib, modelos/metricas.json y la tabla gold_metricas_modelos.
Ejecución:  python pipeline/modelos.py
"""
import json
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from config import (CANALES_OFERTA, MODELOS, PRODUCTOS, SEED, SEGMENTOS, VERSION_ABANDONO, VERSION_FRAUDE, VERSION_NBO)
from ingesta import conectar

EVENTOS_VIDA = ["nuevo_empleo", "graduacion", "mudanza", "boda", "nacimiento"]
FEATS_NBO = ["edad", "ingreso_mensual_gtq", "antiguedad_meses", "recibe_remesas", "score_buro", "max_dias_mora_12m",
             "ratio_deuda_ingreso", "n_productos", "gasto_90d", "gasto_viajes_90d", "gasto_restaurantes_90d",
             "gasto_ecommerce_90d", "gasto_educacion_90d", "gasto_servicios_basicos_90d", "pct_gasto_digital_90d",
             "remesas_90d_gtq", "saldo_prom_3m", "sentimiento_prom_90d", "n_sesiones_30d", "tiene_prestamo"]
FEATS_ABANDONO = ["dias_sin_login", "var_actividad", "tendencia_saldo", "n_reclamos_90d", "sentimiento_prom_90d",
                  "n_productos", "antiguedad_meses", "planilla_interrumpida", "n_sesiones_30d", "edad", "log_ingreso"]
TEXTO_ABANDONO = {"dias_sin_login": "Días sin ingresar a la app", "var_actividad": "Caída de transacciones",
                  "tendencia_saldo": "Saldo en descenso", "n_reclamos_90d": "Reclamos recientes",
                  "sentimiento_prom_90d": "Sentimiento negativo en contactos", "n_productos": "Pocos productos con el banco",
                  "antiguedad_meses": "Cliente relativamente nuevo", "planilla_interrumpida": "Dejó de recibir su planilla",
                  "n_sesiones_30d": "Pocas sesiones en la app", "edad": "Perfil de edad con mayor rotación", "log_ingreso": "Nivel de ingreso"}


# ---------------------------------------------------------------------------
# Matrices de entrada (se comparten entre entrenamiento y predicción)
# ---------------------------------------------------------------------------
def matriz_nbo(feat_rows, productos, canales):
    """Variables del cliente + one-hot de segmento, evento de vida, producto y canal (HistGB acepta NaN)."""
    X = feat_rows[FEATS_NBO].astype(float).to_numpy()
    ev = feat_rows.evento_vida_reciente.fillna("").to_numpy()
    seg = np.stack([(feat_rows.segmento.to_numpy() == s) for s in SEGMENTOS], axis=1).astype(float)
    evm = np.stack([(ev == e) for e in EVENTOS_VIDA], axis=1).astype(float)
    prod = np.stack([(np.asarray(productos) == p) for p in PRODUCTOS], axis=1).astype(float)
    can = np.stack([(np.asarray(canales) == c) for c in CANALES_OFERTA], axis=1).astype(float)
    return np.hstack([X, seg, evm, prod, can])


def matriz_abandono(feat):
    df = feat.assign(log_ingreso=np.log(feat.ingreso_mensual_gtq))
    return df[FEATS_ABANDONO].astype(float).to_numpy()


def features_fraude(tx):
    """Variables por transacción de débito para el detector de anomalías."""
    d = tx[tx.tipo == "debito"].sort_values(["cliente_id", "ts"]).copy()
    d["n_txn_1h"] = d.groupby("cliente_id").rolling("60min", on="ts")["monto_gtq"].count().to_numpy()
    d["mediana_cliente"] = d.groupby("cliente_id").monto_gtq.transform("median")
    hora = d.ts.dt.hour
    X = pd.DataFrame({
        "log_monto": np.log1p(d.monto_gtq), "ratio_monto_mediana": np.log1p(d.monto_gtq / (d.mediana_cliente + 1)),
        "hora_sin": np.sin(2 * np.pi * hora / 24), "hora_cos": np.cos(2 * np.pi * hora / 24), "madrugada": (hora < 5).astype(int),
        "log_dist_km": np.log1p(d.dist_km_hogar), "internacional": (d.pais != "GT").astype(int),
        "dispositivo_nuevo": d.dispositivo_nuevo.astype(int), "n_txn_1h": d.n_txn_1h, "ecommerce": (d.canal == "ecommerce").astype(int)})
    return d, X


# ---------------------------------------------------------------------------
# Entrenamiento
# ---------------------------------------------------------------------------
def entrenar_nbo(feat, campanas):
    fx = feat.set_index("cliente_id")
    rows = fx.loc[campanas.cliente_id].reset_index()
    X = matriz_nbo(rows, campanas.producto.to_numpy(), campanas.canal.to_numpy())
    y = campanas.aceptada.to_numpy()
    Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=.25, random_state=SEED, stratify=y)
    m = HistGradientBoostingClassifier(max_iter=200, learning_rate=.05, max_leaf_nodes=15, min_samples_leaf=40,
                                       l2_regularization=1.0, random_state=SEED).fit(Xtr, ytr)
    p = m.predict_proba(Xte)[:, 1]
    umbral = .25
    cm = confusion_matrix(yte, p >= umbral).tolist()
    met = {"auc": round(float(roc_auc_score(yte, p)), 3), "precision": round(float(precision_score(yte, p >= umbral)), 3),
           "recall": round(float(recall_score(yte, p >= umbral)), 3), "umbral_clasificacion": umbral,
           "matriz_confusion": {"vn": cm[0][0], "fp": cm[0][1], "fn": cm[1][0], "vp": cm[1][1]},
           "n_entrenamiento": int(len(ytr)), "n_prueba": int(len(yte)), "tasa_aceptacion_historica": round(float(y.mean()), 3),
           "variables": FEATS_NBO + [f"segmento={s}" for s in SEGMENTOS] + [f"evento={e}" for e in EVENTOS_VIDA]
                        + [f"producto={p}" for p in PRODUCTOS] + [f"canal={c}" for c in CANALES_OFERTA]}
    return m, met


def entrenar_abandono(feat, etiquetas):
    X = matriz_abandono(feat)
    y = etiquetas.set_index("cliente_id").loc[feat.cliente_id].baja_60d.to_numpy().astype(int)
    sc = StandardScaler().fit(X)
    Xs = np.clip(sc.transform(X), -3, 3)
    Xtr, Xte, ytr, yte = train_test_split(Xs, y, test_size=.3, random_state=SEED, stratify=y)
    m = LogisticRegression(C=.3, max_iter=2000).fit(Xtr, ytr)
    p = m.predict_proba(Xte)[:, 1]
    met = {"auc": round(float(roc_auc_score(yte, p)), 3), "n_entrenamiento": int(len(ytr)), "n_prueba": int(len(yte)),
           "tasa_evento": round(float(y.mean()), 3), "coeficientes": dict(zip(FEATS_ABANDONO, m.coef_[0].round(3).tolist()))}
    return {"scaler": sc, "modelo": m}, met


def entrenar_fraude(tx, etiquetas):
    d, X = features_fraude(tx)
    m = IsolationForest(n_estimators=300, contamination=.012, random_state=SEED).fit(X)
    raw = -m.score_samples(X)
    score = (raw - raw.min()) / (raw.max() - raw.min())
    t_2fa, t_bloqueo = float(np.quantile(score, .988)), float(np.quantile(score, .997))
    real = d.txn_id.isin(set(etiquetas.txn_id)).to_numpy().astype(int)
    marcadas = score >= t_2fa
    met = {"auc": round(float(roc_auc_score(real, score)), 3), "recall": round(float(real[marcadas].sum() / max(real.sum(), 1)), 3),
           "precision_alertas": round(float(real[marcadas].mean()), 3), "tasa_alertas": round(float(marcadas.mean()), 4),
           "fraudes_confirmados": int(real.sum()), "umbral_2fa": round(t_2fa, 4), "umbral_bloqueo": round(t_bloqueo, 4),
           "n_transacciones": int(len(d))}
    return {"modelo": m, "min": float(raw.min()), "max": float(raw.max()), "umbral_2fa": t_2fa, "umbral_bloqueo": t_bloqueo}, met


def main():
    con = conectar()
    feat = pd.read_sql("SELECT * FROM gold_features_cliente", con)
    campanas = pd.read_sql("SELECT * FROM silver_campanas_historial", con)
    et_ab = pd.read_sql("SELECT * FROM silver_etiquetas_abandono", con)
    et_fr = pd.read_sql("SELECT * FROM silver_etiquetas_fraude", con)
    tx = pd.read_sql("SELECT * FROM silver_transacciones", con)
    tx["ts"] = pd.to_datetime(tx.ts)

    MODELOS.mkdir(parents=True, exist_ok=True)
    nbo, met_nbo = entrenar_nbo(feat, campanas)
    joblib.dump(nbo, MODELOS / "nbo.joblib")
    ab, met_ab = entrenar_abandono(feat, et_ab)
    joblib.dump(ab, MODELOS / "abandono.joblib")
    fr, met_fr = entrenar_fraude(tx, et_fr)
    joblib.dump(fr, MODELOS / "fraude.joblib")

    metricas = {
        "fecha_entrenamiento": datetime.now().isoformat(timespec="seconds"),
        "nota": "Métricas calculadas sobre datos sintéticos; no representan desempeño real.",
        "modelos": [
            {"id": "nbo", "nombre": "Siguiente mejor oferta", "algoritmo": "Gradient Boosting (HistGradientBoostingClassifier)",
             "version": VERSION_NBO, "rol": "Servicio principal del MVP", **met_nbo},
            {"id": "abandono", "nombre": "Riesgo de abandono (60 días)", "algoritmo": "Regresión logística", "version": VERSION_ABANDONO,
             "rol": "Insumo del motor de decisión (R8)", **met_ab},
            {"id": "fraude", "nombre": "Anomalías transaccionales", "algoritmo": "Isolation Forest", "version": VERSION_FRAUDE,
             "rol": "Insumo del motor de decisión (R7)", **met_fr},
        ]}
    (MODELOS / "metricas.json").write_text(json.dumps(metricas, ensure_ascii=False, indent=2), encoding="utf-8")
    filas = [{"modelo": m["id"], "nombre": m["nombre"], "algoritmo": m["algoritmo"], "version": m["version"], "metrica": k, "valor": float(v)}
             for m in metricas["modelos"] for k, v in m.items() if isinstance(v, (int, float)) and not isinstance(v, bool)]
    pd.DataFrame(filas).assign(fecha_entrenamiento=metricas["fecha_entrenamiento"]).to_sql("gold_metricas_modelos", con, if_exists="replace", index=False)
    con.close()
    print(f"NBO       AUC={met_nbo['auc']}  precisión={met_nbo['precision']}  recall={met_nbo['recall']}  (n_train={met_nbo['n_entrenamiento']})")
    print(f"Abandono  AUC={met_ab['auc']}  tasa_evento={met_ab['tasa_evento']}")
    print(f"Fraude    AUC={met_fr['auc']}  recall={met_fr['recall']}  precisión_alertas={met_fr['precision_alertas']}")
    print(f"Modelos guardados en {MODELOS}")


if __name__ == "__main__":
    main()

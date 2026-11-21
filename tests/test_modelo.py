# -*- coding: utf-8 -*-
"""Prueba unitaria 3: el modelo entrenado cumple una métrica mínima y las predicciones tienen la forma esperada."""
import json

import joblib
import numpy as np
import pandas as pd
import pytest

from config import MODELOS, PRODUCTOS
from modelos import FEATS_NBO, matriz_nbo
from reglas import PROB_MINIMA_OFERTA


@pytest.fixture(scope="module")
def metricas():
    ruta = MODELOS / "metricas.json"
    if not ruta.exists():
        pytest.skip("No hay modelos entrenados: ejecute python pipeline/modelos.py")
    return {m["id"]: m for m in json.loads(ruta.read_text(encoding="utf-8"))["modelos"]}


def test_auc_minimo_de_cada_modelo(metricas):
    assert metricas["nbo"]["auc"] >= 0.65
    assert metricas["abandono"]["auc"] >= 0.70
    assert metricas["fraude"]["auc"] >= 0.80


def test_matriz_confusion_consistente(metricas):
    cm = metricas["nbo"]["matriz_confusion"]
    assert cm["vn"] + cm["fp"] + cm["fn"] + cm["vp"] == metricas["nbo"]["n_prueba"]


def test_modelo_nbo_devuelve_probabilidades(con):
    m = joblib.load(MODELOS / "nbo.joblib")
    feat = pd.read_sql("SELECT * FROM gold_features_cliente LIMIT 5", con)
    X = matriz_nbo(feat, ["tarjeta_credito"] * 5, ["push_app"] * 5)
    assert X.shape == (5, len(FEATS_NBO) + 5 + 5 + len(PRODUCTOS) + 3)
    p = m.predict_proba(X)[:, 1]
    assert p.shape == (5,) and np.all((p >= 0) & (p <= 1))


def test_predicciones_en_gold(con):
    nbo = pd.read_sql("SELECT * FROM gold_pred_nbo", con)
    assert nbo.prob_aceptacion.dropna().between(0, 1).all()
    assert (nbo[nbo.ofrecer == 1].prob_aceptacion >= PROB_MINIMA_OFERTA).all()
    assert nbo.productos_bloqueados.fillna("").str.contains("R1").sum() > 0, "debe haber bloqueos por R1"
    nba = pd.read_sql("SELECT * FROM gold_siguiente_mejor_accion", con)
    assert len(nba) == con.execute("SELECT COUNT(*) FROM gold_features_cliente").fetchone()[0]
    assert set(nba.tipo_accion.unique()) <= {"Seguridad", "Retención", "Oferta", "Bienestar financiero", "Educación"}


def test_trazabilidad_registro(con):
    reg = pd.read_sql("SELECT * FROM gold_registro_predicciones", con)
    assert reg.prediccion_id.is_unique
    assert reg[["modelo", "version", "hash_features", "decision"]].notna().all().all()

# -*- coding: utf-8 -*-
"""Prueba unitaria 4: la API responde con la recomendación del cliente y maneja clientes inexistentes."""
import pytest
from fastapi.testclient import TestClient

from config import DB_PATH


@pytest.fixture(scope="module")
def cliente():
    if not DB_PATH.exists():
        pytest.skip("No existe data/novabank.db")
    from main import app
    return TestClient(app)


def test_salud(cliente):
    r = cliente.get("/salud")
    assert r.status_code == 200 and r.json()["estado"] == "ok" and r.json()["clientes"] >= 1000


def test_recomendacion_cliente_existente(cliente):
    r = cliente.get("/recomendacion/NB-000001")
    assert r.status_code == 200
    cuerpo = r.json()
    for campo in ["cliente_id", "tipo_accion", "siguiente_mejor_accion", "regla_decision", "prob_abandono_60d", "modelo_version"]:
        assert campo in cuerpo, campo
    assert cuerpo["cliente_id"] == "NB-000001"


def test_recomendacion_cliente_inexistente(cliente):
    r = cliente.get("/recomendacion/NB-999999")
    assert r.status_code == 404


def test_lista_recomendaciones_filtra_por_tipo(cliente):
    r = cliente.get("/recomendaciones", params={"tipo": "Oferta", "limite": 5})
    assert r.status_code == 200
    datos = r.json()["recomendaciones"]
    assert 0 < len(datos) <= 5 and all(d["tipo_accion"] == "Oferta" for d in datos)


def test_registrar_accion(cliente):
    r = cliente.post("/acciones", json={"cliente_id": "NB-000001", "tipo_accion": "Prueba", "descripcion": "registro de prueba",
                                       "canal": "test", "disparador": "pytest"})
    assert r.status_code == 201 and r.json()["accion_id"].startswith("ACC-")
    # limpieza: la prueba no debe dejar rastro en la zona Gold
    import sqlite3
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM gold_acciones_enviadas WHERE disparador = 'pytest'")
    con.execute("UPDATE gold_siguiente_mejor_accion SET estado = 'pendiente' WHERE cliente_id = 'NB-000001'")
    con.commit()
    con.close()


def test_tablero_en_vivo(cliente):
    """El tablero se sirve desde la API y sus datos salen de la zona Gold en el momento."""
    r = cliente.get("/tablero")
    assert r.status_code == 200 and "NovaBank" in r.text
    d = cliente.get("/tablero/datos").json()
    assert len(d["acciones"]) == cliente.get("/salud").json()["clientes"]
    assert {"Oferta", "Educación"} <= {a["tipo_accion"] for a in d["acciones"]}
    assert d["kpi_diarios"] and d["metricas"] and d["firma"] == cliente.get("/tablero/estado").json()["firma"]

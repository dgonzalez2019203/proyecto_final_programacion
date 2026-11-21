# -*- coding: utf-8 -*-
"""Prueba unitaria 1: el generador produce datos dentro de rangos y distribuciones realistas."""
import pytest

from config import BRONZE
from validar_datos import validar


@pytest.fixture(scope="module")
def resumen():
    if not BRONZE.exists():
        pytest.skip("No existe data/bronze: ejecute primero python pipeline/generar_datos.py")
    return validar()


def test_todas_las_verificaciones_pasan(resumen):
    fallas = [r for r in resumen["verificaciones"] if not r["ok"]]
    assert not fallas, fallas


def test_distribucion_ingreso_es_asimetrica(resumen):
    p = resumen["percentiles_ingreso_gtq"]
    assert p[0.5] < (p[0.05] + p[0.95]) / 2, "la mediana debe quedar por debajo del promedio de extremos (sesgo a la derecha)"


def test_segmentos_balanceados(resumen):
    assert all(.05 <= v <= .40 for v in resumen["segmentos"].values())

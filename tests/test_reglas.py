# -*- coding: utf-8 -*-
"""Prueba unitaria 2: motor de decisión (reglas R1 a R8) con casos de frontera. No necesita base de datos."""
from reglas import elegir_oferta, evaluar_producto, siguiente_mejor_accion

CLIENTE_OK = {"segmento": "Profesional", "edad": 35, "sin_historial_buro": False, "score_buro": 700, "max_dias_mora_12m": 0, "consentimiento_marketing": True}


def test_r1_sin_buro_bloquea_credito_pero_no_ahorro():
    c = {**CLIENTE_OK, "sin_historial_buro": True, "score_buro": None}
    assert evaluar_producto("tarjeta_credito", c).startswith("R1")
    assert evaluar_producto("cuenta_ahorro_meta", c) is None


def test_r1_score_y_mora():
    assert evaluar_producto("prestamo_personal", {**CLIENTE_OK, "score_buro": 579}).startswith("R1")
    assert evaluar_producto("prestamo_personal", {**CLIENTE_OK, "score_buro": 580}) is None
    assert evaluar_producto("prestamo_personal", {**CLIENTE_OK, "max_dias_mora_12m": 30}) == "R1 mora reciente"


def test_r2_pyme_solo_emprendedor():
    assert evaluar_producto("credito_pyme", CLIENTE_OK) == "R2 solo segmento Emprendedor"
    assert evaluar_producto("credito_pyme", {**CLIENTE_OK, "segmento": "Emprendedor"}) is None


def test_r3_edad_seguro():
    assert evaluar_producto("seguro_vida", {**CLIENTE_OK, "edad": 66}) == "R3 edad máxima"
    assert evaluar_producto("seguro_vida", {**CLIENTE_OK, "edad": 65}) is None


def test_elegir_oferta_salta_bloqueados_y_toma_el_siguiente():
    candidatos = [{"producto": "tarjeta_credito", "canal": "push_app", "prob": .5}, {"producto": "seguro_vida", "canal": "email", "prob": .3}]
    of = elegir_oferta(candidatos, {**CLIENTE_OK, "score_buro": 500})
    assert of["producto"] == "seguro_vida" and of["ofrecer"] is True
    assert "tarjeta_credito: R1" in of["productos_bloqueados"]


def test_r4_sin_consentimiento_usa_banner():
    of = elegir_oferta([{"producto": "plazo_fijo", "canal": "email", "prob": .4}], {**CLIENTE_OK, "consentimiento_marketing": False})
    assert of["canal"] == "banner_app" and of["regla_aplicada"].startswith("R4")


def test_r5_fatiga_pospone():
    of = elegir_oferta([{"producto": "plazo_fijo", "canal": "email", "prob": .4}], CLIENTE_OK, oferta_reciente=True)
    assert of["ofrecer"] is False and of["regla_aplicada"].startswith("R5")


def test_r6_probabilidad_minima():
    assert elegir_oferta([{"producto": "plazo_fijo", "canal": "email", "prob": .09}], CLIENTE_OK)["ofrecer"] is False
    assert elegir_oferta([{"producto": "plazo_fijo", "canal": "email", "prob": .10}], CLIENTE_OK)["ofrecer"] is True


def test_sin_producto_elegible():
    of = elegir_oferta([{"producto": "credito_pyme", "canal": "asesor", "prob": .9}], CLIENTE_OK)
    assert of["producto"] is None and of["regla_aplicada"] == "Sin producto elegible"


def test_arbitraje_r7_seguridad_gana_a_todo():
    oferta = {"producto": "plazo_fijo", "canal": "email", "prob": .9, "regla_aplicada": "R1–R4 superadas", "ofrecer": True}
    tipo, _, regla = siguiente_mejor_accion(oferta, prob_abandono=.9, accion_retencion="Llamada", fraude_bloqueado=True)
    assert tipo == "Seguridad" and regla.startswith("R7")


def test_arbitraje_r8_retencion_gana_a_oferta():
    oferta = {"producto": "plazo_fijo", "canal": "email", "prob": .9, "regla_aplicada": "R1–R4 superadas", "ofrecer": True}
    tipo, desc, regla = siguiente_mejor_accion(oferta, prob_abandono=.6, accion_retencion="Llamada de asesor", fraude_bloqueado=False)
    assert tipo == "Retención" and desc == "Llamada de asesor" and regla.startswith("R8")


def test_arbitraje_oferta_y_educacion():
    oferta = {"producto": "plazo_fijo", "canal": "push_app", "prob": .4, "regla_aplicada": "R1–R4 superadas", "ofrecer": True}
    tipo, desc, _ = siguiente_mejor_accion(oferta, .1, "x", False, hora_optima=21)
    assert tipo == "Oferta" and desc == "Ofrecer Depósito a plazo fijo por push en la app a las 21:00"
    assert siguiente_mejor_accion(None, .1, "x", False)[0] == "Educación"

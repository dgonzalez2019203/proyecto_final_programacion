# -*- coding: utf-8 -*-
"""Motor de decisión de NovaBank: reglas de negocio R1 a R8 (sistema experto del MP2).

Las reglas son funciones puras sobre un diccionario con los datos del cliente, para que
puedan probarse una por una con pytest sin base de datos ni modelos.

Elegibilidad de la oferta (R1 a R5) y umbral (R6):
  R1  Productos de crédito solo con historial de buró, score >= 580 y sin mora >= 30 días.
  R2  Crédito PYME solo para el segmento Emprendedor.
  R3  Seguro de vida solo hasta los 65 años.
  R4  Sin consentimiento de marketing solo se muestra un banner dentro de la app.
  R5  Fatiga de contacto: si recibió una oferta en los últimos 14 días, se pospone.
  R6  Solo se ofrece si la probabilidad de aceptación es al menos 10 %.
Arbitraje de la siguiente mejor acción (R7 y R8):
  R7  Seguridad primero: con una alerta de fraude bloqueada en los últimos 7 días no hay ofertas.
  R8  Retención antes que venta: con riesgo de abandono >= 60 % la acción es de retención.
"""
from config import PRODUCTOS, PRODUCTOS_CREDITO

SCORE_MINIMO = 580
MORA_MAXIMA = 30
EDAD_MAX_SEGURO = 65
DIAS_FATIGA = 14
PROB_MINIMA_OFERTA = 0.10
UMBRAL_RETENCION = 0.60
DIAS_BLOQUEO_FRAUDE = 7
NOMBRE_CANAL = {"push_app": "push en la app", "email": "correo", "asesor": "asesor", "banner_app": "banner en la app"}


def evaluar_producto(producto, cliente):
    """Devuelve None si el producto es elegible, o el texto de la regla que lo bloquea.

    `cliente` es un dict con: segmento, edad, sin_historial_buro, score_buro, max_dias_mora_12m.
    """
    if producto in PRODUCTOS_CREDITO:
        if cliente.get("sin_historial_buro"):
            return "R1 sin historial de buró"
        if (cliente.get("score_buro") or 0) < SCORE_MINIMO:
            return f"R1 score de buró menor a {SCORE_MINIMO}"
        if (cliente.get("max_dias_mora_12m") or 0) >= MORA_MAXIMA:
            return "R1 mora reciente"
    if producto == "credito_pyme" and cliente.get("segmento") != "Emprendedor":
        return "R2 solo segmento Emprendedor"
    if producto == "seguro_vida" and cliente.get("edad", 0) > EDAD_MAX_SEGURO:
        return "R3 edad máxima"
    return None


def elegir_oferta(candidatos, cliente, oferta_reciente=False):
    """Recorre los candidatos (lista de dicts producto/canal/prob ordenada por prob desc) y aplica R1 a R6.

    Devuelve un dict con producto, canal, prob, regla_aplicada y productos_bloqueados, o None
    si no hay producto elegible.
    """
    bloqueos, elegido = [], None
    for cand in candidatos:
        motivo = evaluar_producto(cand["producto"], cliente)
        if motivo:
            bloqueos.append(f"{cand['producto']}: {motivo}")
            continue
        elegido = dict(cand)
        break
    if elegido is None:
        return {"producto": None, "canal": None, "prob": None, "regla_aplicada": "Sin producto elegible",
                "productos_bloqueados": "; ".join(bloqueos), "ofrecer": False}
    regla = "R1–R4 superadas"
    if not cliente.get("consentimiento_marketing", True):
        elegido["canal"], regla = "banner_app", "R4 sin consentimiento de marketing: solo banner en la app"
    if oferta_reciente:
        regla = f"R5 fatiga de contacto: oferta pospuesta {DIAS_FATIGA} días"
    if elegido["prob"] < PROB_MINIMA_OFERTA:
        regla = f"R6 probabilidad menor al {PROB_MINIMA_OFERTA:.0%}: sin oferta activa"
    elegido.update({"regla_aplicada": regla, "productos_bloqueados": "; ".join(bloqueos),
                    "ofrecer": not oferta_reciente and elegido["prob"] >= PROB_MINIMA_OFERTA})
    return elegido


def siguiente_mejor_accion(oferta, prob_abandono, accion_retencion, fraude_bloqueado, hora_optima=20):
    """Arbitraje R7 > R8 > oferta > educación. Devuelve (tipo_accion, descripcion, regla)."""
    if fraude_bloqueado:
        return ("Seguridad", "Contactar al cliente por alerta de fraude (sin ofertas comerciales)", "R7 seguridad primero")
    if prob_abandono >= UMBRAL_RETENCION:
        return ("Retención", accion_retencion, "R8 retención antes que venta")
    if oferta and oferta.get("ofrecer"):
        canal = NOMBRE_CANAL.get(oferta["canal"], oferta["canal"])
        nombre = PRODUCTOS[oferta["producto"]][1]
        return ("Oferta", f"Ofrecer {nombre} por {canal} a las {int(hora_optima):02d}:00", oferta["regla_aplicada"])
    return ("Educación", "Contenido de educación financiera en la app", oferta["regla_aplicada"] if oferta else "Sin producto elegible")

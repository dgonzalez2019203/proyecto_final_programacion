# -*- coding: utf-8 -*-
"""Flujo de automatización de NovaBank: disparador → obtener predicciones → regla de decisión → acción.

En el diseño del MP2 este flujo lo dispara Cloud Scheduler y lo ejecuta Cloud Run. Aquí el
disparador es APScheduler (reloj local) y las acciones son:
  - Oferta por correo   -> se genera el correo HTML en automatizacion/salida/correos/ (y se envía por SMTP si hay credenciales)
  - Oferta por push/banner -> se registra como notificación programada
  - Retención           -> se agrega a la lista de llamadas del asesor (salida/lista_retencion.csv)
  - Seguridad           -> se registra para el equipo de fraude
Cada acción se registra en la tabla gold_acciones_enviadas a través de la API (POST /acciones).

Ejecución:
  python automatizacion/flujo_local.py --ahora            # corre una vez y termina (para la demo)
  python automatizacion/flujo_local.py --ahora --tipo Oferta --max 20   # solo ofertas: genera correos y push
  python automatizacion/flujo_local.py --programar 07:00  # queda corriendo y dispara todos los días a esa hora
Variables opcionales para envío real: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_DESTINO_PRUEBA
"""
import argparse
import csv
import os
import smtplib
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import httpx

sys.stdout.reconfigure(encoding="utf-8")
BASE = Path(__file__).resolve().parent
SALIDA = BASE / "salida"
API = os.environ.get("NOVABANK_API", "http://127.0.0.1:8000")
PLANTILLA = (BASE / "correo_oferta.html").read_text(encoding="utf-8")
MAX_POR_CORRIDA = int(os.environ.get("NOVABANK_MAX_ACCIONES", "50"))


def obtener_predicciones(tipo=None, maximo=MAX_POR_CORRIDA):
    params = {"estado": "pendiente", "limite": maximo}
    if tipo:
        params["tipo"] = tipo
    r = httpx.get(f"{API}/recomendaciones", params=params, timeout=30)
    r.raise_for_status()
    return r.json()["recomendaciones"]


def regla_de_decision(rec):
    """Convierte la recomendación en una acción concreta: (tipo, canal, descripcion)."""
    if rec["tipo_accion"] == "Seguridad":
        return "Seguridad", "equipo_fraude", "Caso abierto para el equipo de fraude; sin ofertas comerciales"
    if rec["tipo_accion"] == "Retención":
        return "Retención", "asesor", rec["siguiente_mejor_accion"]
    if rec["tipo_accion"] == "Oferta":
        canal = rec["canal"] or "push_app"
        return "Oferta", canal, rec["siguiente_mejor_accion"]
    return None  # Educación: contenido en la app, no genera acción saliente


def ejecutar_accion(rec, tipo, canal, descripcion, ahora):
    (SALIDA / "correos").mkdir(parents=True, exist_ok=True)
    if tipo == "Oferta" and canal == "email":
        html = PLANTILLA.format(cliente_id=rec["cliente_id"], producto=rec["producto"], prob=f"{rec['prob_aceptacion'] * 100:.0f}", fecha=ahora[:10])
        ruta = SALIDA / "correos" / f"{rec['cliente_id']}_{rec['producto'].replace(' ', '_')}.html"
        ruta.write_text(html, encoding="utf-8")
        detalle = f"correo generado: {ruta.name}"
        if os.environ.get("SMTP_HOST"):
            detalle += " | " + enviar_smtp(f"NovaBank: {rec['producto']} para ti", html)
        return detalle
    if tipo == "Oferta":
        return f"notificación {canal} programada: {descripcion}"
    if tipo == "Retención":
        with open(SALIDA / "lista_retencion.csv", "a", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow([ahora, rec["cliente_id"], f"{rec['prob_abandono_60d']:.2f}", descripcion])
        return "agregado a lista_retencion.csv"
    return "caso registrado para el equipo de fraude"


def enviar_smtp(asunto, html):
    msg = MIMEMultipart("alternative")
    msg["Subject"], msg["From"], msg["To"] = asunto, os.environ["SMTP_USER"], os.environ.get("SMTP_DESTINO_PRUEBA", os.environ["SMTP_USER"])
    msg.attach(MIMEText(html, "html", "utf-8"))
    with smtplib.SMTP(os.environ["SMTP_HOST"], int(os.environ.get("SMTP_PORT", "587"))) as s:
        s.starttls()
        s.login(os.environ["SMTP_USER"], os.environ["SMTP_PASS"])
        s.send_message(msg)
    return f"enviado por SMTP a {msg['To']}"


def registrar(rec, tipo, canal, descripcion, detalle, disparador):
    r = httpx.post(f"{API}/acciones", json={"cliente_id": rec["cliente_id"], "tipo_accion": tipo, "descripcion": descripcion,
                                           "canal": canal, "disparador": disparador, "detalle": detalle}, timeout=30)
    r.raise_for_status()
    return r.json()["accion_id"]


def correr_flujo(disparador="manual", tipo=None, maximo=MAX_POR_CORRIDA):
    ahora = datetime.now().isoformat(timespec="seconds")
    print(f"[{ahora}] Disparador: {disparador}. Obteniendo predicciones de {API} …")
    recs = obtener_predicciones(tipo, maximo)
    resumen = {}
    SALIDA.mkdir(parents=True, exist_ok=True)
    with open(SALIDA / "log_ejecuciones.csv", "a", newline="", encoding="utf-8") as log:
        w = csv.writer(log)
        for rec in recs:
            decision = regla_de_decision(rec)
            if decision is None:
                continue
            tipo, canal, descripcion = decision
            detalle = ejecutar_accion(rec, tipo, canal, descripcion, ahora)
            accion_id = registrar(rec, tipo, canal, descripcion, detalle, disparador)
            w.writerow([ahora, disparador, accion_id, rec["cliente_id"], tipo, canal, detalle])
            resumen[tipo] = resumen.get(tipo, 0) + 1
            print(f"  {accion_id} {rec['cliente_id']:<10} {tipo:<10} {canal:<14} {detalle}")
    print(f"Listo: {sum(resumen.values())} acciones ejecutadas {resumen}. Registro en gold_acciones_enviadas y {SALIDA / 'log_ejecuciones.csv'}")
    return resumen


def main():
    ap = argparse.ArgumentParser(description="Flujo de automatización NovaBank")
    ap.add_argument("--ahora", action="store_true", help="ejecuta el flujo una vez")
    ap.add_argument("--programar", metavar="HH:MM", help="deja el disparador corriendo todos los días a esa hora")
    ap.add_argument("--cada", type=int, metavar="MIN", help="dispara cada N minutos (útil para la demo)")
    ap.add_argument("--tipo", choices=["Seguridad", "Retención", "Oferta"], help="procesa solo ese tipo de acción")
    ap.add_argument("--max", type=int, default=MAX_POR_CORRIDA, help=f"acciones por corrida (por defecto {MAX_POR_CORRIDA})")
    args = ap.parse_args()
    if args.ahora or not (args.programar or args.cada):
        return correr_flujo("manual", args.tipo, args.max)
    from apscheduler.schedulers.blocking import BlockingScheduler
    sched = BlockingScheduler()
    if args.programar:
        h, m = args.programar.split(":")
        sched.add_job(lambda: correr_flujo(f"cron diario {args.programar}", args.tipo, args.max), "cron", hour=int(h), minute=int(m))
    if args.cada:
        sched.add_job(lambda: correr_flujo(f"intervalo {args.cada} min", args.tipo, args.max), "interval", minutes=args.cada)
    print(f"Disparador activo ({'diario ' + args.programar if args.programar else ''}{' cada ' + str(args.cada) + ' min' if args.cada else ''}). Ctrl+C para salir.")
    sched.start()


if __name__ == "__main__":
    main()

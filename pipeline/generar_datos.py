# -*- coding: utf-8 -*-
"""Paso 1 del pipeline: genera las fuentes SINTÉTICAS de NovaBank y las deja en la zona Bronze.

Simula ocho fuentes de un banco digital guatemalteco (core bancario, switch de tarjetas, app
móvil, buró de crédito, CRM de campañas y call center) para N_CLIENTES clientes y DIAS días.
Todo se genera con una semilla fija, así que el resultado es reproducible.

Las etiquetas que los modelos deben aprender (quién acepta una oferta, quién abandona, qué
compras fueron fraude) se construyen con reglas más ruido y se guardan como fuentes del CRM y
del switch (ofertas históricas, bajas observadas y contracargos confirmados), igual que en un
banco real donde esos resultados ya ocurrieron.

Ejecución:  python pipeline/generar_datos.py
Salida:     data/bronze/<fuente>/fecha_ingesta=AAAA-MM-DD/<archivo>
"""
import json
import random
import shutil
import unicodedata
from datetime import datetime, time, timedelta

import numpy as np
import pandas as pd

from config import (BRONZE, DIAS, FECHA_FIN, FECHAS, FUENTES_BRONZE, N_CLIENTES, PART_INGESTA,
                    PRODUCTOS, SEED, CANALES_OFERTA)

rng = np.random.default_rng(SEED)
random.seed(SEED)

# ---------------------------------------------------------------------------
# Catálogos de referencia (contexto guatemalteco)
# ---------------------------------------------------------------------------
DEPTOS = [  # nombre, lat, lon, peso, remesas_alta, factor_ingreso, urbano
    ("Guatemala", 14.6349, -90.5069, .42, False, 1.15, True),
    ("Quetzaltenango", 14.8347, -91.5180, .08, True, 1.00, True),
    ("Escuintla", 14.3050, -90.7850, .06, False, .90, False),
    ("Sacatepéquez", 14.5586, -90.7295, .05, False, 1.05, True),
    ("Alta Verapaz", 15.4700, -90.3700, .05, True, .80, False),
    ("Huehuetenango", 15.3197, -91.4709, .05, True, .80, False),
    ("San Marcos", 14.9639, -91.7944, .04, True, .80, False),
    ("Chimaltenango", 14.6611, -90.8194, .04, False, .88, False),
    ("Petén", 16.9258, -89.8933, .04, False, .85, False),
    ("Izabal", 15.7270, -88.5944, .04, False, .88, False),
    ("Suchitepéquez", 14.5344, -91.5033, .04, False, .85, False),
    ("Retalhuleu", 14.5372, -91.6778, .03, False, .85, False),
    ("Zacapa", 14.9722, -89.5306, .03, False, .88, False),
    ("Jutiapa", 14.2917, -89.8958, .03, True, .82, False),
]
PAISES = {"US": (25.76, -80.19), "MX": (19.43, -99.13), "SV": (13.69, -89.22), "ES": (40.42, -3.70)}
SEG_PARAMS = {  # segmento: (edad_min, edad_max, ingreso_mediana_gtq, peso, sesiones_app_dia)
    "Joven digital": (18, 27, 4200, .25, 1.1), "Profesional": (26, 45, 11000, .28, .8),
    "Emprendedor": (25, 55, 9000, .15, .7), "Familiar": (30, 55, 7500, .22, .55), "Senior": (56, 72, 6500, .10, .3),
}
CATEGORIAS = {  # nombre: (monto_mediana_gtq, sigma, canal)
    "supermercado": (320, .6, "POS"), "restaurantes": (145, .55, "POS"), "combustible": (250, .4, "POS"),
    "transporte_apps": (48, .5, "ecommerce"), "telefonia_recargas": (75, .4, "app"), "farmacia": (160, .6, "POS"),
    "entretenimiento_streaming": (95, .5, "ecommerce"), "viajes": (1800, .7, "ecommerce"), "educacion": (950, .5, "app"),
    "ecommerce": (380, .8, "ecommerce"), "servicios_basicos": (420, .4, "app"), "ropa_calzado": (450, .6, "POS"),
    "retiro_cajero": (500, .5, "ATM"), "transferencia_enviada": (800, .8, "app"),
}
CAT_NOMBRES = list(CATEGORIAS)
BASE_PREF = {"supermercado": 10, "restaurantes": 7, "combustible": 5, "transporte_apps": 4, "telefonia_recargas": 4,
             "farmacia": 3, "entretenimiento_streaming": 3, "viajes": .6, "educacion": 1, "ecommerce": 4,
             "servicios_basicos": .6, "ropa_calzado": 2, "retiro_cajero": 4, "transferencia_enviada": 3}
AJUSTE_SEG = {
    "Joven digital": {"transporte_apps": 2.2, "entretenimiento_streaming": 2, "ecommerce": 1.8, "restaurantes": 1.3,
                      "telefonia_recargas": 1.6, "combustible": .5, "educacion": 1.5},
    "Profesional": {"restaurantes": 1.5, "viajes": 2.5, "ecommerce": 1.5, "combustible": 1.3},
    "Emprendedor": {"transferencia_enviada": 2.5, "combustible": 1.5, "servicios_basicos": 1.4, "viajes": 1.3},
    "Familiar": {"supermercado": 1.5, "farmacia": 1.5, "educacion": 2.5, "servicios_basicos": 1.5, "restaurantes": .8},
    "Senior": {"farmacia": 2.5, "supermercado": 1.3, "retiro_cajero": 1.8, "ecommerce": .4, "transporte_apps": .4,
               "entretenimiento_streaming": .5},
}
COMERCIOS = {
    "supermercado": ["Súper La Canasta", "Mercadito Central", "Hiper Ahorro GT"], "restaurantes": ["Pollo Dorado", "Comedor Doña Tere", "Café Volcán"],
    "combustible": ["Gasolinera El Trébol", "Servicentro Ruta 9"], "transporte_apps": ["RutaYa", "MoviGT"],
    "telefonia_recargas": ["RecargaTel", "Conecta Móvil"], "farmacia": ["Farmacia San Rafael", "FarmaSalud"],
    "entretenimiento_streaming": ["StreamPlus", "CineMax Online"], "viajes": ["VuelaCentro", "Hotel Lago Azul"],
    "educacion": ["Colegio Nuevo Horizonte", "Academia Code GT"], "ecommerce": ["MercaNet", "TiendaYa"],
    "servicios_basicos": ["Energía Nacional", "Agua Metropolitana", "Cable e Internet GT"], "ropa_calzado": ["Moda Quetzal", "Calzado Real"],
    "retiro_cajero": ["Cajero NovaBank", "Red Cajeros Unidos"], "transferencia_enviada": ["Transferencia ACH", "Transferencia interna"],
}
HORAS_P = np.array([.5, .3, .2, .15, .15, .3, 1, 2, 3, 4, 4.5, 5, 6.5, 6, 4.5, 4, 4.2, 5, 6, 6.2, 5.5, 4, 2.5, 1.2])
HORAS_P = HORAS_P / HORAS_P.sum()
NOMBRES_F = ["María", "Ana", "Carmen", "Rosa", "Sofía", "Andrea", "Lucía", "Gabriela", "Valeria", "Mónica", "Paola", "Ingrid", "Karla", "Fernanda"]
NOMBRES_M = ["José", "Juan", "Luis", "Carlos", "Jorge", "Diego", "Mario", "Pedro", "Fernando", "Miguel", "Kevin", "Edgar", "Byron", "Hugo"]
APELLIDOS = ["López", "García", "Pérez", "Hernández", "González", "Rodríguez", "Morales", "Martínez", "Ramírez", "Castillo",
             "Cruz", "Mejía", "Juárez", "Reyes", "Orellana", "Barrios", "Chávez", "Ajú", "Tzul", "Xicará", "Coyoy", "Pop", "Caal"]
DISPOSITIVOS = ["Samsung Galaxy A15", "Xiaomi Redmi Note 13", "iPhone 13", "iPhone 15", "Motorola Moto G54", "Honor X7b"]
EVENTOS_VIDA = ["nuevo_empleo", "graduacion", "mudanza", "boda", "nacimiento"]
PLANTILLAS = {  # intención: (sentimiento base, textos)
    "consulta_saldo": (.3, ["¿Me confirman si ya se acreditó mi pago de planilla?", "Quisiera saber el saldo disponible de mi tarjeta."]),
    "solicitud_producto": (.5, ["Me interesa una tarjeta de crédito para viajes, ¿qué requisitos piden?", "Quisiera información sobre préstamos personales."]),
    "reclamo_cobro": (-.7, ["Me cobraron dos veces la membresía y nadie me resuelve.", "Aparece un cargo que no reconozco, necesito que lo reviertan."]),
    "problema_app": (-.5, ["La app se cierra cada vez que intento transferir.", "No me llega el código de verificación."]),
    "cancelar_cuenta": (-.8, ["Quiero cancelar mi cuenta, en otro banco me ofrecen mejores condiciones.", "Estoy pensando trasladar mi planilla a otro banco."]),
    "felicitacion": (.8, ["Excelente la atención del asesor.", "Me gustó mucho la nueva función de metas de ahorro."]),
}
INTENCIONES = list(PLANTILLAS)
P_INT_ABANDONO = [.15, .05, .30, .25, .20, .05]
P_INT_NORMAL = [.38, .22, .12, .12, .02, .14]


def sigmoid(x):
    return 1.0 / (1.0 + np.exp(-x))


def zscore(s):
    s = np.asarray(s, dtype=float)
    return (s - s.mean()) / (s.std() + 1e-9)


def sin_acentos(texto):
    return "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")


def ruta_bronze(fuente):
    carpeta, archivo = FUENTES_BRONZE[fuente]
    ruta = BRONZE / carpeta / PART_INGESTA / archivo
    ruta.parent.mkdir(parents=True, exist_ok=True)
    return ruta


# ---------------------------------------------------------------------------
# 1. Clientes (core bancario) con verdad oculta
# ---------------------------------------------------------------------------
def generar_clientes():
    n = N_CLIENTES
    segs = list(SEG_PARAMS)
    seg = rng.choice(segs, size=n, p=[SEG_PARAMS[s][3] for s in segs])
    dep = rng.choice(len(DEPTOS), size=n, p=[d[3] for d in DEPTOS])
    edad = np.array([rng.integers(SEG_PARAMS[s][0], SEG_PARAMS[s][1] + 1) for s in seg])
    ingreso = np.round(np.array([SEG_PARAMS[s][2] for s in seg]) * np.array([DEPTOS[k][5] for k in dep]) * rng.lognormal(0, .42, n), -1)
    genero = rng.choice(["F", "M"], size=n, p=[.49, .51])
    antig = rng.integers(1, 97, n)
    recibe_remesas = rng.random(n) < np.where([DEPTOS[k][4] for k in dep], .35, .15)
    formal = np.isin(seg, ["Profesional", "Familiar", "Senior"]) | ((seg == "Joven digital") & (rng.random(n) < .6))
    disciplina = rng.normal(0, 1, n)
    urbano = np.array([DEPTOS[k][6] for k in dep])
    sin_buro = rng.random(n) < (.08 + .22 * (seg == "Emprendedor") + .18 * (seg == "Joven digital") + .08 * (~urbano))

    tiene_ahorro = rng.random(n) < .55
    tiene_tc = (rng.random(n) < sigmoid((ingreso - 7000) / 2500) * .75) & ~sin_buro
    tiene_prestamo = (rng.random(n) < .18) & ~sin_buro
    tiene_seguro = rng.random(n) < np.where(seg == "Familiar", .25, .10)
    tiene_plazo = rng.random(n) < np.select([seg == "Senior", seg == "Profesional"], [.30, .15], .05)
    tiene_pyme = (seg == "Emprendedor") & (rng.random(n) < .30) & ~sin_buro

    score = np.clip(rng.normal(650 + .004 * (ingreso - 7000) + .8 * (edad - 35) + 28 * disciplina, 60), 300, 850).round()
    mora = np.where(rng.random(n) < sigmoid(-(score - 560) / 40), rng.choice([15, 30, 60, 90], size=n, p=[.4, .3, .2, .1]), 0)
    deuda = np.round(ingreso * 12 * np.clip(rng.lognormal(-1.3, .8, n), 0, 3) * (.2 + .35 * tiene_tc + 1.0 * tiene_prestamo + .6 * tiene_pyme), -1)
    consultas = rng.poisson(1.0 + 1.5 * (score < 600))

    # verdad oculta: quién abandona en los próximos 60 días
    n_prod = 2 + tiene_ahorro + tiene_tc + tiene_prestamo + tiene_seguro + tiene_plazo + tiene_pyme
    r = -2.3 + .8 * (seg == "Joven digital") - .35 * (n_prod - 3) - .35 * (antig > 36) + .5 * (ingreso < 4000) + rng.normal(0, .8, n)
    churn = rng.random(n) < sigmoid(r)

    filas = []
    for i in range(n):
        nombre = random.choice(NOMBRES_F if genero[i] == "F" else NOMBRES_M)
        ap1, ap2 = random.choice(APELLIDOS), random.choice(APELLIDOS)
        d = DEPTOS[dep[i]]
        modelo = random.choice(DISPOSITIVOS)
        filas.append({
            "cliente_id": f"NB-{i + 1:06d}", "nombre_completo": f"{nombre} {ap1} {ap2}",
            "dpi": f"{rng.integers(1000, 9999)} {rng.integers(10000, 99999)} {rng.integers(101, 2220):04d}",
            "email": f"{sin_acentos(nombre).lower()}.{sin_acentos(ap1).lower()}{i + 1}@correo-ejemplo.com",
            "telefono": f"+502 {rng.choice([3, 4, 5])}{rng.integers(100, 999)}-{rng.integers(1000, 9999)}",
            "fecha_nacimiento": (FECHA_FIN - timedelta(days=int(edad[i] * 365.25 + rng.integers(0, 364)))).isoformat(),
            "genero": genero[i], "departamento": d[0], "urbano": bool(d[6]),
            "lat_hogar": round(d[1] + rng.normal(0, .04), 5), "lon_hogar": round(d[2] + rng.normal(0, .04), 5),
            "segmento": seg[i], "edad": int(edad[i]), "ingreso_mensual_gtq": float(ingreso[i]),
            "empleo_formal": bool(formal[i]), "antiguedad_meses": int(antig[i]),
            "fecha_alta": (FECHA_FIN - timedelta(days=int(antig[i] * 30.4))).isoformat(),
            "recibe_remesas": bool(recibe_remesas[i]), "consentimiento_marketing": bool(rng.random() < .92),
            "tiene_ahorro": bool(tiene_ahorro[i]), "tiene_tarjeta_credito": bool(tiene_tc[i]), "tiene_prestamo": bool(tiene_prestamo[i]),
            "tiene_seguro": bool(tiene_seguro[i]), "tiene_plazo_fijo": bool(tiene_plazo[i]), "tiene_credito_pyme": bool(tiene_pyme[i]),
            "dispositivo_principal": f"dev_{rng.integers(10 ** 7, 10 ** 8)}",
            "dispositivo_secundario": f"dev_{rng.integers(10 ** 7, 10 ** 8)}" if rng.random() < .25 else "",
            "modelo_dispositivo": modelo, "so": "iOS" if modelo.startswith("iPhone") else "Android",
            "evento_vida_reciente": str(rng.choice(EVENTOS_VIDA)) if rng.random() < .2 else "",
        })
    cli = pd.DataFrame(filas)
    buro = pd.DataFrame({"cliente_id": cli.cliente_id, "sin_historial": sin_buro, "score": np.where(sin_buro, np.nan, score),
                         "max_dias_mora_12m": np.where(sin_buro, np.nan, mora), "deuda_total_gtq": np.where(sin_buro, np.nan, deuda),
                         "consultas_6m": np.where(sin_buro, np.nan, consultas)})
    caida = np.where(churn, np.where(rng.random(n) < .4, rng.uniform(0, .25, n), rng.uniform(.5, .9, n)), 0.0)
    pausa = np.where(~churn & (rng.random(n) < .15), rng.integers(DIAS - 45, DIAS - 18, n), -1)
    viaje = np.where(rng.random(n) < .04, rng.integers(20, DIAS - 6, n), -1)
    oculto = pd.DataFrame({"cliente_id": cli.cliente_id, "abandono": churn, "disciplina": disciplina,
                           "intensidad_caida": caida, "pausa_ini": pausa, "viaje_ini": viaje})
    return cli, buro, oculto


# ---------------------------------------------------------------------------
# 2. App móvil: sesiones diarias (agregado) y eventos crudos de los últimos 7 días (JSONL anidado)
# ---------------------------------------------------------------------------
def generar_sesiones(cli, oculto):
    horas_pico = np.array([7, 8, 12, 13, 19, 20, 21, 22])
    diarias, eventos = [], []
    ini_crudo = DIAS - 7
    for i, c in enumerate(cli.itertuples()):
        lam = SEG_PARAMS[c.segmento][4] * rng.uniform(.6, 1.4)
        mult = np.ones(DIAS)
        if oculto.abandono.iat[i]:
            mult[DIAS - 60:] = np.maximum(.05, 1 - oculto.intensidad_caida.iat[i] * np.linspace(0, 1, 60))
        if oculto.pausa_ini.iat[i] >= 0:
            p0 = oculto.pausa_ini.iat[i]
            mult[p0:p0 + 25] *= .1
        cnt = rng.poisson(lam * mult)
        h = int(rng.choice(horas_pico))
        for d in np.nonzero(cnt)[0]:
            diarias.append({"cliente_id": c.cliente_id, "fecha": FECHAS[d].isoformat(), "n_sesiones": int(cnt[d]),
                            "hora_promedio": int(np.clip(h + rng.normal(0, 1), 0, 23))})
        for d in range(ini_crudo, DIAS):
            for _ in range(cnt[d]):
                ts = datetime.combine(FECHAS[d], time(int((h + rng.normal(0, 1.5)) % 24), int(rng.integers(0, 60))))
                evs, t = [{"t": 0, "tipo": "login", "metodo": str(rng.choice(["biometria", "pin"], p=[.7, .3]))}], 0
                for tipo, p in [("ver_saldo", .8), ("ver_movimientos", .5), ("transferencia", .22), ("ver_oferta", .2)]:
                    if rng.random() < p:
                        t += int(rng.integers(5, 90))
                        e = {"t": t, "tipo": tipo}
                        if tipo == "ver_oferta":
                            e["producto"] = str(rng.choice(list(PRODUCTOS)))
                            e["respuesta"] = str(rng.choice(["ignorar", "rechazar", "aceptar"], p=[.62, .30, .08]))
                        evs.append(e)
                evs.append({"t": t + int(rng.integers(5, 40)), "tipo": "logout"})
                eventos.append({"session_id": f"S{len(eventos) + 1:07d}", "cliente_id": c.cliente_id, "ts_inicio": ts.isoformat(),
                                "app": {"version": "5.3.0", "plataforma": c.so}, "dispositivo_id": c.dispositivo_principal, "eventos": evs})
    return pd.DataFrame(diarias), eventos


# ---------------------------------------------------------------------------
# 3. Switch de tarjetas: transacciones + fraude inyectado (contracargos confirmados aparte)
# ---------------------------------------------------------------------------
def multiplicador_base():
    m = np.ones(DIAS)
    for d, f in enumerate(FECHAS):
        if f.weekday() in (4, 5):
            m[d] *= 1.25
        if f.day in (1, 15, 16) or (f + timedelta(days=1)).day == 1:
            m[d] *= 1.3
        if f.month == 9 and f.day in (14, 15):
            m[d] *= 1.4
    return m


def generar_transacciones(cli, oculto):
    base = multiplicador_base()
    cats = np.array(CAT_NOMBRES)
    meses = sorted({(f.year, f.month) for f in FECHAS})
    partes = []
    for i, c in enumerate(cli.itertuples()):
        es_churn = bool(oculto.abandono.iat[i])
        lam = np.clip(.12 + c.ingreso_mensual_gtq / 45000, .18, .8)
        mult = base.copy()
        if es_churn:
            mult[DIAS - 60:] *= np.maximum(.1, 1 - oculto.intensidad_caida.iat[i] * np.linspace(0, 1, 60))
        if oculto.pausa_ini.iat[i] >= 0:
            p0 = oculto.pausa_ini.iat[i]
            mult[p0:p0 + 25] *= .25
        dia = np.repeat(np.arange(DIAS), rng.poisson(lam * mult))
        m = len(dia)
        pref = np.array([BASE_PREF[k] * AJUSTE_SEG[c.segmento].get(k, 1) for k in CAT_NOMBRES])
        pref = rng.dirichlet(pref / pref.sum() * 40)
        cat = rng.choice(cats, size=m, p=pref)
        med = np.array([CATEGORIAS[k][0] for k in cat])
        sig = np.array([CATEGORIAS[k][1] for k in cat])
        monto = np.round(rng.lognormal(np.log(med) + .35 * np.log(c.ingreso_mensual_gtq / 7000), sig), 2)
        canal = np.array([CATEGORIAS[k][2] for k in cat], dtype=object)
        lat = c.lat_hogar + rng.normal(0, .03, m)
        lon = c.lon_hogar + rng.normal(0, .03, m)
        pais = np.full(m, "GT", dtype=object)
        intl = np.isin(cat, ["viajes", "ecommerce"]) & (rng.random(m) < .08)
        for j in np.nonzero(intl)[0]:
            p = str(rng.choice(list(PAISES)))
            pais[j], lat[j], lon[j] = p, PAISES[p][0], PAISES[p][1]
        if oculto.viaje_ini.iat[i] >= 0:
            v0, destino = oculto.viaje_ini.iat[i], str(rng.choice(["US", "MX", "ES"]))
            en_viaje = (dia >= v0) & (dia < v0 + int(rng.integers(3, 7))) & (canal != "app")
            pais[en_viaje] = destino
            lat[en_viaje] = PAISES[destino][0] + rng.normal(0, .05, en_viaje.sum())
            lon[en_viaje] = PAISES[destino][1] + rng.normal(0, .05, en_viaje.sum())
        disp = np.full(m, "", dtype=object)
        digital = np.isin(canal, ["ecommerce", "app"])
        disp[digital] = c.dispositivo_principal
        if c.dispositivo_secundario:
            disp[digital & (rng.random(m) < .2)] = c.dispositivo_secundario
        deb = pd.DataFrame({
            "cliente_id": c.cliente_id, "dia": dia, "hora": rng.choice(24, size=m, p=HORAS_P), "minuto": rng.integers(0, 60, m),
            "tipo": "debito", "categoria": cat, "monto_gtq": monto, "canal": canal,
            "comercio": [random.choice(COMERCIOS[k]) for k in cat], "pais": pais,
            "lat": np.round(lat, 4), "lon": np.round(lon, 4), "dispositivo_id": disp, "fraude": 0})
        cred = []
        corte = DIAS - int(rng.integers(10, 45)) if ((es_churn and rng.random() < .4) or rng.random() < .03) else DIAS
        for d, f in enumerate(FECHAS):
            if c.empleo_formal and d < corte and (f.day == 15 or (f + timedelta(days=1)).day == 1):
                cred.append((d, "planilla", round(c.ingreso_mensual_gtq / 2 * rng.uniform(.97, 1.03), 2), "Pago de planilla"))
        if c.segmento == "Emprendedor":
            for d in rng.integers(0, DIAS, rng.poisson(6 * DIAS / 30)):
                if d < corte:
                    cred.append((int(d), "deposito_negocio", round(c.ingreso_mensual_gtq / 6 * rng.lognormal(0, .3), 2), "Depósito de ventas"))
        if c.recibe_remesas:
            for (y, mth) in meses:
                dias_mes = [k for k, f in enumerate(FECHAS) if f.year == y and f.month == mth]
                cred.append((int(rng.choice(dias_mes)), "remesa_recibida", round(rng.lognormal(np.log(380), .35) * 7.7, 2), "Remesa familiar"))
        if cred:
            cd = pd.DataFrame(cred, columns=["dia", "categoria", "monto_gtq", "comercio"])
            k = len(cd)
            cd = cd.assign(cliente_id=c.cliente_id, hora=rng.integers(6, 20, k), minuto=rng.integers(0, 60, k), tipo="credito",
                           canal="abono", pais="GT", lat=round(c.lat_hogar, 4), lon=round(c.lon_hogar, 4), dispositivo_id="", fraude=0)
            partes.append(cd[deb.columns])
        partes.append(deb)
    tx = pd.concat(partes, ignore_index=True)
    return inyectar_fraude(tx)


def inyectar_fraude(tx):
    cand = tx.index[(tx.tipo == "debito") & tx.canal.isin(["POS", "ecommerce"])].to_numpy()
    sel = rng.choice(cand, size=int(len(cand) * .0035), replace=False)
    k = len(sel)
    sigiloso = rng.random(k) < .4
    factor = np.where(sigiloso, rng.uniform(1.2, 3, k), rng.uniform(4, 12, k))
    tx.loc[sel, "monto_gtq"] = (tx.loc[sel, "monto_gtq"].to_numpy() * factor).round(2)
    evidentes = sel[~sigiloso]
    tx.loc[evidentes, "hora"] = rng.integers(0, 5, len(evidentes))
    tx.loc[sel, "canal"] = "ecommerce"
    tx.loc[sel, "categoria"] = rng.choice(["ecommerce", "viajes", "ropa_calzado"], k)
    tx.loc[sel, "dispositivo_id"] = [f"dev_{rng.integers(10 ** 7, 10 ** 8)}" for _ in range(k)]
    tx.loc[sel, "fraude"] = 1
    for j in evidentes:
        u = rng.random()
        if u < .35:
            p = str(rng.choice(list(PAISES)))
            tx.loc[j, ["pais", "lat", "lon"]] = [p, PAISES[p][0], PAISES[p][1]]
        elif u < .65:
            dd = DEPTOS[rng.integers(1, len(DEPTOS))]
            tx.loc[j, ["lat", "lon"]] = [round(dd[1], 4), round(dd[2], 4)]
    extra = []
    for j in evidentes[rng.random(len(evidentes)) < .35]:
        base = tx.loc[j].copy()
        for paso in (1, 2):
            e = base.copy()
            e["minuto"] = min(59, int(base["minuto"]) + paso * int(rng.integers(3, 9)))
            e["monto_gtq"] = round(float(base["monto_gtq"]) * rng.uniform(.5, 1.2), 2)
            extra.append(e)
    if extra:
        tx = pd.concat([tx, pd.DataFrame(extra)], ignore_index=True)
    tx["ts"] = pd.to_datetime([FECHAS[d] for d in tx.dia]) + pd.to_timedelta(tx.hora.astype(int) * 3600 + tx.minuto.astype(int) * 60, unit="s")
    tx = tx.sort_values(["ts", "cliente_id"]).reset_index(drop=True)
    tx.insert(0, "txn_id", [f"TX{i + 1:08d}" for i in range(len(tx))])
    return tx


# ---------------------------------------------------------------------------
# 4. Call center, saldos y campañas del CRM (con aceptación dependiente del perfil)
# ---------------------------------------------------------------------------
def generar_interacciones(cli, oculto):
    filas = []
    for i, c in enumerate(cli.itertuples()):
        churn = bool(oculto.abandono.iat[i])
        for _ in range(rng.poisson(1.1 if churn else .6)):
            intent = str(rng.choice(INTENCIONES, p=P_INT_ABANDONO if churn else P_INT_NORMAL))
            base, textos = PLANTILLAS[intent]
            d = int(rng.integers(DIAS - 90, DIAS))
            ts = datetime.combine(FECHAS[d], time(int(rng.integers(7, 22)), int(rng.integers(0, 60))))
            filas.append({"interaccion_id": f"INT{len(filas) + 1:06d}", "cliente_id": c.cliente_id,
                          "canal": str(rng.choice(["chat_app", "whatsapp", "email", "llamada"], p=[.45, .25, .15, .15])),
                          "ts": ts.isoformat(), "intencion": intent, "texto": random.choice(textos),
                          "sentimiento": round(float(np.clip(base + rng.normal(0, .15), -1, 1)), 2)})
    return pd.DataFrame(filas)


def generar_saldos(cli, oculto):
    meses = sorted({(f.year, f.month) for f in FECHAS})
    filas = []
    for i, c in enumerate(cli.itertuples()):
        base = c.ingreso_mensual_gtq * rng.lognormal(.2, .6) * (2.5 if c.tiene_plazo_fijo else 1)
        caida = [1.0] * len(meses)
        if oculto.abandono.iat[i] and oculto.intensidad_caida.iat[i] > .5:
            caida[-2], caida[-1] = rng.uniform(.6, .9), rng.uniform(.25, .6)
        elif rng.random() < .08:
            caida[-1] = rng.uniform(.5, .8)
        for k, (y, mth) in enumerate(meses):
            filas.append({"cliente_id": c.cliente_id, "anio_mes": f"{y}-{mth:02d}",
                          "saldo_promedio_gtq": round(base * (1 + .01 * k) * rng.normal(1, .12) * caida[k], 2)})
    return pd.DataFrame(filas)


def generar_campanas(cli, buro, tx, saldos):
    """Ofertas históricas del CRM. La aceptación depende del perfil (verdad simulada que el modelo debe aprender)."""
    f = cli.merge(buro, on="cliente_id")
    deb = tx[tx.tipo == "debito"]
    piv = deb.pivot_table(index="cliente_id", columns="categoria", values="monto_gtq", aggfunc="sum").reindex(f.cliente_id).fillna(0)
    sal = saldos.groupby("cliente_id").saldo_promedio_gtq.mean().reindex(f.cliente_id).fillna(0).to_numpy()
    seg, ev = f.segmento.to_numpy(), f.evento_vida_reciente.fillna("").to_numpy()
    score = f.score.fillna(650).to_numpy()
    mora = f.max_dias_mora_12m.fillna(0).to_numpy()
    ratio = (f.deuda_total_gtq.fillna(0) / (f.ingreso_mensual_gtq * 12)).to_numpy()
    z_estilo = zscore(np.log1p(piv.get("viajes", 0) + piv.get("restaurantes", 0) + piv.get("ecommerce", 0)))
    logit = {
        "tarjeta_credito": -2.7 + 1.76 * z_estilo + 1.28 * (f.ingreso_mensual_gtq > 6000) - 2.40 * (score < 580) + 0.96 * (ev == "nuevo_empleo"),
        "prestamo_personal": -3.0 + 1.44 * ((f.edad >= 25) & (f.edad <= 45)) + 1.12 * zscore(np.log1p(piv.get("educacion", 0) + piv.get("servicios_basicos", 0)))
                             + 0.96 * (ratio < .3) - 1.92 * (mora >= 30) + 0.80 * (ev == "mudanza"),
        "cuenta_ahorro_meta": -2.6 + 2.08 * f.recibe_remesas + 1.28 * (seg == "Joven digital"),
        "seguro_vida": -3.2 + 1.92 * (seg == "Familiar") + 1.28 * f.tiene_prestamo + 0.80 * ((f.edad >= 30) & (f.edad <= 55)) + 1.60 * np.isin(ev, ["nacimiento", "boda"]),
        "plazo_fijo": -3.4 + 2.24 * zscore(np.log1p(sal)) + 1.60 * np.isin(seg, ["Senior", "Profesional"]),
        "credito_pyme": -3.8 + 2.60 * (seg == "Emprendedor") + 0.96 * zscore(np.log1p(f.ingreso_mensual_gtq)),
    }
    logit = {k: np.asarray(v, dtype=float) for k, v in logit.items()}
    efecto_canal = {"push_app": .4 * (seg == "Joven digital"), "email": np.full(len(f), -.4), "asesor": .6 * np.isin(seg, ["Senior", "Emprendedor"]) - .2}
    filas = []
    for i, c in enumerate(f.itertuples()):
        disponibles = [p for p, (col, _) in PRODUCTOS.items() if not getattr(c, col) and (p != "credito_pyme" or c.segmento == "Emprendedor")]
        if not disponibles:
            continue
        for _ in range(1 + rng.poisson(1.2)):
            p = str(rng.choice(disponibles))
            canal = str(rng.choice(CANALES_OFERTA, p=[.5, .3, .2]))
            prob = sigmoid(logit[p][i] + efecto_canal[canal][i] + rng.normal(0, .3))
            fecha = FECHAS[int(rng.integers(0, DIAS))]
            filas.append({"oferta_id": f"OF{len(filas) + 1:06d}", "cliente_id": c.cliente_id, "fecha": fecha.isoformat(),
                          "producto": p, "canal": canal, "aceptada": int(rng.random() < prob)})
    return pd.DataFrame(filas)


# ---------------------------------------------------------------------------
# 5. Escritura de la zona Bronze (tal como llega cada fuente, sin transformar)
# ---------------------------------------------------------------------------
def escribir_bronze(cli, buro, oculto, tx, ses_diarias, eventos, inter, saldos, campanas):
    if BRONZE.exists():
        shutil.rmtree(BRONZE)
    cli.to_csv(ruta_bronze("clientes"), index=False, encoding="utf-8")
    saldos.to_csv(ruta_bronze("saldos_mensuales"), index=False, encoding="utf-8")
    # el switch no sabe qué fue fraude: la etiqueta llega después como contracargo confirmado
    tx.drop(columns=["fraude", "dia", "hora", "minuto"]).to_csv(ruta_bronze("transacciones"), index=False, encoding="utf-8")
    tx[tx.fraude == 1][["txn_id", "cliente_id"]].assign(fuente="contracargo_confirmado", fecha_confirmacion=FECHA_FIN.isoformat()) \
        .to_csv(ruta_bronze("contracargos"), index=False, encoding="utf-8")
    ses_diarias.to_csv(ruta_bronze("sesiones_diarias"), index=False, encoding="utf-8")
    with open(ruta_bronze("eventos_app"), "w", encoding="utf-8") as fh:
        for e in eventos:
            fh.write(json.dumps(e, ensure_ascii=False) + "\n")
    respuestas = [{"cliente_id": r.cliente_id, "fecha_consulta": FECHA_FIN.isoformat(), "proveedor": "Buró sintético GT",
                   "resultado": None if r.sin_historial else {"score": int(r.score), "max_dias_mora_12m": int(r.max_dias_mora_12m),
                                                               "deuda_total_gtq": float(r.deuda_total_gtq), "consultas_6m": int(r.consultas_6m)}}
                  for r in buro.itertuples()]
    ruta_bronze("buro_credito").write_text(json.dumps(respuestas, ensure_ascii=False, indent=1), encoding="utf-8")
    campanas.to_csv(ruta_bronze("campanas_historial"), index=False, encoding="utf-8")
    # bajas observadas: resultado de la cohorte anterior (el banco ya sabe quién se fue); sirve para entrenar abandono
    oculto[["cliente_id", "abandono"]].rename(columns={"abandono": "baja_60d"}).assign(fuente="cierre_cuenta_o_inactividad") \
        .to_csv(ruta_bronze("etiquetas_abandono"), index=False, encoding="utf-8")
    inter.to_csv(ruta_bronze("interacciones"), index=False, encoding="utf-8")


def main():
    print("Generando fuentes sintéticas…")
    cli, buro, oculto = generar_clientes()
    ses_diarias, eventos = generar_sesiones(cli, oculto)
    tx = generar_transacciones(cli, oculto)
    inter = generar_interacciones(cli, oculto)
    saldos = generar_saldos(cli, oculto)
    campanas = generar_campanas(cli, buro, tx, saldos)
    escribir_bronze(cli, buro, oculto, tx, ses_diarias, eventos, inter, saldos, campanas)
    print(f"  Clientes: {len(cli):,} | Transacciones: {len(tx):,} | Sesiones-día: {len(ses_diarias):,} | Eventos crudos: {len(eventos):,}")
    print(f"  Ofertas históricas: {len(campanas):,} (aceptación {campanas.aceptada.mean():.1%}) | Abandono oculto: {oculto.abandono.mean():.1%} | Fraudes: {int(tx.fraude.sum())}")
    print(f"  Bronze escrito en {BRONZE}")


if __name__ == "__main__":
    main()

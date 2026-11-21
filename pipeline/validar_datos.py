# -*- coding: utf-8 -*-
"""Paso 2 del pipeline: valida los datos sintéticos de la zona Bronze.

Revisa, por fuente: cantidad de registros, rangos de las variables clave, porcentaje de nulos,
duplicados de llave y forma de las distribuciones (percentiles y proporciones por categoría).
Escribe el resultado en data/reportes/validacion_datos.json y .md para incluirlo en el informe.

Ejecución:  python pipeline/validar_datos.py
Devuelve código de salida 1 si alguna verificación falla.
"""
import json
import sys

import pandas as pd

from config import BRONZE, FUENTES_BRONZE, N_CLIENTES, PART_INGESTA, REPORTES, SEGMENTOS

LLAVES = {"clientes": ["cliente_id"], "transacciones": ["txn_id"], "campanas_historial": ["oferta_id"],
          "interacciones": ["interaccion_id"], "saldos_mensuales": ["cliente_id", "anio_mes"],
          "sesiones_diarias": ["cliente_id", "fecha"], "etiquetas_abandono": ["cliente_id"], "contracargos": ["txn_id"]}


def leer_bronze(fuente):
    carpeta, archivo = FUENTES_BRONZE[fuente]
    ruta = BRONZE / carpeta / PART_INGESTA / archivo
    if archivo.endswith(".csv"):
        return pd.read_csv(ruta)
    if archivo.endswith(".jsonl"):
        return pd.read_json(ruta, lines=True)
    return pd.json_normalize(json.loads(ruta.read_text(encoding="utf-8")))


def check(resultados, nombre, ok, detalle):
    resultados.append({"verificacion": nombre, "ok": bool(ok), "detalle": detalle})


def validar():
    resultados = []
    cli = leer_bronze("clientes")
    tx = leer_bronze("transacciones")
    camp = leer_bronze("campanas_historial")
    ses = leer_bronze("sesiones_diarias")
    sal = leer_bronze("saldos_mensuales")
    buro = leer_bronze("buro_credito")

    # 1. Volumen
    check(resultados, "Al menos 1,000 clientes", len(cli) >= 1000, f"{len(cli):,} clientes (objetivo {N_CLIENTES:,})")
    check(resultados, "Cada cliente tiene transacciones", tx.cliente_id.nunique() >= .97 * len(cli),
          f"{tx.cliente_id.nunique():,} de {len(cli):,} clientes con movimientos")
    # 2. Duplicados de llave
    for fuente, llave in LLAVES.items():
        df = leer_bronze(fuente)
        dup = int(df.duplicated(subset=llave).sum())
        check(resultados, f"Sin duplicados en {fuente} ({'+'.join(llave)})", dup == 0, f"{dup} duplicados en {len(df):,} filas")
    # 3. Nulos en columnas obligatorias
    for fuente, cols in {"clientes": ["cliente_id", "segmento", "ingreso_mensual_gtq", "departamento"],
                         "transacciones": ["txn_id", "cliente_id", "ts", "monto_gtq", "categoria"],
                         "campanas_historial": ["cliente_id", "producto", "canal", "aceptada"]}.items():
        df = leer_bronze(fuente)
        nulos = df[cols].isna().sum()
        check(resultados, f"Sin nulos obligatorios en {fuente}", int(nulos.sum()) == 0, nulos.to_dict())
    # 4. Rangos
    check(resultados, "Edad entre 18 y 75", cli.edad.between(18, 75).all(), f"min {cli.edad.min()}, max {cli.edad.max()}")
    check(resultados, "Ingreso mensual entre Q500 y Q150,000", cli.ingreso_mensual_gtq.between(500, 150000).all(),
          f"min Q{cli.ingreso_mensual_gtq.min():,.0f}, mediana Q{cli.ingreso_mensual_gtq.median():,.0f}, max Q{cli.ingreso_mensual_gtq.max():,.0f}")
    check(resultados, "Montos de transacción positivos y < Q100,000", (tx.monto_gtq > 0).all() and (tx.monto_gtq < 100000).all(),
          f"min Q{tx.monto_gtq.min():.2f}, mediana Q{tx.monto_gtq.median():.2f}, p99 Q{tx.monto_gtq.quantile(.99):,.2f}, max Q{tx.monto_gtq.max():,.2f}")
    scores = buro["resultado.score"].dropna()
    check(resultados, "Score de buró entre 300 y 850", scores.between(300, 850).all(), f"{len(scores):,} con buró; sin historial: {buro['resultado.score'].isna().sum():,}")
    check(resultados, "Aceptación de ofertas es 0/1", set(camp.aceptada.unique()) <= {0, 1}, f"tasa de aceptación {camp.aceptada.mean():.1%}")
    check(resultados, "Saldos promedio no negativos", (sal.saldo_promedio_gtq >= 0).all(), f"mediana Q{sal.saldo_promedio_gtq.median():,.0f}")
    check(resultados, "Sesiones diarias entre 1 y 20", ses.n_sesiones.between(1, 20).all(), f"max {ses.n_sesiones.max()}")
    # 5. Distribuciones
    seg_pct = cli.segmento.value_counts(normalize=True).round(3).to_dict()
    check(resultados, "Los 5 segmentos están presentes", set(seg_pct) == set(SEGMENTOS), seg_pct)
    tasa = camp.aceptada.mean()
    check(resultados, "Tasa de aceptación histórica realista (5 % a 40 %)", .05 <= tasa <= .40, f"{tasa:.1%}")
    frac_fraude = len(leer_bronze("contracargos")) / len(tx)
    check(resultados, "Fraude confirmado entre 0.1 % y 1 % de transacciones", .001 <= frac_fraude <= .01, f"{frac_fraude:.3%}")
    cat_pct = tx[tx.tipo == "debito"].categoria.value_counts(normalize=True).round(3).head(6).to_dict()
    check(resultados, "Supermercado es la categoría más frecuente", next(iter(cat_pct)) == "supermercado", cat_pct)

    resumen = {
        "fecha_ingesta": PART_INGESTA.split("=")[1],
        "fuentes": {f: int(len(leer_bronze(f))) for f in FUENTES_BRONZE},
        "percentiles_monto_gtq": tx.monto_gtq.quantile([.05, .25, .5, .75, .95]).round(2).to_dict(),
        "percentiles_ingreso_gtq": cli.ingreso_mensual_gtq.quantile([.05, .25, .5, .75, .95]).round(0).to_dict(),
        "segmentos": seg_pct, "departamentos_top5": cli.departamento.value_counts().head(5).to_dict(),
        "verificaciones": resultados, "total": len(resultados), "aprobadas": sum(r["ok"] for r in resultados),
    }
    return resumen


def escribir_reporte(resumen):
    REPORTES.mkdir(parents=True, exist_ok=True)
    (REPORTES / "validacion_datos.json").write_text(json.dumps(resumen, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    lineas = ["# Validación de datos sintéticos (zona Bronze)", "",
              f"Fecha de ingesta: {resumen['fecha_ingesta']}. Verificaciones aprobadas: **{resumen['aprobadas']} de {resumen['total']}**.", "",
              "| Fuente | Registros |", "|---|---:|"] + [f"| {f} | {n:,} |" for f, n in resumen["fuentes"].items()]
    lineas += ["", "| Verificación | Resultado | Detalle |", "|---|---|---|"]
    lineas += [f"| {r['verificacion']} | {'OK' if r['ok'] else 'FALLA'} | {r['detalle']} |" for r in resumen["verificaciones"]]
    (REPORTES / "validacion_datos.md").write_text("\n".join(lineas) + "\n", encoding="utf-8")


def main():
    resumen = validar()
    escribir_reporte(resumen)
    for r in resumen["verificaciones"]:
        print(f"  [{'OK' if r['ok'] else 'FALLA'}] {r['verificacion']}: {r['detalle']}")
    print(f"Validación: {resumen['aprobadas']}/{resumen['total']} aprobadas. Reporte en {REPORTES}")
    return 0 if resumen["aprobadas"] == resumen["total"] else 1


if __name__ == "__main__":
    sys.exit(main())

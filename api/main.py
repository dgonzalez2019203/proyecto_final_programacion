# -*- coding: utf-8 -*-
"""API de hiperpersonalización de NovaBank (FastAPI).

Expone las predicciones y acciones de la zona Gold de SQLite. En el diseño del MP2 esta capa
corre en Cloud Run detrás de Apigee; aquí corre en local con uvicorn.

Ejecución:  uvicorn api.main:app --reload --port 8000
Documentación interactiva: http://localhost:8000/docs
Tablero en vivo:           http://localhost:8000/tablero
"""
import sqlite3
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "pipeline"))
from config import DB_PATH, VERSION_NBO  # noqa: E402

app = FastAPI(title="NovaBank · API de hiperpersonalización (MVP)", version="1.0.0",
              description="Prototipo académico con datos 100 % sintéticos. Devuelve la siguiente mejor acción por cliente.")


def conectar():
    if not DB_PATH.exists():
        raise HTTPException(503, "La base de datos no existe. Ejecute python pipeline/ejecutar_pipeline.py")
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


class Accion(BaseModel):
    cliente_id: str
    tipo_accion: str
    descripcion: str
    canal: Optional[str] = None
    disparador: str = "manual"
    detalle: Optional[str] = None


@app.get("/salud", tags=["Operación"])
def salud():
    con = conectar()
    n = con.execute("SELECT COUNT(*) FROM gold_siguiente_mejor_accion").fetchone()[0]
    fecha = con.execute("SELECT MAX(fecha_decision) FROM gold_siguiente_mejor_accion").fetchone()[0]
    con.close()
    return {"estado": "ok", "clientes": n, "fecha_decision": fecha, "modelo_nbo": VERSION_NBO}


@app.get("/recomendacion/{cliente_id}", tags=["Hiperpersonalización"])
def recomendacion(cliente_id: str):
    """Siguiente mejor acción para un cliente, con la oferta NBO, el riesgo de abandono y la regla aplicada."""
    con = conectar()
    fila = con.execute("""
        SELECT a.cliente_id, a.tipo_accion, a.siguiente_mejor_accion, a.regla_decision, a.estado, a.fecha_decision,
               n.producto_recomendado, n.producto_nombre, n.prob_aceptacion, n.canal_optimo, n.hora_optima, n.regla_aplicada,
               n.productos_bloqueados, n.top3_productos, n.modelo_version,
               b.prob_abandono_60d, b.nivel_riesgo, b.razon_principal,
               f.segmento, f.departamento, f.edad, f.n_productos, f.gasto_90d, f.n_sesiones_30d
        FROM gold_siguiente_mejor_accion a
        JOIN gold_pred_nbo n USING (cliente_id)
        JOIN gold_pred_abandono b USING (cliente_id)
        JOIN gold_features_cliente f USING (cliente_id)
        WHERE a.cliente_id = ?""", (cliente_id,)).fetchone()
    con.close()
    if fila is None:
        raise HTTPException(404, f"Cliente {cliente_id} no encontrado")
    d = dict(fila)
    return {
        "cliente_id": d["cliente_id"], "tipo_accion": d["tipo_accion"], "siguiente_mejor_accion": d["siguiente_mejor_accion"],
        "regla_decision": d["regla_decision"], "estado": d["estado"], "fecha_decision": d["fecha_decision"],
        "oferta": {"producto": d["producto_recomendado"], "nombre": d["producto_nombre"], "prob_aceptacion": d["prob_aceptacion"],
                   "canal": d["canal_optimo"], "hora": d["hora_optima"], "regla_aplicada": d["regla_aplicada"],
                   "productos_bloqueados": d["productos_bloqueados"], "top3": d["top3_productos"]},
        "prob_abandono_60d": d["prob_abandono_60d"], "nivel_riesgo_abandono": d["nivel_riesgo"], "razon_abandono": d["razon_principal"],
        "perfil": {"segmento": d["segmento"], "departamento": d["departamento"], "edad": d["edad"], "n_productos": d["n_productos"],
                   "gasto_90d_gtq": d["gasto_90d"], "sesiones_app_30d": d["n_sesiones_30d"]},
        "modelo_version": d["modelo_version"], "aviso": "Datos sintéticos: prototipo académico",
    }


@app.get("/recomendaciones", tags=["Hiperpersonalización"])
def recomendaciones(tipo: Optional[str] = Query(None, description="Seguridad, Retención, Oferta o Educación"),
                    estado: Optional[str] = Query(None, description="pendiente o enviada"), limite: int = Query(100, le=2000)):
    """Lista de acciones recomendadas (la consume el flujo de automatización)."""
    con = conectar()
    sql, args = "SELECT * FROM gold_siguiente_mejor_accion WHERE 1=1", []
    if tipo:
        sql, args = sql + " AND tipo_accion = ?", args + [tipo]
    if estado:
        sql, args = sql + " AND estado = ?", args + [estado]
    # prioridad del motor de decisión: seguridad > retención > oferta (por probabilidad) > educación
    orden = " ORDER BY CASE tipo_accion WHEN 'Seguridad' THEN 0 WHEN 'Retención' THEN 1 WHEN 'Oferta' THEN 2 ELSE 3 END, prob_aceptacion DESC LIMIT ?"
    filas = [dict(r) for r in con.execute(sql + orden, args + [limite]).fetchall()]
    con.close()
    return {"total": len(filas), "recomendaciones": filas}


@app.post("/acciones", status_code=201, tags=["Automatización"])
def registrar_accion(accion: Accion):
    """Registra una acción ejecutada (correo, push, llamada) y marca la recomendación como enviada."""
    con = conectar()
    accion_id = f"ACC-{uuid.uuid4().hex[:10].upper()}"
    con.execute("INSERT INTO gold_acciones_enviadas VALUES (?,?,?,?,?,?,?,?,?)",
                (accion_id, accion.cliente_id, accion.tipo_accion, accion.descripcion, accion.canal, "enviada",
                 datetime.now().isoformat(timespec="seconds"), accion.disparador, accion.detalle))
    con.execute("UPDATE gold_siguiente_mejor_accion SET estado = 'enviada' WHERE cliente_id = ?", (accion.cliente_id,))
    con.commit()
    con.close()
    return {"accion_id": accion_id, "estado": "enviada"}


@app.get("/metricas", tags=["Operación"])
def metricas():
    con = conectar()
    filas = [dict(r) for r in con.execute("SELECT modelo, nombre, version, metrica, valor FROM gold_metricas_modelos").fetchall()]
    con.close()
    return {"metricas": filas, "aviso": "Calculadas sobre datos sintéticos"}


def _reentrenar():
    import subprocess
    base = Path(__file__).resolve().parents[1]
    for paso in ("transformar.py", "modelos.py", "predecir.py"):
        subprocess.run([sys.executable, str(base / "pipeline" / paso)], check=True, cwd=str(base / "pipeline"))


@app.post("/ejecutar-pipeline", status_code=202, tags=["Automatización"])
def ejecutar_pipeline(tareas: BackgroundTasks):
    """Disparador del pipeline (lo que en Google Cloud invocaría Cloud Scheduler): recalcula features, modelos y predicciones."""
    tareas.add_task(_reentrenar)
    return {"estado": "aceptado", "detalle": "El pipeline corre en segundo plano (transformar → modelos → predecir)"}


# ------------------------------------------------------------------ tablero en vivo
TABLERO = Path(__file__).resolve().parents[1] / "tablero" / "index.html"


def _tabla_existe(con, nombre):
    return con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (nombre,)).fetchone() is not None


def _firma(con):
    """Cambia cada vez que corre el pipeline o la automatización registra una acción."""
    ultima = con.execute("SELECT MAX(ts) FROM registro_ejecuciones").fetchone()[0] if _tabla_existe(con, "registro_ejecuciones") else ""
    enviadas = con.execute("SELECT COUNT(*) FROM gold_acciones_enviadas").fetchone()[0] if _tabla_existe(con, "gold_acciones_enviadas") else 0
    pend = con.execute("SELECT COUNT(*) FROM gold_siguiente_mejor_accion WHERE estado = 'pendiente'").fetchone()[0]
    return f"{ultima}|{enviadas}|{pend}"


@app.get("/", include_in_schema=False)
def inicio():
    return RedirectResponse("/tablero")


@app.get("/tablero", include_in_schema=False)
def tablero():
    return FileResponse(TABLERO, media_type="text/html")


@app.get("/tablero/estado", tags=["Tablero"])
def tablero_estado():
    """Firma de los datos: el tablero la consulta cada pocos segundos y se recarga cuando cambia."""
    con = conectar()
    try:
        return {"firma": _firma(con)}
    finally:
        con.close()


@app.get("/tablero/datos", tags=["Tablero"])
def tablero_datos():
    """Todo lo que dibuja el tablero, leído en el momento de la zona Gold."""
    con = conectar()
    try:
        q = lambda sql: [dict(r) for r in con.execute(sql).fetchall()]  # noqa: E731
        acciones = q("""
            SELECT a.cliente_id, f.segmento, f.departamento, a.tipo_accion, a.siguiente_mejor_accion, a.regla_decision,
                   n.producto_nombre, a.canal, a.prob_aceptacion, a.prob_abandono_60d, b.nivel_riesgo, a.estado
            FROM gold_siguiente_mejor_accion a
            JOIN gold_features_cliente f USING (cliente_id)
            JOIN gold_pred_nbo n USING (cliente_id)
            JOIN gold_pred_abandono b USING (cliente_id)""")
        return {
            "firma": _firma(con),
            "acciones": acciones,
            "kpi_diarios": q("SELECT fecha, monto_total_gtq, transacciones, ofertas, aceptadas, alertas_fraude FROM gold_kpi_diarios ORDER BY fecha"),
            "alertas": q("SELECT ts, cliente_id, monto_gtq, pais, score_anomalia, decision, motivos FROM gold_alertas_fraude ORDER BY score_anomalia DESC"),
            "metricas": q("SELECT modelo, nombre, algoritmo, version, metrica, valor FROM gold_metricas_modelos ORDER BY nombre, metrica"),
            "enviadas_total": con.execute("SELECT COUNT(*) FROM gold_acciones_enviadas").fetchone()[0],
            "enviadas": q("SELECT ts_envio, cliente_id, tipo_accion, descripcion, disparador FROM gold_acciones_enviadas ORDER BY ts_envio DESC LIMIT 12"),
            "ejecuciones": q("SELECT ts, semilla, clientes, duracion_s FROM registro_ejecuciones ORDER BY ts DESC LIMIT 6")
                           if _tabla_existe(con, "registro_ejecuciones") else [],
        }
    finally:
        con.close()

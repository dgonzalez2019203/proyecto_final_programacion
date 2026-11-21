# -*- coding: utf-8 -*-
"""Exporta la zona Gold a CSV para herramientas de BI que no leen SQLite directamente (Looker Studio vía Google Sheets o BigQuery).

Power BI se conecta a data/novabank.db por ODBC; Looker Studio necesita una fuente en Google, así que
este paso deja los CSV listos para subirlos a Google Sheets o cargarlos con `bq load` al sandbox de BigQuery.

Ejecución:  python pipeline/exportar_gold.py
Salida:     data/looker_studio/*.csv
"""
import pandas as pd

from config import DATA
from ingesta import conectar

TABLAS = ["gold_siguiente_mejor_accion", "gold_pred_nbo", "gold_pred_abandono", "gold_alertas_fraude",
          "gold_features_cliente", "gold_kpi_diarios", "gold_metricas_modelos", "gold_acciones_enviadas"]


def main():
    salida = DATA / "looker_studio"
    salida.mkdir(parents=True, exist_ok=True)
    con = conectar()
    for t in TABLAS:
        df = pd.read_sql(f"SELECT * FROM {t}", con)
        if t == "gold_features_cliente":   # Looker Studio solo necesita el perfil, no los tokens ni coordenadas
            df = df[["cliente_id", "segmento", "departamento", "urbano", "edad", "ingreso_mensual_gtq", "antiguedad_meses",
                     "n_productos", "gasto_30d", "gasto_90d", "n_sesiones_30d", "dias_sin_login", "saldo_prom_3m", "sin_historial_buro", "score_buro"]]
        df.to_csv(salida / f"{t}.csv", index=False, encoding="utf-8")
        print(f"  {t}: {len(df):,} filas")
    # vista plana para el tablero: una fila por cliente con acción, oferta y riesgo
    plano = pd.read_sql("""
        SELECT a.cliente_id, f.segmento, f.departamento, f.edad, f.ingreso_mensual_gtq, f.n_productos, f.gasto_90d, f.n_sesiones_30d,
               a.tipo_accion, a.siguiente_mejor_accion, a.regla_decision, a.estado, a.fecha_decision,
               n.producto_nombre, n.prob_aceptacion, n.canal_optimo, n.hora_optima, n.regla_aplicada,
               b.prob_abandono_60d, b.nivel_riesgo, b.razon_principal
        FROM gold_siguiente_mejor_accion a JOIN gold_pred_nbo n USING (cliente_id)
        JOIN gold_pred_abandono b USING (cliente_id) JOIN gold_features_cliente f USING (cliente_id)""", con)
    plano.to_csv(salida / "vista_cliente_accion.csv", index=False, encoding="utf-8")
    print(f"  vista_cliente_accion: {len(plano):,} filas\nCSV en {salida}")
    con.close()


if __name__ == "__main__":
    main()

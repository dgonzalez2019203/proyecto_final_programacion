# Mini proyecto #3 · Prototipo funcional (MVP) de hiperpersonalización para NovaBank

Curso 131, Programación para la Ciencia de Datos (UMG). Autor: Diego Fernando González González (1113248945). Entrega: 20/11/2026.

NovaBank aprobó el diseño del Miniproyecto #2 y pidió un prototipo que funcione de principio a fin. Este repositorio implementa el
servicio de **siguiente mejor oferta (NBO)** con el **motor de decisión** del diseño, que además usa los scores de abandono y de
fraude para elegir una sola acción por cliente:

```
[Datos sintéticos] → [Ingesta Bronze] → [SQLite Bronze/Silver/Gold] → [Modelo NBO] → [Predicción + reglas R1–R8]
      → [API FastAPI] → [Automatización: trigger → predicción → regla → correo/registro] → [Tablero en Looker Studio]
```

**Todos los datos son sintéticos.** No se versionan: `pipeline/generar_datos.py` los genera con una semilla fija, sin datos de
personas reales. Lo mismo pasa con la base `data/novabank.db` y los modelos de `modelos/`, que se crean al correr el pipeline.

## Instalación

Requiere Python 3.12 y Git. En Windows (PowerShell):

```bash
git clone https://github.com/dgonzalez2019203/proyecto_final_programacion.git
cd proyecto_final_programacion
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

En Linux o macOS la activación es `source .venv/bin/activate`.

## Ejecución (de principio a fin en alrededor de un minuto)

```bash
python pipeline/ejecutar_pipeline.py
```

Corre los seis pasos: genera 1,200 clientes con 180 días de historia, valida los datos (24 verificaciones), carga Bronze y Silver a
`data/novabank.db`, construye las variables en Gold, entrena los tres modelos y escribe las predicciones con la acción recomendada.
Cada paso también puede correrse por separado (`python pipeline/<paso>.py`).

### Pruebas (ingesta + unitarias)

Primero hay que correr el pipeline, porque las pruebas revisan la base que construye.

```bash
pytest tests -v
```

| Archivo | Qué comprueba |
|---|---|
| `tests/test_ingesta.py` | **Pruebas de ingesta:** número de registros Bronze = archivo fuente, columnas y tipos, nulos en columnas obligatorias, duplicados de llave, PII fuera de Silver, integridad entre capas (46 casos) |
| `tests/test_validacion.py` | Unitaria 1: rangos y distribuciones del generador |
| `tests/test_reglas.py` | Unitaria 2: reglas R1 a R8 del motor de decisión, casos de frontera |
| `tests/test_modelo.py` | Unitaria 3: AUC mínimo, forma de las probabilidades, predicciones en Gold, trazabilidad |
| `tests/test_api.py` | Unitaria 4: endpoints de la API (200, 404, filtro, registro de acción, tablero) |

### API

```bash
uvicorn api.main:app --reload --port 8000
```

Documentación interactiva en http://localhost:8000/docs. Endpoints:

| Método y ruta | Qué devuelve |
|---|---|
| `GET /recomendacion/{cliente_id}` | Siguiente mejor acción del cliente: oferta (producto, canal, hora, probabilidad), riesgo de abandono, regla aplicada y versión del modelo |
| `GET /recomendaciones?tipo=Oferta&estado=pendiente` | Lista de acciones para el flujo de automatización |
| `POST /acciones` | Registra una acción ejecutada y marca la recomendación como enviada |
| `POST /ejecutar-pipeline` | Disparador del pipeline (lo que en la nube llama Cloud Scheduler) |
| `GET /salud`, `GET /metricas` | Estado del servicio y métricas de los modelos |

Ejemplo: `curl http://localhost:8000/recomendacion/NB-000696`

### Automatización

Con la API corriendo, en otra terminal:

```bash
python automatizacion/flujo_local.py --ahora
```

Obtiene las recomendaciones pendientes, aplica la regla de decisión y ejecuta la acción: genera el correo HTML de la oferta
(`automatizacion/salida/correos/`), programa la notificación push, agrega el cliente a la lista del asesor de retención o abre el
caso de fraude. Cada acción se registra en `gold_acciones_enviadas`. Para dejarlo programado: `--programar 07:00` o `--cada 5`.
El despliegue opcional en Cloud Run + Cloud Scheduler está en `automatizacion/cloud_run/`.

### Tablero en vivo (para la demo)

Con la API corriendo, abrir http://localhost:8000/tablero. Lee la zona Gold de SQLite y se actualiza solo cada vez que corre el
pipeline o la automatización. Tiene tres pestañas: Resumen, Cliente 360 y Modelos y fraude. No usa librerías externas, así que
funciona sin internet.

```bash
python pipeline/ejecutar_pipeline.py --semilla 7 --clientes 1500   # otros datos: cambia todo el tablero
python automatizacion/flujo_local.py --ahora --tipo Oferta --max 15 # acciones de oferta: suben las enviadas
python automatizacion/reiniciar_demo.py                            # todas las acciones vuelven a pendiente
```

### Tablero de BI (Looker Studio, en lugar de Power BI)

El tablero de BI está en Looker Studio, la herramienta de visualización del stack de Google Cloud del MP2. El cambio respecto a
Power BI se argumenta en la sección 9 del informe, por eso no hay archivo `.pbix`. Tiene tres páginas: resumen con 5 KPI y las
acciones recomendadas, Cliente 360 y modelos y fraude. La versión exportada está en `looker_studio/NovaBank_Dashboard.pdf`.

```bash
python pipeline/exportar_gold.py   # exporta las tablas gold_* a CSV (data/looker_studio/), la fuente del tablero
```

## Estructura

```
pipeline/        config.py, generar_datos.py, validar_datos.py, ingesta.py, transformar.py, reglas.py, modelos.py, predecir.py, ejecutar_pipeline.py
api/             main.py (FastAPI)
tablero/         index.html (tablero en vivo servido por la API)
automatizacion/  flujo_local.py, correo_oferta.html, cloud_run/ (Dockerfile y guía)
tests/           pruebas de ingesta y unitarias (pytest)
looker_studio/   tablero de BI exportado en PDF
docs/            flujo_mvp.drawio (archivo fuente del diagrama del flujo)

Se generan al correr el pipeline (no se versionan):
data/            bronze/ (archivos crudos), silver/ (Parquet), novabank.db (SQLite), reportes/
modelos/         nbo.joblib, abandono.joblib, fraude.joblib, metricas.json
```

## Capas de datos en SQLite

| Capa | Tablas | Contenido |
|---|---|---|
| Bronze (`bronze_*`) | 10 | Archivos crudos tal como llegan: CSV del core y del switch, JSONL de la app, JSON del buró, CSV del CRM y del call center |
| Silver (`silver_*`) | 11 | Limpio y tipado, PII seudonimizada (SHA-256), JSON aplanado, distancia al hogar y dispositivo desconocido |
| Gold (`gold_*`) | 9 | `features_cliente`, `pred_nbo`, `pred_abandono`, `alertas_fraude`, `siguiente_mejor_accion`, `registro_predicciones`, `metricas_modelos`, `kpi_diarios`, `acciones_enviadas` |

## Modelos y métricas (datos sintéticos)

| Modelo | Algoritmo | Métrica | Uso |
|---|---|---|---|
| Siguiente mejor oferta | HistGradientBoosting | AUC 0.76, precisión 0.43, recall 0.54 (umbral 0.25) | Servicio principal |
| Abandono a 60 días | Regresión logística | AUC 0.96 | Regla R8 (retención antes que venta) |
| Anomalías transaccionales | Isolation Forest | AUC 0.98, recall 0.77 | Regla R7 (seguridad primero) |

Las métricas exactas de cada corrida quedan en `modelos/metricas.json`. Al ser datos sintéticos, con datos reales serían menores.

## Limitaciones

- Datos sintéticos: las relaciones entre variables las define el generador, por eso las métricas son optimistas.
- SQLite sustituye a BigQuery y los archivos locales a Cloud Storage; el diseño completo del MP2 no se despliega.
- El envío de correo real requiere credenciales SMTP (variables `SMTP_*`); por defecto el correo se genera como archivo.
- No hay grupo de control: el piloto de 90 días con medición de uplift sigue siendo el siguiente paso.

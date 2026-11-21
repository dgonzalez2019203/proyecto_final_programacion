# Despliegue opcional en Google Cloud (Cloud Run + Cloud Scheduler)

Reproduce en la nube el flujo que la demo corre en local, usando el stack del Miniproyecto #2:
la API en **Cloud Run** y el disparador diario en **Cloud Scheduler**. Ambos tienen capa gratuita.
Si el despliegue no se concreta, el prototipo sigue completo con `automatizacion/flujo_local.py`.

Requisitos: cuenta de Google Cloud con facturación habilitada, `gcloud` instalado y un proyecto creado.

```bash
gcloud config set project TU_PROYECTO
gcloud services enable run.googleapis.com cloudbuild.googleapis.com cloudscheduler.googleapis.com
```

## 1. Desplegar la API

Desde la raíz del repositorio (el Dockerfile está en esta carpeta):

```bash
gcloud run deploy novabank-api --source . --region us-central1 --allow-unauthenticated --dockerfile automatizacion/cloud_run/Dockerfile
```

La salida muestra la URL del servicio, por ejemplo `https://novabank-api-xxxxx-uc.a.run.app`. Probar:

```bash
curl https://novabank-api-xxxxx-uc.a.run.app/recomendacion/NB-000696
```

## 2. Programar el disparador diario

Cloud Scheduler llama a `POST /ejecutar-pipeline` todos los días a las 02:00 (hora de Guatemala), igual que el batch nocturno del diseño:

```bash
gcloud scheduler jobs create http novabank-pipeline-diario --schedule "0 2 * * *" --time-zone "America/Guatemala" --uri "https://novabank-api-xxxxx-uc.a.run.app/ejecutar-pipeline" --http-method POST --location us-central1
```

Ejecutarlo de inmediato para la demo:

```bash
gcloud scheduler jobs run novabank-pipeline-diario --location us-central1
```

## 3. Apuntar el flujo local a la nube

```bash
NOVABANK_API=https://novabank-api-xxxxx-uc.a.run.app python automatizacion/flujo_local.py --ahora
```

## Limitaciones

- La base SQLite vive dentro del contenedor: cada despliegue la regenera y las acciones registradas no persisten entre instancias. En producción iría en BigQuery.
- El endpoint `/ejecutar-pipeline` está abierto para simplificar la demo; en producción se protege con IAM y un token de Cloud Scheduler.

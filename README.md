# Revisor de Mayordomo Mail

Interfaz en **Flask** (HTML + sesiones) para revisar correos pendientes, validar/corregir la clasificación automática y guardar el resultado en **Azure Cosmos DB (SQL API)**.

## Requisitos

- Python 3.9+
- Dependencias: `pip install -r requirements.txt`

## Configuración (Cosmos DB)

Rellena `config.py` / `.env` con:

- `COSMOS_ENDPOINT`
- `COSMOS_KEY`
- (Opcional) `COSMOS_DATABASE` y nombres de contenedores `COSMOS_CONTAINER_*`
- (Opcional) `FLASK_SECRET_KEY`

Alta de usuarios:

```bash
python scripts/manage_users.py add --username <user> --password <pass>
```

## Uso

```bash
python flask_app.py
```

## Persistencia

- Contenedor `entrada`: correos pendientes (con locks de revisión).
- Contenedor `resultados`: histórico de revisiones (incluye `status` y notas).
- Contenedor `descartes`: registros descartados.
- Contenedor `logs`: trazabilidad de clics.
- Contenedor `revisiones`: snapshots para listado/edición admin (incluye `history`).

## Docker

```bash
docker build -t mymail-review:latest .
docker run -p 8000:8000 mymail-review:latest
```

## Azure (ACR + App Service)

- Build y push:

```bash
az acr login --name <ACR_NAME>
docker build -t <ACR_NAME>.azurecr.io/mymail-review:latest .
docker push <ACR_NAME>.azurecr.io/mymail-review:latest
```

- App Service (contenedor): configura el puerto de la app en `8000` y define estas settings:
  - `PORT=8000`
  - `COSMOS_ENDPOINT=...`
  - `COSMOS_KEY=...`
  - `FLASK_SECRET_KEY=...`
  - (opcional) `APP_VERSION=...`


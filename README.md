# Revisor de Mayordomo Mail

Interfaz en **Flask** (HTML + sesiones) para revisar las filas del fichero `Validados_V3.xlsx` (hoja **1 dic - 8 dic**), validar/corregir la clasificación automática y guardar el resultado en tablas de Azure.

## Requisitos

- Python 3.9+
- Dependencias: `pip install -r requirements.txt`

## Configuración (Azure)

Rellena `config.py` con:

- `AZURE_STORAGE_CONNECTION_STRING`
- Nombres de tabla: `TABLE_USERS`, `TABLE_LOGS`, `TABLE_RESULTADOS`, `TABLE_DESCARTES`
- (Opcional) `FLASK_SECRET_KEY`

Alta de usuarios:

```bash
python scripts/manage_users.py add --username <user> --password <pass>
```

## Uso

1. Coloca `Validados_V3.xlsx` en la raíz del proyecto.
2. Ejecuta la app:

   ```bash
   python flask_app.py
   ```

3. El estado final por defecto es **Pendiente**; para poder **Guardar** o **Saltar**, cambia el estado.

## Carga de entrada (Excel)

Sube el Excel a blob `entrada` y carga sus filas en la tabla `entrada`:

```bash
python scripts/load_entrada.py --excel Validados_V3.xlsx --sheet "1 dic - 8 dic" --replace
```

## Salida

- Tabla `resultados`: histórico de revisiones (incluye `status` y notas).
- Tabla `descartes`: registros descartados.
- Tabla `logs`: trazabilidad de clics (login/logout/guardar/saltar) con usuario, fecha/día y resultado.
- `Validados_V3.xlsx`: se actualiza en cada guardado/descartado, eliminando la fila de la hoja.

## Docker

```bash
docker build -t mymail-review:latest .
docker run -p 8000:8000 mymail-review:latest
```

## Docker (dev con hot-reload)

```bash
docker compose -f docker-compose.dev.yml up --build
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
  - `AZURE_STORAGE_CONNECTION_STRING=...`
  - `FLASK_SECRET_KEY=...`
  - (opcional) `APP_VERSION=...`

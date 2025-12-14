# Revisor de Mayordomo Mail

Interfaz en Streamlit para revisar las filas del fichero `Validados_V3.xlsx` (hoja **1 dic - 8 dic**), validar o corregir la clasificación automática y guardar el resultado en `revisiones.csv`.

## Requisitos

- Python 3.9+
- Dependencias: `pip install -r requirements.txt`

## Uso

1. Coloca `Validados_V3.xlsx` en la raíz del proyecto (ya incluido en este repositorio).
2. Ejecuta el frontal:

   ```bash
   streamlit run app.py
   ```

3. Usa la barra lateral para cargar un logo y editar el título. La aplicación muestra una fila aleatoria pendiente, con campos de solo lectura para **Subject**, **Question** y **MailToAgent** en formato ampliado. El contador lateral indica cuántas filas quedan en la cola actual.
4. Completa el formulario de revisión y pulsa **Guardar**. La fila se añade a `revisiones.csv` junto al estado y notas de revisión, y se elimina del Excel original para que no vuelva a aparecer.
5. Si necesitas pasar al siguiente sin registrar nada, pulsa **Saltar sin guardar**.
6. Se carga automáticamente el siguiente registro hasta vaciar la hoja.

## Salida

- `revisiones.csv`: histórico de las revisiones con fecha UTC, estado final y comentarios.
- `Validados_V3.xlsx`: se actualiza en cada guardado, eliminando la fila revisada de la hoja **1 dic - 8 dic**.

## Depuración y ejecución desde VS Code

- Recomendado: ejecuta la app con Streamlit, no con `python app.py` (este último genera advertencias y `st.session_state` no funciona correctamente).
- Desde PowerShell (entorno virtual activo):

```powershell
streamlit run app.py
```

- Si prefieres usar el intérprete del venv directamente:

```powershell
& .\.venv\Scripts\python.exe -m streamlit run app.py
```

- Hay una configuración de lanzamiento en `.vscode/launch.json` llamada "Streamlit: Run (module)" que inicia Streamlit en el terminal integrado para depuración en VS Code.

Estructura front-end básica para el proyecto.

Estructura:

- public/: archivos estáticos que se sirven tal cual (HTML, imágenes, etc.)
- src/: código fuente (JS, componentes, CSS) pensado para bundlers o frameworks

Archivos incluidos:

- public/index.html — punto de entrada HTML.
- src/index.js — script de arranque (módulo ES).
- src/App.js — componente principal de ejemplo.
- src/styles.css — estilos básicos.
- package.json — metadatos y scripts básicos.

Cómo probar rápido:

1. Desde la carpeta `app`, instala un servidor estático ligero si no tienes uno (opcional):

```powershell
npm install -g http-server
```

2. Sirve la carpeta `public`:

```powershell
cd app
http-server public -p 3000
```

3. Abre http://localhost:3000

(Esto es una plantilla — para un proyecto real, añade un bundler como Vite/React/Create React App según prefieras.)

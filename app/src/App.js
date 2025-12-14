export function createApp(root) {
  const container = document.createElement('div');
  container.className = 'mymail-app';
  container.innerHTML = `
    <div class="header">
      <img src="/../../ey.png" alt="Logo" class="logo" onerror="this.style.display='none'" />
      <h2>Revisor de Mayordomo Mail - Front</h2>
    </div>
    <p>Esta es una plantilla de frontend. Adapta según tu stack (React/Vue/Svelte).</p>
  `;
  root.appendChild(container);
  return container;
}

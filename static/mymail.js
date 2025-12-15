(() => {
  const overlay = document.getElementById("mymail-overlay");
  const overlayText = document.getElementById("mymail-overlay-text");
  const show = (message) => {
    if (!overlay) return;
    if (overlayText && message) overlayText.textContent = message;
    overlay.style.display = "flex";
    overlay.setAttribute("aria-hidden", "false");
  };
  const hide = () => {
    if (!overlay) return;
    overlay.style.display = "none";
    overlay.setAttribute("aria-hidden", "true");
  };

  window.addEventListener("pageshow", hide);
  document.addEventListener("DOMContentLoaded", () => {
    hide();

    document.querySelectorAll("form[data-overlay]").forEach((form) => {
      form.addEventListener("submit", (e) => {
        const submitter = e.submitter;
        const message =
          submitter?.dataset?.overlayMessage ||
          form.dataset.overlayMessage ||
          "Procesando...";
        show(message);
      });
    });

    const statusSelect = document.getElementById("statusSelect");
    const btnSave = document.getElementById("btnSave");
    const btnSkip = document.getElementById("btnSkip");
    const reviewerNote = document.getElementById("reviewerNote");

    const syncButtons = () => {
      if (!statusSelect || !btnSave || !btnSkip) return;
      const disabled = (statusSelect.value || "Pendiente") === "Pendiente";
      btnSave.disabled = disabled;
      btnSkip.disabled = false;

      const status = statusSelect.value || "";
      const needsNote = status.startsWith("KO") || status === "DUDA";
      if (reviewerNote) reviewerNote.required = needsNote;
      if (needsNote && reviewerNote && !reviewerNote.value.trim()) btnSave.disabled = true;
    };

    statusSelect?.addEventListener("change", syncButtons);
    reviewerNote?.addEventListener("input", syncButtons);
    syncButtons();

    overlay?.addEventListener("click", hide);
  });
})();

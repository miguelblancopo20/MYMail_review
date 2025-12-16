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
    const pageStartMs = Date.now();

    document.querySelectorAll("form[data-overlay]").forEach((form) => {
      form.addEventListener("submit", (e) => {
        const elapsedSecondsInput = form.querySelector("#elapsedSeconds");
        if (elapsedSecondsInput) {
          const elapsed = Math.max(0, Math.round((Date.now() - pageStartMs) / 1000));
          elapsedSecondsInput.value = String(elapsed);
        }
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
    const koMymReasonWrap = document.getElementById("koMymReasonWrap");
    const koMymReason = document.getElementById("koMymReason");
    const reqNote = document.getElementById("reqNote");

    const syncButtons = () => {
      if (!statusSelect || !btnSave || !btnSkip) return;
      const disabled = (statusSelect.value || "Pendiente") === "Pendiente";
      btnSave.disabled = disabled;
      btnSkip.disabled = false;

      const status = statusSelect.value || "";
      const needsNote = status.startsWith("KO") || status === "DUDA";
      if (reviewerNote) reviewerNote.required = needsNote;
      if (needsNote && reviewerNote && !reviewerNote.value.trim()) btnSave.disabled = true;
      if (reqNote) reqNote.classList.toggle("hidden", !needsNote);

      const needsKoMymReason = status === "KO MYM";
      if (koMymReasonWrap) koMymReasonWrap.classList.toggle("hidden", !needsKoMymReason);
      if (koMymReason) koMymReason.required = needsKoMymReason;
      if (needsKoMymReason && koMymReason && !koMymReason.value) btnSave.disabled = true;
    };

    statusSelect?.addEventListener("change", syncButtons);
    reviewerNote?.addEventListener("input", syncButtons);
    koMymReason?.addEventListener("change", syncButtons);
    syncButtons();

    overlay?.addEventListener("click", hide);
  });
})();

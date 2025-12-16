(() => {
  const overlay = document.getElementById("mymail-overlay");
  const overlayText = document.getElementById("mymail-overlay-text");
  const modal = document.getElementById("mymail-modal");
  const modalText = document.getElementById("mymail-modal-text");
  const modalRefresh = document.getElementById("mymail-modal-refresh");
  const modalClose = document.getElementById("mymail-modal-close");
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

  const showModal = (message) => {
    if (!modal) return;
    if (modalText) modalText.textContent = message || "";
    modal.style.display = "flex";
    modal.setAttribute("aria-hidden", "false");
  };
  const hideModal = () => {
    if (!modal) return;
    modal.style.display = "none";
    modal.setAttribute("aria-hidden", "true");
  };

  window.addEventListener("pageshow", hide);
  document.addEventListener("DOMContentLoaded", () => {
    hide();
    const pageStartMs = Date.now();

    const pageErrorEl = document.getElementById("page-error");
    const pageError = pageErrorEl?.dataset?.error || "";
    const pageRefreshUrl = pageErrorEl?.dataset?.refreshUrl || "";

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
      const needsNote = status.startsWith("KO") || status === "DUDA" || status === "FDS";
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

    const heartbeatEl = document.getElementById("lock-heartbeat");
    const heartbeatUrl = heartbeatEl?.dataset?.heartbeatUrl || "";
    const refreshUrl = pageRefreshUrl || heartbeatEl?.dataset?.refreshUrl || window.location.href;
    const initialError = pageError || heartbeatEl?.dataset?.error || "";
    if (initialError) showModal(initialError);

    const timerEl = document.getElementById("lockTimer");
    let lockUntilMs = Number(timerEl?.dataset?.lockUntilMs || 0);
    const renderTimer = () => {
      if (!timerEl) return;
      if (!Number.isFinite(lockUntilMs) || lockUntilMs <= 0) {
        timerEl.style.display = "none";
        return;
      }
      const remaining = Math.max(0, lockUntilMs - Date.now());
      const totalSeconds = Math.floor(remaining / 1000);
      const mm = String(Math.floor(totalSeconds / 60)).padStart(2, "0");
      const ss = String(totalSeconds % 60).padStart(2, "0");
      timerEl.textContent = `${mm}:${ss}`;
      timerEl.style.display = "";
    };
    renderTimer();
    window.setInterval(renderTimer, 1000);

    modalRefresh?.addEventListener("click", () => {
      hideModal();
      window.location.href = refreshUrl;
    });
    modalClose?.addEventListener("click", hideModal);
    modal?.addEventListener("click", (e) => {
      if (e.target === modal) hideModal();
    });

    if (heartbeatUrl) {
      let lastSentMs = 0;
      let inFlight = false;

      const sendHeartbeat = () => {
        const now = Date.now();
        if (inFlight) return;
        if (now - lastSentMs < 60_000) return;
        lastSentMs = now;
        inFlight = true;
        fetch(heartbeatUrl, { method: "POST", headers: { "X-Requested-With": "fetch" } })
          .then((res) => {
            if (res.status === 409) {
              inFlight = false;
              lockUntilMs = 0;
              renderTimer();
              showModal("Sesion caducada (10 min) o registro ya procesado por otro usuario.");
              return null;
            }
            if (!res.ok) {
              inFlight = false;
              return null;
            }
            return res
              .json()
              .then((data) => {
                inFlight = false;
                const newUntil = Number(data?.lock_until_ms || 0);
                if (Number.isFinite(newUntil) && newUntil > 0) {
                  lockUntilMs = newUntil;
                  if (timerEl) timerEl.dataset.lockUntilMs = String(newUntil);
                  renderTimer();
                }
              })
              .catch(() => {
                inFlight = false;
              });
          })
          .catch(() => {
            inFlight = false;
          });
      };

      ["click", "keydown", "mousemove", "scroll", "touchstart"].forEach((evt) => {
        window.addEventListener(evt, sendHeartbeat, { passive: true });
      });
    }

    const btnAiTematica = document.getElementById("btnAiTematica");
    const aiTematicaResult = document.getElementById("aiTematicaResult");
    btnAiTematica?.addEventListener("click", () => {
      const url = btnAiTematica?.dataset?.aiUrl || "";
      if (!url) return;
      if (aiTematicaResult) aiTematicaResult.textContent = "";
      show("Consultando IA...");
      fetch(url, { method: "POST", headers: { "X-Requested-With": "fetch" } })
        .then((res) => res.json().then((j) => ({ status: res.status, json: j })))
        .then(({ status, json }) => {
          hide();
          if (!json?.ok) {
            const msg = json?.error || `Error (${status})`;
            showModal(msg);
            return;
          }
          if (aiTematicaResult) aiTematicaResult.textContent = String(json?.suggestion || "");
        })
        .catch((err) => {
          hide();
          showModal(String(err || "Error consultando IA."));
        });
    });
  });
})();

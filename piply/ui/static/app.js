function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

async function piplyRequest(url, options = {}) {
  const response = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  if (response.status === 401) {
    // The session expired, or authentication was switched on while this page
    // was open. Showing "Authentication required" in place would leave someone
    // clicking a form that can never succeed, so send them to sign in and
    // return here afterwards.
    const back = encodeURIComponent(window.location.pathname + window.location.search);
    window.location.href = `/login?next=${back}`;
    throw new Error("Your session has expired. Redirecting to sign in.");
  }

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || "Request failed");
  }

  return response.json().catch(() => ({}));
}

function formatDurationSeconds(totalSeconds) {
  if (totalSeconds === null || totalSeconds === undefined || Number.isNaN(Number(totalSeconds))) {
    return "-";
  }

  const rounded = Math.max(0, Math.floor(Number(totalSeconds)));
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const seconds = rounded % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

function collectCommandOverrides(scope = document) {
  const overrides = {};
  scope.querySelectorAll("[data-command-override]").forEach((field) => {
    const taskId = field.dataset.taskId;
    const value = field.value.trim();
    if (taskId && value) {
      overrides[taskId] = value;
    }
  });
  return overrides;
}

/*
 * Runtime inputs.
 *
 * A pipeline that normally receives its variables from an upstream trigger has
 * nothing to fill them in when it is started by hand. Rather than running a
 * command containing a literal `{practice}`, ask for the values first.
 */

// Resolver for the prompt currently on screen, so opening a second one (by
// clicking Run twice) settles the first as cancelled rather than leaving its
// caller waiting on a promise that can never resolve.
let pendingRuntimeInputs = null;

function closeRuntimeInputsModal() {
  const existing = document.getElementById("runtime-inputs-backdrop");
  if (existing) {
    existing.remove();
  }
  document.body.classList.remove("modal-open");
  if (pendingRuntimeInputs) {
    const settle = pendingRuntimeInputs;
    pendingRuntimeInputs = null;
    settle(null);
  }
}

/**
 * Show the runtime-input prompt and resolve with the collected values, or with
 * null if the user cancels.
 */
function promptForRuntimeInputs(details) {
  return new Promise((resolve) => {
    closeRuntimeInputsModal();
    pendingRuntimeInputs = resolve;

    const source = (details.triggered_by || []).length
      ? `<p class="runtime-inputs-note">These are normally supplied by
           <strong>${details.triggered_by.map(escapeHtml).join("</strong>, <strong>")}</strong>
           when it triggers this pipeline. A manual run has to provide them.</p>`
      : `<p class="runtime-inputs-note">This pipeline's configuration leaves these
           values to be supplied at run time.</p>`;

    const fields = details.required
      .map((item) => {
        const id = `runtime-input-${item.name}`;
        const used = (item.tasks || []).map(escapeHtml).join(", ");
        return `
          <label class="runtime-input-row" for="${escapeHtml(id)}">
            <span class="runtime-input-name">${escapeHtml(item.name)}</span>
            <input id="${escapeHtml(id)}" type="text" autocomplete="off" spellcheck="false"
                   data-runtime-input="${escapeHtml(item.name)}"
                   placeholder="value for {${escapeHtml(item.name)}}">
            <span class="runtime-input-usage">used by ${used}</span>
          </label>`;
      })
      .join("");

    const backdrop = document.createElement("div");
    backdrop.id = "runtime-inputs-backdrop";
    backdrop.className = "modal-backdrop";
    backdrop.innerHTML = `
      <div class="modal" role="dialog" aria-modal="true" aria-labelledby="runtime-inputs-title">
        <h2 id="runtime-inputs-title">Missing runtime values</h2>
        <p class="runtime-inputs-subtitle">${escapeHtml(details.pipeline_title || details.pipeline_id)}</p>
        ${source}
        <form id="runtime-inputs-form" class="runtime-inputs-form">
          ${fields}
          <p class="runtime-inputs-error" id="runtime-inputs-error" hidden></p>
          <div class="modal-actions">
            <button type="button" class="button secondary" id="runtime-inputs-cancel">Cancel</button>
            <button type="submit" class="button primary">Run Pipeline</button>
          </div>
        </form>
      </div>`;

    document.body.appendChild(backdrop);
    document.body.classList.add("modal-open");

    const form = backdrop.querySelector("#runtime-inputs-form");
    const errorLine = backdrop.querySelector("#runtime-inputs-error");
    const firstField = backdrop.querySelector("[data-runtime-input]");
    if (firstField) {
      firstField.focus();
    }

    const finish = (value) => {
      document.removeEventListener("keydown", onKeydown);
      // Claim the resolver first so closing does not settle it as a cancel.
      pendingRuntimeInputs = null;
      closeRuntimeInputsModal();
      resolve(value);
    };

    function onKeydown(event) {
      if (event.key === "Escape") {
        finish(null);
      }
    }
    document.addEventListener("keydown", onKeydown);

    backdrop.querySelector("#runtime-inputs-cancel").addEventListener("click", () => finish(null));
    backdrop.addEventListener("click", (event) => {
      if (event.target === backdrop) {
        finish(null);
      }
    });

    form.addEventListener("submit", (event) => {
      event.preventDefault();
      const values = {};
      const missing = [];
      form.querySelectorAll("[data-runtime-input]").forEach((field) => {
        const value = field.value.trim();
        // Every prompted value is required: substituting an empty string would
        // produce a different broken command rather than an obvious one.
        if (!value) {
          missing.push(field.dataset.runtimeInput);
          field.classList.add("invalid");
        } else {
          field.classList.remove("invalid");
          values[field.dataset.runtimeInput] = value;
        }
      });

      if (missing.length) {
        errorLine.textContent = `Provide a value for: ${missing.join(", ")}.`;
        errorLine.hidden = false;
        const firstInvalid = form.querySelector(".invalid");
        if (firstInvalid) {
          firstInvalid.focus();
        }
        return;
      }
      finish(values);
    });
  });
}

/**
 * Return the runtime values needed to start this pipeline.
 *
 * Resolves to {} when nothing is missing, or null when the user cancelled. A
 * failure to check is not fatal: the run proceeds as it did before this
 * existed, so a problem here never blocks starting a pipeline.
 */
async function collectRuntimeInputs(pipelineId, taskId = null) {
  let details;
  try {
    const query = taskId ? `?task_id=${encodeURIComponent(taskId)}` : "";
    details = await piplyRequest(`/api/pipelines/${pipelineId}/runtime-inputs${query}`);
  } catch (error) {
    return {};
  }
  if (details.ready || !(details.required || []).length) {
    return {};
  }
  return promptForRuntimeInputs(details);
}

async function triggerPipeline(pipelineId, options = {}) {
  const button = document.querySelector(`[data-run-button="${pipelineId}"]`);
  const resetButton = () => {
    if (button) {
      button.disabled = false;
      button.textContent = button.dataset.originalLabel || "Run now";
    }
  };

  const variables = options.variables || (await collectRuntimeInputs(pipelineId));
  if (variables === null) {
    return; // cancelled
  }

  if (button) {
    button.disabled = true;
    button.dataset.originalLabel = button.textContent;
    button.textContent = "Starting...";
  }

  try {
    const payload = {
      command_overrides: options.commandOverrides || collectCommandOverrides(options.scope || document),
      variables,
    };
    const run = await piplyRequest(`/api/pipelines/${pipelineId}/run`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    window.location.href = `/runs/${run.id}`;
  } catch (error) {
    alert(error.message);
    resetButton();
  }
}

async function triggerTask(pipelineId, taskId, options = {}) {
  const variables = options.variables || (await collectRuntimeInputs(pipelineId, taskId));
  if (variables === null) {
    return; // cancelled
  }

  const payload = {
    command_overrides: options.commandOverrides || collectCommandOverrides(options.scope || document),
    variables,
  };
  try {
    const run = await piplyRequest(`/api/pipelines/${pipelineId}/tasks/${taskId}/run`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    window.location.href = `/runs/${run.id}`;
  } catch (error) {
    alert(error.message);
  }
}

async function retryRun(runId, mode, taskId = null) {
  const body = JSON.stringify({ mode, task_id: taskId });
  try {
    const run = await piplyRequest(`/api/runs/${runId}/retry`, {
      method: "POST",
      body,
    });
    window.location.href = `/runs/${run.id}`;
  } catch (error) {
    alert(error.message);
  }
}

async function togglePipelinePause(pipelineId, paused) {
  try {
    await piplyRequest(`/api/pipelines/${pipelineId}/${paused ? "pause" : "resume"}`, {
      method: "POST",
      body: "{}",
    });
    window.location.reload();
  } catch (error) {
    alert(error.message);
  }
}

async function cancelRun(runId) {
  const shouldCancel = window.confirm("Cancel this run?");
  if (!shouldCancel) {
    return;
  }
  try {
    await piplyRequest(`/api/runs/${runId}/cancel`, {
      method: "POST",
      body: "{}",
    });
    window.location.reload();
  } catch (error) {
    alert(error.message);
  }
}

async function deleteRun(runId, redirectUrl = "/runs") {
  const shouldDelete = window.confirm("Delete this run from history?");
  if (!shouldDelete) {
    return;
  }
  try {
    await piplyRequest(`/api/runs/${runId}`, {
      method: "DELETE",
    });
    window.location.href = redirectUrl;
  } catch (error) {
    alert(error.message);
  }
}

async function deletePipeline(pipelineId, redirectUrl = "/pipelines") {
  const shouldDelete = window.confirm("Delete this pipeline and its stored run history?");
  if (!shouldDelete) {
    return;
  }
  try {
    await piplyRequest(`/api/pipelines/${pipelineId}`, {
      method: "DELETE",
    });
    window.location.href = redirectUrl;
  } catch (error) {
    alert(error.message);
  }
}

function copyText(value) {
  navigator.clipboard.writeText(value).catch(() => {});
}

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

async function triggerPipeline(pipelineId, options = {}) {
  const button = document.querySelector(`[data-run-button="${pipelineId}"]`);
  if (button) {
    button.disabled = true;
    button.dataset.originalLabel = button.textContent;
    button.textContent = "Starting...";
  }

  try {
    const payload = {
      command_overrides: options.commandOverrides || collectCommandOverrides(options.scope || document),
    };
    const run = await piplyRequest(`/api/pipelines/${pipelineId}/run`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
    window.location.href = `/runs/${run.id}`;
  } catch (error) {
    alert(error.message);
    if (button) {
      button.disabled = false;
      button.textContent = button.dataset.originalLabel || "Run now";
    }
  }
}

async function triggerTask(pipelineId, taskId, options = {}) {
  const payload = {
    command_overrides: options.commandOverrides || collectCommandOverrides(options.scope || document),
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

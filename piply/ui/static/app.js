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

async function triggerPipeline(pipelineId) {
  const button = document.querySelector(`[data-run-button="${pipelineId}"]`);
  if (button) {
    button.disabled = true;
    button.dataset.originalLabel = button.textContent;
    button.textContent = "Starting...";
  }

  try {
    const run = await piplyRequest(`/api/pipelines/${pipelineId}/run`, {
      method: "POST",
      body: "{}",
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

function copyText(value) {
  navigator.clipboard.writeText(value).catch(() => {});
}

function dagPalette(status) {
  const palette = {
    success: { fill: "rgba(27, 156, 96, 0.12)", stroke: "rgba(27, 156, 96, 0.72)" },
    running: { fill: "rgba(57, 117, 255, 0.12)", stroke: "rgba(57, 117, 255, 0.72)" },
    failed: { fill: "rgba(217, 90, 90, 0.12)", stroke: "rgba(217, 90, 90, 0.72)" },
    skipped: { fill: "rgba(123, 140, 166, 0.12)", stroke: "rgba(123, 140, 166, 0.6)" },
    queued: { fill: "rgba(232, 155, 44, 0.1)", stroke: "rgba(232, 155, 44, 0.64)" },
  };
  return palette[status] || palette.queued;
}

function renderDagInto(target, tasks, taskStateMap = {}, options = {}) {
  const container = typeof target === "string" ? document.getElementById(target) : target;
  if (!container) {
    return;
  }

  if (!window.dagre || !window.dagre.graphlib) {
    container.innerHTML = '<div class="empty-state">Graph library not available. The task cards below still show the full flow.</div>';
    return;
  }

  if (!tasks.length) {
    container.innerHTML = '<div class="empty-state">No tasks available to draw yet.</div>';
    return;
  }

  const clickableStatuses = options.clickableStatuses || [];
  const graph = new window.dagre.graphlib.Graph();
  graph.setGraph({ rankdir: "LR", nodesep: 30, ranksep: 80, marginx: 18, marginy: 18 });
  graph.setDefaultEdgeLabel(() => ({}));

  tasks.forEach((task) => {
    graph.setNode(task.task_id, {
      ...task,
      status: taskStateMap[task.task_id] || task.status || "queued",
      width: 220,
      height: 76,
    });
  });

  tasks.forEach((task) => {
    (task.depends_on || []).forEach((dependency) => graph.setEdge(dependency, task.task_id));
  });

  window.dagre.layout(graph);

  const width = Math.max(...graph.nodes().map((nodeId) => graph.node(nodeId).x + 160), 420);
  const height = Math.max(...graph.nodes().map((nodeId) => graph.node(nodeId).y + 90), 220);
  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.style.width = "100%";
  svg.style.height = `${height}px`;

  const defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
  const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
  marker.setAttribute("id", `${container.id || "dag"}-arrow`);
  marker.setAttribute("markerWidth", "10");
  marker.setAttribute("markerHeight", "10");
  marker.setAttribute("refX", "8");
  marker.setAttribute("refY", "3");
  marker.setAttribute("orient", "auto");
  const arrow = document.createElementNS("http://www.w3.org/2000/svg", "path");
  arrow.setAttribute("d", "M0,0 L0,6 L9,3 z");
  arrow.setAttribute("fill", "#b4c6d8");
  marker.appendChild(arrow);
  defs.appendChild(marker);
  svg.appendChild(defs);

  graph.edges().forEach((edge) => {
    const edgePath = graph.edge(edge);
    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    const points = edgePath.points.map((point) => `${point.x},${point.y}`).join(" ");
    path.setAttribute("d", `M ${points}`);
    path.setAttribute("class", "dag-edge");
    path.setAttribute("marker-end", `url(#${container.id || "dag"}-arrow)`);
    svg.appendChild(path);
  });

  graph.nodes().forEach((nodeId) => {
    const node = graph.node(nodeId);
    const palette = dagPalette(node.status);
    const clickable = clickableStatuses.includes(node.status);
    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.setAttribute("transform", `translate(${node.x - node.width / 2}, ${node.y - node.height / 2})`);

    const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    rect.setAttribute("width", node.width);
    rect.setAttribute("height", node.height);
    rect.setAttribute("rx", "14");
    rect.setAttribute("class", "dag-node");
    rect.setAttribute("fill", palette.fill);
    rect.setAttribute("stroke", palette.stroke);
    rect.setAttribute("stroke-width", "2");
    group.appendChild(rect);

    const title = document.createElementNS("http://www.w3.org/2000/svg", "text");
    title.setAttribute("x", "16");
    title.setAttribute("y", "28");
    title.setAttribute("class", "dag-label");
    title.textContent = node.title;
    group.appendChild(title);

    const subtitle = document.createElementNS("http://www.w3.org/2000/svg", "text");
    subtitle.setAttribute("x", "16");
    subtitle.setAttribute("y", "48");
    subtitle.setAttribute("class", "dag-type");
    subtitle.textContent = `${node.task_type} | ${node.task_id}`;
    group.appendChild(subtitle);

    const state = document.createElementNS("http://www.w3.org/2000/svg", "text");
    state.setAttribute("x", "16");
    state.setAttribute("y", "64");
    state.setAttribute("class", "dag-type");
    state.textContent = node.status;
    group.appendChild(state);

    if (clickable && typeof options.onNodeClick === "function") {
      group.style.cursor = "pointer";
      group.addEventListener("click", () => options.onNodeClick(node));
    }

    svg.appendChild(group);
  });

  container.innerHTML = "";
  container.appendChild(svg);
}

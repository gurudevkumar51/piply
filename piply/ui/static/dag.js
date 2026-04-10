(function () {
  const LAYOUTS = [
    { id: "flow", label: "Flow view", rankdir: "LR" },
    { id: "stage", label: "Stage view", rankdir: "TB" },
    { id: "focus", label: "Focus view", rankdir: "LR" },
  ];

  function dagPalette(status) {
    const palette = {
      success: { fill: "rgba(27, 156, 96, 0.12)", stroke: "rgba(27, 156, 96, 0.74)", dot: "#1b9c60" },
      running: { fill: "rgba(57, 117, 255, 0.14)", stroke: "rgba(57, 117, 255, 0.78)", dot: "#3975ff" },
      failed: { fill: "rgba(217, 90, 90, 0.13)", stroke: "rgba(217, 90, 90, 0.76)", dot: "#d95a5a" },
      cancelled: { fill: "rgba(106, 94, 156, 0.12)", stroke: "rgba(106, 94, 156, 0.74)", dot: "#6a5e9c" },
      skipped: { fill: "rgba(123, 140, 166, 0.11)", stroke: "rgba(123, 140, 166, 0.64)", dot: "#7b8ca6" },
      queued: { fill: "rgba(232, 155, 44, 0.12)", stroke: "rgba(232, 155, 44, 0.7)", dot: "#e89b2c" },
    };
    return palette[status] || palette.queued;
  }

  function durationLabel(durationSeconds) {
    if (durationSeconds === null || durationSeconds === undefined || Number.isNaN(Number(durationSeconds))) {
      return null;
    }
    if (typeof window.formatDurationSeconds === "function") {
      return window.formatDurationSeconds(durationSeconds);
    }
    return `${Math.max(0, Math.floor(Number(durationSeconds)))}s`;
  }

  function midpoint(points) {
    if (!points.length) {
      return { x: 0, y: 0 };
    }
    return points[Math.floor(points.length / 2)];
  }

  function currentLayout(state) {
    return LAYOUTS[state.layoutIndex % LAYOUTS.length];
  }

  function nextLayout(state) {
    state.layoutIndex = (state.layoutIndex + 1) % LAYOUTS.length;
    return currentLayout(state);
  }

  function computeDepths(tasks) {
    const taskMap = Object.fromEntries(tasks.map((task) => [task.task_id, task]));
    const cache = {};

    function visit(taskId) {
      if (cache[taskId] !== undefined) {
        return cache[taskId];
      }
      const task = taskMap[taskId];
      if (!task || !(task.depends_on || []).length) {
        cache[taskId] = 0;
        return 0;
      }
      const depth = Math.max(...task.depends_on.map((dependency) => visit(dependency))) + 1;
      cache[taskId] = depth;
      return depth;
    }

    tasks.forEach((task) => visit(task.task_id));
    return cache;
  }

  function lineageFor(tasks, selectedNodeId) {
    const upstream = new Set();
    const downstream = new Set();
    const taskMap = Object.fromEntries(tasks.map((task) => [task.task_id, task]));
    const dependents = {};
    tasks.forEach((task) => {
      (task.depends_on || []).forEach((dependency) => {
        dependents[dependency] = dependents[dependency] || [];
        dependents[dependency].push(task.task_id);
      });
    });

    function walkUp(taskId) {
      upstream.add(taskId);
      (taskMap[taskId]?.depends_on || []).forEach((dependency) => {
        if (!upstream.has(dependency)) {
          walkUp(dependency);
        }
      });
    }

    function walkDown(taskId) {
      downstream.add(taskId);
      (dependents[taskId] || []).forEach((dependency) => {
        if (!downstream.has(dependency)) {
          walkDown(dependency);
        }
      });
    }

    if (selectedNodeId && taskMap[selectedNodeId]) {
      walkUp(selectedNodeId);
      walkDown(selectedNodeId);
    }
    return new Set([...upstream, ...downstream]);
  }

  function ensureControls(shell, state, redraw) {
    let toolbar = shell.querySelector("[data-dag-toolbar]");
    if (toolbar) {
      const layoutButton = toolbar.querySelector("[data-dag-layout]");
      if (layoutButton) {
        layoutButton.textContent = currentLayout(state).label;
      }
      return;
    }

    toolbar = document.createElement("div");
    toolbar.className = "dag-toolbar";
    toolbar.setAttribute("data-dag-toolbar", "true");
    toolbar.innerHTML = `
      <div class="segmented">
        <span class="small-kicker dag-toolbar-label">Graph controls</span>
        <button type="button" class="toolbar-button" data-dag-zoom-in>+</button>
        <button type="button" class="toolbar-button" data-dag-zoom-out>-</button>
        <button type="button" class="toolbar-button" data-dag-reset>Center</button>
      </div>
      <div class="segmented">
        <button type="button" class="toolbar-button" data-dag-layout>${currentLayout(state).label}</button>
        <button type="button" class="toolbar-button" data-dag-clear hidden>Clear focus</button>
      </div>
    `;
    shell.insertBefore(toolbar, shell.firstChild);

    toolbar.querySelector("[data-dag-zoom-in]").addEventListener("click", () => {
      state.scale = Math.min(2.5, Number((state.scale + 0.15).toFixed(2)));
      redraw();
    });
    toolbar.querySelector("[data-dag-zoom-out]").addEventListener("click", () => {
      state.scale = Math.max(0.55, Number((state.scale - 0.15).toFixed(2)));
      redraw();
    });
    toolbar.querySelector("[data-dag-reset]").addEventListener("click", () => {
      state.scale = 1;
      state.panX = 0;
      state.panY = 0;
      redraw();
    });
    toolbar.querySelector("[data-dag-layout]").addEventListener("click", (event) => {
      event.currentTarget.textContent = nextLayout(state).label;
      redraw();
    });
    toolbar.querySelector("[data-dag-clear]").addEventListener("click", () => {
      state.selectedNodeId = null;
      redraw();
    });
  }

  function bindPanAndZoom(viewport, state, redraw) {
    if (viewport.dataset.dagBound === "true") {
      return;
    }
    viewport.dataset.dagBound = "true";

    let dragging = false;
    let lastX = 0;
    let lastY = 0;

    viewport.addEventListener("wheel", (event) => {
      event.preventDefault();
      const delta = event.deltaY < 0 ? 0.08 : -0.08;
      state.scale = Math.max(0.55, Math.min(2.5, Number((state.scale + delta).toFixed(2))));
      redraw();
    }, { passive: false });

    viewport.addEventListener("mousedown", (event) => {
      if (event.target.closest(".dag-node-group")) {
        return;
      }
      dragging = true;
      lastX = event.clientX;
      lastY = event.clientY;
      viewport.classList.add("dragging");
    });

    viewport.addEventListener("dblclick", () => {
      state.selectedNodeId = null;
      redraw();
    });

    window.addEventListener("mousemove", (event) => {
      if (!dragging) {
        return;
      }
      state.panX += event.clientX - lastX;
      state.panY += event.clientY - lastY;
      lastX = event.clientX;
      lastY = event.clientY;
      redraw();
    });

    window.addEventListener("mouseup", () => {
      dragging = false;
      viewport.classList.remove("dragging");
    });
  }

  function renderStageBands(rootGroup, graph, tasks, width) {
    const depths = computeDepths(tasks);
    const groupedNodes = {};
    graph.nodes().forEach((nodeId) => {
      const depth = depths[nodeId] || 0;
      groupedNodes[depth] = groupedNodes[depth] || [];
      groupedNodes[depth].push(graph.node(nodeId));
    });

    Object.entries(groupedNodes).forEach(([depthText, nodes]) => {
      const depth = Number(depthText);
      const top = Math.min(...nodes.map((node) => node.y - node.height / 2)) - 18;
      const bottom = Math.max(...nodes.map((node) => node.y + node.height / 2)) + 18;
      const band = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      band.setAttribute("x", "4");
      band.setAttribute("y", String(top));
      band.setAttribute("width", String(width - 8));
      band.setAttribute("height", String(Math.max(76, bottom - top)));
      band.setAttribute("rx", "20");
      band.setAttribute("class", "dag-stage-band");
      rootGroup.appendChild(band);

      const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
      label.setAttribute("x", "22");
      label.setAttribute("y", String(top + 24));
      label.setAttribute("class", "dag-stage-label");
      label.textContent = depth === 0 ? "Stage 1 · entry" : `Stage ${depth + 1}`;
      rootGroup.appendChild(label);
    });
  }

  function renderInto(target, tasks, taskStateMap = {}, options = {}) {
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

    const shell = container.closest(".dag-shell") || container.parentElement || container;
    const state = container._dagState || {
      scale: 1,
      panX: 0,
      panY: 0,
      layoutIndex: Math.max(0, LAYOUTS.findIndex((item) => item.id === (options.defaultLayout || "flow"))),
      selectedNodeId: options.selectedNodeId || null,
    };
    if (options.selectedNodeId !== undefined) {
      state.selectedNodeId = options.selectedNodeId;
    }
    container._dagState = state;

    const redraw = () => renderInto(container, tasks, taskStateMap, options);
    ensureControls(shell, state, redraw);
    const clearButton = shell.querySelector("[data-dag-clear]");
    if (clearButton) {
      clearButton.hidden = !state.selectedNodeId;
    }

    const layout = currentLayout(state);
    const clickableStatuses = options.clickableStatuses || [];
    const lineage = layout.id === "focus" ? lineageFor(tasks, state.selectedNodeId) : null;
    const graph = new window.dagre.graphlib.Graph();
    graph.setGraph({
      rankdir: layout.rankdir,
      nodesep: layout.id === "stage" ? 28 : 34,
      ranksep: layout.id === "stage" ? 96 : 92,
      marginx: 22,
      marginy: 22,
    });
    graph.setDefaultEdgeLabel(() => ({}));

    tasks.forEach((task) => {
      graph.setNode(task.task_id, {
        ...task,
        status: taskStateMap[task.task_id] || task.status || "queued",
        width: 250,
        height: 116,
      });
    });

    tasks.forEach((task) => {
      (task.depends_on || []).forEach((dependency) => {
        graph.setEdge(dependency, task.task_id, { label: `after ${dependency}` });
      });
    });

    window.dagre.layout(graph);

    const nodeMetrics = graph.nodes().map((nodeId) => graph.node(nodeId));
    const width = Math.max(...nodeMetrics.map((node) => node.x + 190), 600);
    const height = Math.max(...nodeMetrics.map((node) => node.y + 130), 320);

    const viewport = document.createElement("div");
    viewport.className = `dag-viewport ${layout.id === "focus" ? "focus-mode" : ""}`;
    bindPanAndZoom(viewport, state, redraw);

    const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("class", "dag-svg");

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
    arrow.setAttribute("fill", "#aabdd1");
    marker.appendChild(arrow);
    defs.appendChild(marker);
    svg.appendChild(defs);

    const rootGroup = document.createElementNS("http://www.w3.org/2000/svg", "g");
    rootGroup.setAttribute("transform", `translate(${state.panX} ${state.panY}) scale(${state.scale})`);
    svg.appendChild(rootGroup);

    if (layout.id === "stage") {
      renderStageBands(rootGroup, graph, tasks, width);
    }

    graph.edges().forEach((edge) => {
      const edgePath = graph.edge(edge);
      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      const points = edgePath.points.map((point) => `${point.x},${point.y}`).join(" ");
      path.setAttribute("d", `M ${points}`);
      const isDimmed = lineage && !(lineage.has(edge.v) && lineage.has(edge.w));
      path.setAttribute("class", `dag-edge${isDimmed ? " dimmed" : ""}`);
      path.setAttribute("marker-end", `url(#${container.id || "dag"}-arrow)`);
      rootGroup.appendChild(path);

      if (options.showEdgeLabels !== false) {
        const labelPoint = midpoint(edgePath.points);
        const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
        label.setAttribute("x", String(labelPoint.x));
        label.setAttribute("y", String(labelPoint.y - 8));
        label.setAttribute("text-anchor", "middle");
        label.setAttribute("class", `dag-edge-label${isDimmed ? " dimmed" : ""}`);
        label.textContent = edge.v;
        rootGroup.appendChild(label);
      }
    });

    graph.nodes().forEach((nodeId) => {
      const node = graph.node(nodeId);
      const palette = dagPalette(node.status);
      const isSelected = state.selectedNodeId === node.task_id;
      const isDimmed = lineage && !lineage.has(node.task_id);
      const clickable = clickableStatuses.includes(node.status) || typeof options.onNodeClick === "function";
      const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
      group.setAttribute(
        "transform",
        `translate(${node.x - node.width / 2}, ${node.y - node.height / 2})`,
      );
      group.setAttribute("class", `dag-node-group ${node.status}${isSelected ? " selected" : ""}${isDimmed ? " dimmed" : ""}`);

      const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      rect.setAttribute("width", node.width);
      rect.setAttribute("height", node.height);
      rect.setAttribute("rx", "18");
      rect.setAttribute("class", `dag-node dag-node-${node.status}${isSelected ? " selected" : ""}`);
      rect.setAttribute("fill", palette.fill);
      rect.setAttribute("stroke", palette.stroke);
      rect.setAttribute("stroke-width", isSelected ? "3" : "2");
      group.appendChild(rect);

      const dot = document.createElementNS("http://www.w3.org/2000/svg", "circle");
      dot.setAttribute("cx", String(node.width - 20));
      dot.setAttribute("cy", "18");
      dot.setAttribute("r", "6");
      dot.setAttribute("fill", palette.dot);
      dot.setAttribute("class", `dag-status-dot ${node.status}`);
      group.appendChild(dot);

      const title = document.createElementNS("http://www.w3.org/2000/svg", "text");
      title.setAttribute("x", "16");
      title.setAttribute("y", "28");
      title.setAttribute("class", "dag-label");
      title.textContent = node.title;
      group.appendChild(title);

      const subtitle = document.createElementNS("http://www.w3.org/2000/svg", "text");
      subtitle.setAttribute("x", "16");
      subtitle.setAttribute("y", "49");
      subtitle.setAttribute("class", "dag-type");
      subtitle.textContent = `${node.task_type} | ${node.task_id}`;
      group.appendChild(subtitle);

      const stateLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
      stateLabel.setAttribute("x", "16");
      stateLabel.setAttribute("y", "70");
      stateLabel.setAttribute("class", "dag-state-label");
      stateLabel.textContent = node.status;
      group.appendChild(stateLabel);

      const durationText = durationLabel(node.duration_seconds);
      if (durationText) {
        const durationNode = document.createElementNS("http://www.w3.org/2000/svg", "text");
        durationNode.setAttribute("x", "16");
        durationNode.setAttribute("y", "90");
        durationNode.setAttribute("class", "dag-duration");
        durationNode.textContent = `duration ${durationText}`;
        group.appendChild(durationNode);
      }

      const hintNode = document.createElementNS("http://www.w3.org/2000/svg", "text");
      hintNode.setAttribute("x", "16");
      hintNode.setAttribute("y", "106");
      hintNode.setAttribute("class", "dag-hint");
      hintNode.textContent = isSelected ? "selected" : "click for actions";
      group.appendChild(hintNode);

      if (clickable && typeof options.onNodeClick === "function") {
        group.style.cursor = "pointer";
        group.addEventListener("click", () => {
          state.selectedNodeId = node.task_id;
          options.onNodeClick(node, { selectedNodeId: state.selectedNodeId });
          redraw();
        });
      }

      rootGroup.appendChild(group);
    });

    viewport.innerHTML = "";
    viewport.appendChild(svg);
    container.innerHTML = "";
    container.appendChild(viewport);
  }

  window.PiplyDag = { renderInto };
  window.renderDagInto = renderInto;
})();

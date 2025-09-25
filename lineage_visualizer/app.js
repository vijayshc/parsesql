(function () {
  const REQUIRED_COLUMNS = ["source_table", "source_column", "expression", "target_column", "target_table", "file"];
  const THEME_STORAGE_KEY = "lineage-theme";

  const state = {
    nodes: new Map(),
    edges: [],
    edgeMap: new Map(),
    groups: [],
    nodeToGroup: new Map(),
    adjacency: {
      upstream: new Map(),
      downstream: new Map()
    },
    columnCount: 0,
    warnings: []
  };

  const refs = {
    fileInput: document.getElementById("fileInput"),
    loadSample: document.getElementById("loadSample"),
    resetButton: document.getElementById("resetButton"),
    downloadButton: document.getElementById("downloadButton"),
    themeToggle: document.getElementById("themeToggle"),
    nodeCount: document.getElementById("nodeCount"),
    edgeCount: document.getElementById("edgeCount"),
    fileName: document.getElementById("fileName"),
    searchInput: document.getElementById("searchInput"),
    message: document.getElementById("message"),
    inspector: document.getElementById("inspector"),
    inspectorEmpty: document.getElementById("inspectorEmpty"),
    infoColumn: document.getElementById("infoColumn"),
    infoTable: document.getElementById("infoTable"),
    infoFile: document.getElementById("infoFile"),
    fileView: document.getElementById("fileView"),
    expressionView: document.getElementById("expressionView"),
    expressionContent: document.getElementById("expressionContent"),
    graphWrapper: document.getElementById("graphWrapper"),
    cardsContainer: document.getElementById("cardsContainer"),
    connectionsLayer: document.getElementById("connectionsLayer"),
    emptyState: document.getElementById("emptyState")
  };

  let currentTheme = document.documentElement.getAttribute("data-theme") || "dark";
  let currentFileName = "";
  const resizeHandler = debounce(() => drawConnections(), 120);
  const scrollHandler = debounce(() => drawConnections(), 60);

  document.addEventListener("DOMContentLoaded", init);

  function init() {
    attachListeners();
    initializeTheme();
    window.addEventListener("resize", resizeHandler);
    refs.graphWrapper.addEventListener("scroll", scrollHandler, { passive: true });
  }

  function attachListeners() {
    refs.fileInput.addEventListener("change", (event) => {
      const [file] = event.target.files;
      if (file) {
        parseCsvFile(file);
      }
    });

    refs.loadSample.addEventListener("click", () => loadSampleCsv());
    refs.resetButton.addEventListener("click", () => resetView());
    refs.downloadButton.addEventListener("click", () => downloadImage());
    refs.searchInput.addEventListener("input", () => handleSearch(refs.searchInput.value.trim().toLowerCase()));
    refs.themeToggle?.addEventListener("click", () => toggleTheme());

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        refs.searchInput.value = "";
        handleSearch("");
      }
    });
  }

  function initializeTheme() {
    let theme = currentTheme;
    try {
      const stored = localStorage.getItem(THEME_STORAGE_KEY);
      if (stored === "light" || stored === "dark") {
        theme = stored;
      }
    } catch (error) {
      theme = currentTheme;
    }
    applyTheme(theme, false);
  }

  function toggleTheme() {
    const next = currentTheme === "light" ? "dark" : "light";
    applyTheme(next);
  }

  function applyTheme(theme, persist = true) {
    const normalized = theme === "light" ? "light" : "dark";
    currentTheme = normalized;
    document.documentElement.setAttribute("data-theme", normalized);
    document.documentElement.style.colorScheme = normalized === "light" ? "light" : "dark";
    if (persist) {
      try {
        localStorage.setItem(THEME_STORAGE_KEY, normalized);
      } catch (error) {
        /* no-op */
      }
    }
    updateThemeToggle();
  }

  function updateThemeToggle() {
    if (!refs.themeToggle) return;
    const isLight = currentTheme === "light";
    refs.themeToggle.textContent = isLight ? "Dark theme" : "Light theme";
    refs.themeToggle.setAttribute("aria-pressed", isLight ? "true" : "false");
    const label = isLight ? "Switch to dark theme" : "Switch to light theme";
    refs.themeToggle.setAttribute("aria-label", label);
    refs.themeToggle.title = label;
  }

  function parseCsvFile(file) {
    showMessage("Loading lineage data…", "info");
    Papa.parse(file, {
      header: true,
      skipEmptyLines: true,
      transformHeader: (header) => header.trim().toLowerCase(),
      complete: (results) => {
        if (results.errors && results.errors.length) {
          const message = results.errors.slice(0, 3).map((e) => `${e.message} (row ${e.row ?? "?"})`).join("; ");
          showMessage(`CSV parsing errors detected: ${message}`, "error");
          return;
        }
        handleParsedData(results.data, file.name);
      }
    });
  }

  function loadSampleCsv() {
    showMessage("Loading sample lineage…", "info");
    fetch("../output.csv")
      .then((response) => {
        if (!response.ok) throw new Error(`Unable to load sample CSV (status ${response.status})`);
        return response.text();
      })
      .then((text) => {
        const results = Papa.parse(text, {
          header: true,
          skipEmptyLines: true,
          transformHeader: (header) => header.trim().toLowerCase()
        });
        handleParsedData(results.data, "output.csv");
        showMessage("Sample lineage loaded.", "success");
      })
      .catch((error) => {
        console.error(error);
        showMessage("Sample could not be loaded. Serve the project via a local web server or upload a CSV.", "error");
      });
  }

  function handleParsedData(rows, filename) {
    if (!rows || rows.length === 0) {
      showMessage("The CSV is empty. Ensure the parser produced lineage rows.", "warning");
      return;
    }

    const missing = REQUIRED_COLUMNS.filter((key) => !(key in rows[0]));
    if (missing.length) {
      showMessage(`CSV missing required columns: ${missing.join(", ")}`, "error");
      return;
    }

    const model = transformRecords(rows);
    if (model.nodes.size === 0) {
      showMessage("No lineage nodes could be derived from this CSV.", "warning");
      resetData();
      return;
    }

    state.nodes = model.nodes;
    state.edges = model.edges;
    state.edgeMap = model.edgeMap;
    state.groups = model.groups;
    state.nodeToGroup = model.nodeToGroup;
    state.adjacency = model.adjacency;
    state.columnCount = model.columnCount;
    state.warnings = model.warnings;

    currentFileName = filename;

  renderCards();
  drawConnections();
    toggleEmptyState(false);
    updateStats(state.columnCount, state.edges.length, filename);

    if (state.warnings.length) {
      showMessage(state.warnings.join("\n"), "warning");
    } else {
      showMessage("Lineage ready. Select any column to explore dependencies.", "success");
    }
  }

  function resetData() {
    state.nodes.clear();
    state.edges = [];
    state.edgeMap.clear();
    state.groups = [];
    state.nodeToGroup.clear();
    state.adjacency = { upstream: new Map(), downstream: new Map() };
    state.columnCount = 0;
    state.warnings = [];
    currentFileName = "";
    refs.cardsContainer.innerHTML = "";
    refs.connectionsLayer.innerHTML = "";
  delete refs.cardsContainer.dataset.minWidth;
  delete refs.cardsContainer.dataset.minHeight;
    updateStats(0, 0, "No file loaded");
    toggleEmptyState(true);
    resetInspector();
    clearHighlights();
  }

  function transformRecords(rows) {
    const nodes = new Map();
    const nodeToGroup = new Map();
    const groups = new Map();
    const adjacencyUp = new Map();
    const adjacencyDown = new Map();
    const edgeMap = new Map();
  const edges = [];
  const warnings = [];
  const edgeKeys = new Set();
  let missingTargetColumnCount = 0;
  let missingSourceColumnCount = 0;
  let fallbackTargetTableCount = 0;
  let fallbackSourceTableCount = 0;

    const ensureGroup = (table) => {
      const key = table.toLowerCase();
      if (!groups.has(key)) {
        groups.set(key, {
          id: `group::table::${key}`,
          label: table,
          type: "table",
          columns: []
        });
      }
      return groups.get(key);
    };

    const ensureColumn = (table, column, file, displayName) => {
      const id = buildNodeId(table, column);
      if (!nodes.has(id)) {
        const group = ensureGroup(table);
        const labelValue = displayName || column || "value";
        const node = {
          id,
          table,
          column,
          file,
          role: "column",
          expression: "",
          groupId: group.id,
          label: `${table}.${labelValue}`,
          displayName: labelValue
        };
        nodes.set(id, node);
        group.columns.push(id);
        nodeToGroup.set(id, group.id);
      }
      return id;
    };

    const link = (sourceId, targetId, expression, file) => {
      const key = `${sourceId}|${targetId}|${expression}`;
      if (edgeKeys.has(key)) return;
      edgeKeys.add(key);
      const edgeId = `e${edgeKeys.size}`;
      const edge = { id: edgeId, source: sourceId, target: targetId, expression, file };
      edges.push(edge);
      edgeMap.set(edgeId, edge);

      if (!adjacencyDown.has(sourceId)) adjacencyDown.set(sourceId, new Set());
      adjacencyDown.get(sourceId).add(edgeId);

      if (!adjacencyUp.has(targetId)) adjacencyUp.set(targetId, new Set());
      adjacencyUp.get(targetId).add(edgeId);
    };

    rows.forEach((row, index) => {
      const record = normalizeRecord(row);

      let targetTable = record.targetTable;
      if (!targetTable) {
        fallbackTargetTableCount += 1;
        targetTable = "Unknown Target";
      }

      let targetColumnKey = record.targetColumn;
      let targetDisplay = record.targetColumn;
      if (!targetColumnKey) {
        missingTargetColumnCount += 1;
        const fallback = record.expression || record.sourceColumn || `column_${index + 1}`;
        targetDisplay = truncate(fallback, 42);
        targetColumnKey = `derived_${hash(`${fallback}_${index}`)}`;
      }

      let sourceTable = record.sourceTable;
      if (!sourceTable) {
        fallbackSourceTableCount += 1;
        sourceTable = "Unknown Source";
      }

      let sourceColumnKey = record.sourceColumn;
      let sourceDisplay = record.sourceColumn;
      if (!sourceColumnKey) {
        missingSourceColumnCount += 1;
        const fallback = record.expression || record.targetColumn || `expr_${index + 1}`;
        sourceDisplay = truncate(fallback, 42);
        sourceColumnKey = `expr_${hash(`${fallback}_${index}`)}`;
      }

      const targetId = ensureColumn(targetTable, targetColumnKey, record.file, targetDisplay);
      const sourceId = ensureColumn(sourceTable, sourceColumnKey, record.file, sourceDisplay);
      link(sourceId, targetId, record.expression, record.file);
    });

    const orderedGroups = orderGroups(Array.from(groups.values()), edges, nodeToGroup);

    if (fallbackTargetTableCount) {
      warnings.push(`${fallbackTargetTableCount} row(s) assigned to "Unknown Target" due to missing target table.`);
    }

    if (fallbackSourceTableCount) {
      warnings.push(`${fallbackSourceTableCount} row(s) assigned to "Unknown Source" due to missing source table.`);
    }

    if (missingTargetColumnCount) {
      warnings.push(`${missingTargetColumnCount} row(s) used derived target column names because none were provided.`);
    }

    if (missingSourceColumnCount) {
      warnings.push(`${missingSourceColumnCount} row(s) used derived source column names because none were provided.`);
    }

    return {
      nodes,
      edges,
      edgeMap,
      groups: orderedGroups,
      nodeToGroup,
      adjacency: { upstream: adjacencyUp, downstream: adjacencyDown },
      columnCount: nodes.size,
      warnings
    };
  }

  function normalizeRecord(row) {
    const normalise = (value) => (value === null || value === undefined ? "" : String(value).trim());
    return {
      sourceTable: normalise(row.source_table),
      sourceColumn: normalise(row.source_column),
      expression: normalise(row.expression),
      targetColumn: normalise(row.target_column),
      targetTable: normalise(row.target_table),
      file: normalise(row.file)
    };
  }

  function orderGroups(groups, edges, nodeToGroup) {
    if (!groups.length) return [];
    const groupById = new Map(groups.map((g) => [g.id, g]));
    const adjacency = new Map(groups.map((g) => [g.id, new Set()]));
    const indegree = new Map(groups.map((g) => [g.id, 0]));

    edges.forEach((edge) => {
      const from = nodeToGroup.get(edge.source);
      const to = nodeToGroup.get(edge.target);
      if (!from || !to || from === to) return;
      const neighbours = adjacency.get(from);
      if (!neighbours.has(to)) {
        neighbours.add(to);
        indegree.set(to, (indegree.get(to) || 0) + 1);
      }
    });

    const queue = [];
    indegree.forEach((count, id) => {
      if (count === 0) queue.push(id);
    });

    const ordered = [];
    const seen = new Set(queue);

    while (queue.length) {
      const current = queue.shift();
      ordered.push(groupById.get(current));
      adjacency.get(current).forEach((next) => {
        indegree.set(next, (indegree.get(next) || 0) - 1);
        if ((indegree.get(next) || 0) === 0 && !seen.has(next)) {
          queue.push(next);
          seen.add(next);
        }
      });
    }

    groups.forEach((group) => {
      if (!seen.has(group.id)) ordered.push(group);
    });

    return ordered;
  }

  function renderCards() {
    refs.cardsContainer.innerHTML = "";
    refs.connectionsLayer.innerHTML = "";

    const layout = computeLaneLayout();

    layout.groups.forEach((group) => {
      const card = document.createElement("article");
      card.className = "lineage-card";
      card.dataset.groupId = group.id;
      card.style.left = `${group.position.x}px`;
      card.style.top = `${group.position.y}px`;

      const header = document.createElement("header");
      header.textContent = group.label;
      card.appendChild(header);

      const list = document.createElement("ul");
      list.className = "column-list";

      group.columns.forEach((nodeId) => {
        const column = state.nodes.get(nodeId);
        if (!column) return;
        const item = document.createElement("li");
        item.className = "column-row";
        item.dataset.nodeId = column.id;
        item.dataset.groupId = group.id;

        const name = document.createElement("span");
        name.className = "column-name";
        name.textContent = column.displayName || column.column || column.label;

        const meta = document.createElement("span");
        meta.className = "column-meta";
        meta.textContent = "column";

        item.appendChild(name);
        item.appendChild(meta);

        item.addEventListener("click", () => highlightColumn(column.id));

        list.appendChild(item);
      });

      card.appendChild(list);
      enableDrag(card);
      refs.cardsContainer.appendChild(card);
    });

    refs.cardsContainer.style.width = `${layout.canvas.width}px`;
    refs.cardsContainer.style.height = `${layout.canvas.height}px`;
    refs.cardsContainer.dataset.minWidth = String(layout.canvas.width);
    refs.cardsContainer.dataset.minHeight = String(layout.canvas.height);
    updateCanvasBounds();
  }

  function computeLaneLayout() {
  const laneGap = 520;
  const verticalGap = 48;
  const stackGap = 160;
    const cardHeaderHeight = 56;
    const rowHeight = 44;
    const cardWidth = 260;

    const incoming = new Map();
    state.edges.forEach((edge) => {
      const sourceGroup = state.nodeToGroup.get(edge.source);
      const targetGroup = state.nodeToGroup.get(edge.target);
      if (!sourceGroup || !targetGroup || sourceGroup === targetGroup) return;
      if (!incoming.has(targetGroup)) incoming.set(targetGroup, new Set());
      incoming.get(targetGroup).add(sourceGroup);
    });

    const levelMap = new Map();
    state.groups.forEach((group) => {
      const parents = incoming.get(group.id) || new Set();
      let level = 0;
      parents.forEach((parentId) => {
        level = Math.max(level, (levelMap.get(parentId) ?? 0) + 1);
      });
      levelMap.set(group.id, level);
    });

    const levelBuckets = new Map();
    state.groups.forEach((group) => {
      const level = levelMap.get(group.id) ?? 0;
      if (!levelBuckets.has(level)) levelBuckets.set(level, []);
      levelBuckets.get(level).push(group);
    });

    const sortedLevels = Array.from(levelBuckets.keys()).sort((a, b) => a - b);

    const positionedGroups = [];
    let requiredWidth = refs.graphWrapper.clientWidth;
    let requiredHeight = refs.graphWrapper.clientHeight;

    sortedLevels.forEach((level) => {
      const groupsAtLevel = levelBuckets.get(level) || [];
      let currentY = verticalGap;
      groupsAtLevel.forEach((group) => {
        const cardHeight = cardHeaderHeight + group.columns.length * rowHeight;
        positionedGroups.push({
          id: group.id,
          label: group.label,
          position: {
            x: level * laneGap,
            y: currentY
          },
          columns: group.columns
        });

        const bottom = currentY + cardHeight;
        if (bottom > requiredHeight) {
          requiredHeight = bottom + stackGap;
        }
        currentY = bottom + stackGap;
      });

      const levelRight = level * laneGap + cardWidth;
      if (levelRight > requiredWidth) {
        requiredWidth = levelRight + laneGap;
      }
    });

    return {
      groups: positionedGroups,
      canvas: {
        width: Math.max(requiredWidth, refs.graphWrapper.clientWidth),
        height: Math.max(requiredHeight, refs.graphWrapper.clientHeight)
      }
    };
  }

  function enableDrag(card) {
    let isDragging = false;
    let startX = 0;
    let startY = 0;
    let initialLeft = 0;
    let initialTop = 0;

    const onPointerDown = (event) => {
      const target = event.target;
      if (target.closest(".column-row")) return;
      isDragging = true;
      startX = event.clientX;
      startY = event.clientY;
      const rect = card.getBoundingClientRect();
      const wrapperRect = refs.graphWrapper.getBoundingClientRect();
      initialLeft = rect.left - wrapperRect.left + refs.graphWrapper.scrollLeft;
      initialTop = rect.top - wrapperRect.top + refs.graphWrapper.scrollTop;
      card.classList.add("dragging");
      card.setPointerCapture(event.pointerId);
      event.preventDefault();
    };

    const onPointerMove = (event) => {
      if (!isDragging) return;
      const deltaX = event.clientX - startX;
      const deltaY = event.clientY - startY;
      const left = initialLeft + deltaX;
      const top = initialTop + deltaY;
      card.style.left = `${left}px`;
      card.style.top = `${top}px`;
      updateCanvasBounds();
      drawConnections();
    };

    const onPointerUp = (event) => {
      if (!isDragging) return;
      isDragging = false;
      card.classList.remove("dragging");
      card.releasePointerCapture(event.pointerId);
      updateCanvasBounds();
      drawConnections();
    };

    card.addEventListener("pointerdown", onPointerDown);
    card.addEventListener("pointermove", onPointerMove);
    card.addEventListener("pointerup", onPointerUp);
    card.addEventListener("pointercancel", onPointerUp);
  }

  function updateCanvasBounds() {
    const cards = Array.from(refs.cardsContainer.querySelectorAll(".lineage-card"));
    if (!cards.length) return;

  const minWidth = parseFloat(refs.cardsContainer.dataset.minWidth || refs.graphWrapper.clientWidth);
  const minHeight = parseFloat(refs.cardsContainer.dataset.minHeight || refs.graphWrapper.clientHeight);

  let maxRight = minWidth;
  let maxBottom = minHeight;

    cards.forEach((card) => {
      const left = parseFloat(card.style.left || "0");
      const top = parseFloat(card.style.top || "0");
      const right = left + card.offsetWidth;
      const bottom = top + card.offsetHeight;
      if (right > maxRight) maxRight = right;
      if (bottom > maxBottom) maxBottom = bottom;
    });

    refs.cardsContainer.style.width = `${Math.max(maxRight + 120, minWidth)}px`;
    refs.cardsContainer.style.height = `${Math.max(maxBottom + 160, minHeight)}px`;
  }

  function drawConnections() {
    if (!state.edges.length) {
      refs.connectionsLayer.innerHTML = "";
      return;
    }

  const wrapper = refs.graphWrapper;
  const svg = refs.connectionsLayer;
  const contentWidth = Math.max(wrapper.scrollWidth, wrapper.clientWidth, refs.cardsContainer.offsetWidth);
  const contentHeight = Math.max(wrapper.scrollHeight, wrapper.clientHeight, refs.cardsContainer.offsetHeight);
  svg.setAttribute("width", contentWidth);
  svg.setAttribute("height", contentHeight);
  svg.setAttribute("viewBox", `0 0 ${contentWidth} ${contentHeight}`);
    svg.innerHTML = "";

    ensureArrowMarker(svg);

    const wrapperRect = wrapper.getBoundingClientRect();
    const offsetX = wrapper.scrollLeft;
    const offsetY = wrapper.scrollTop;
  const edgeOffset = 6;

    state.edges.forEach((edge) => {
      const sourceEl = getColumnElement(edge.source);
      const targetEl = getColumnElement(edge.target);
      if (!sourceEl || !targetEl) return;

      const sourceRect = sourceEl.getBoundingClientRect();
      const targetRect = targetEl.getBoundingClientRect();

      const isSourceLeft = sourceRect.left <= targetRect.left;
      const sourceY = sourceRect.top + sourceRect.height / 2 - wrapperRect.top + offsetY;
      const targetY = targetRect.top + targetRect.height / 2 - wrapperRect.top + offsetY;
      const sourceEdgeX = isSourceLeft
        ? sourceRect.right - wrapperRect.left + offsetX
        : sourceRect.left - wrapperRect.left + offsetX;
      const targetEdgeX = isSourceLeft
        ? targetRect.left - wrapperRect.left + offsetX
        : targetRect.right - wrapperRect.left + offsetX;

      const sourceX = sourceEdgeX + (isSourceLeft ? edgeOffset : -edgeOffset);
      const targetX = targetEdgeX + (isSourceLeft ? -edgeOffset : edgeOffset);

      const midpoint = (sourceX + targetX) / 2;
      const separation = Math.abs(targetX - sourceX);
      const controlMidOffset = Math.max(50, separation / 6);
      const direction = isSourceLeft ? 1 : -1;
      const control1X = (sourceX + midpoint) / 2 + controlMidOffset * direction;
      const control2X = (targetX + midpoint) / 2 - controlMidOffset * direction;

      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", `M ${sourceX} ${sourceY} C ${control1X} ${sourceY}, ${control2X} ${targetY}, ${targetX} ${targetY}`);
      path.setAttribute("class", "link-path");
      path.setAttribute("data-edge-id", edge.id);
    path.setAttribute("marker-end", "url(#arrowhead)");
    path.addEventListener("click", () => highlightLink(edge.id));
      svg.appendChild(path);

    });
  }

  function ensureArrowMarker(svg) {
    let defs = svg.querySelector("defs");
    if (!defs) {
      defs = document.createElementNS("http://www.w3.org/2000/svg", "defs");
      svg.appendChild(defs);
    }
    if (!svg.querySelector("#arrowhead")) {
      const marker = document.createElementNS("http://www.w3.org/2000/svg", "marker");
      marker.setAttribute("id", "arrowhead");
      marker.setAttribute("markerWidth", "10");
      marker.setAttribute("markerHeight", "10");
      marker.setAttribute("refX", "9");
      marker.setAttribute("refY", "5");
      marker.setAttribute("orient", "auto");
      marker.setAttribute("fill", "#38bdf8");

      const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
      path.setAttribute("d", "M 0 0 L 10 5 L 0 10 z");
      marker.appendChild(path);
      defs.appendChild(marker);
    }
  }

  function highlightColumn(nodeId) {
    const column = state.nodes.get(nodeId);
    if (!column) return;

    clearHighlights();

    const upstream = collectReachable(nodeId, state.adjacency.upstream, (edge) => edge.source);
    const downstream = collectReachable(nodeId, state.adjacency.downstream, (edge) => edge.target);

    const selectedRow = getColumnElement(nodeId);
    selectedRow?.classList.add("selected");

    upstream.nodes.forEach((id) => getColumnElement(id)?.classList.add("upstream"));
    downstream.nodes.forEach((id) => getColumnElement(id)?.classList.add("downstream"));

    const applyLabelClass = (edgeIds, className) => {
      edgeIds.forEach((edgeId) => {
        getEdgeElement(edgeId)?.classList.add(className);
      });
    };

  const directUp = state.adjacency.upstream.get(nodeId) || new Set();
  const directDown = state.adjacency.downstream.get(nodeId) || new Set();
  applyLabelClass(new Set([...directUp, ...directDown]), "edge-selected");

    document.querySelectorAll(".column-row").forEach((row) => {
      if (!row.classList.contains("selected") && !row.classList.contains("upstream") && !row.classList.contains("downstream")) {
        row.classList.add("dimmed");
      }
    });

    document.querySelectorAll(".link-path").forEach((path) => {
      if (!path.classList.contains("edge-selected") && !path.classList.contains("edge-upstream") && !path.classList.contains("edge-downstream")) {
        path.classList.add("dimmed");
      }
    });

    populateInspector(column);
  }

  function highlightLink(edgeId) {
    const edge = state.edgeMap.get(edgeId);
    if (!edge) return;
    const targetNode = state.nodes.get(edge.target);
    clearHighlights();

    getEdgeElement(edgeId)?.classList.add("edge-selected");

    const expressions = new Set();
    if (edge.expression) expressions.add(edge.expression);

    if (targetNode) {
      const row = getColumnElement(targetNode.id);
      row?.classList.add("selected");
      populateInspector(targetNode, expressions);
    } else {
      populateInspector({
        id: edge.id,
        displayName: "Link",
        column: "",
        groupId: null,
        table: "",
        file: ""
      }, expressions);
    }

    document.querySelectorAll(".column-row").forEach((row) => {
      if (!row.classList.contains("selected")) {
        row.classList.add("dimmed");
      }
    });

    document.querySelectorAll(".link-path").forEach((path) => {
      if (path.dataset.edgeId !== edge.id) {
        path.classList.add("dimmed");
      }
    });
  }

  function collectReachable(startNode, adjacencyMap, getNextNode) {
    const nodeSet = new Set();
    const edgeSet = new Set();
    const queue = [...(adjacencyMap.get(startNode) || [])];

    while (queue.length) {
      const edgeId = queue.shift();
      const edge = state.edgeMap.get(edgeId);
      if (!edge) continue;
      edgeSet.add(edgeId);
      const nextNode = getNextNode(edge);
      if (!nodeSet.has(nextNode)) {
        nodeSet.add(nextNode);
        const nextEdges = adjacencyMap.get(nextNode);
        if (nextEdges) {
          nextEdges.forEach((id) => {
            if (!edgeSet.has(id)) queue.push(id);
          });
        }
      }
    }

    return { nodes: nodeSet, edges: edgeSet };
  }

  function handleSearch(query) {
    clearHighlights();
    if (!query) {
      showMessage("", "info");
      return;
    }

    const matches = [];
    state.nodes.forEach((column) => {
      if (
        column.displayName.toLowerCase().includes(query) ||
        (column.table && column.table.toLowerCase().includes(query)) ||
        (column.file && column.file.toLowerCase().includes(query)) ||
        (column.expression && column.expression.toLowerCase().includes(query))
      ) {
        matches.push(column.id);
      }
    });

    if (!matches.length) {
      showMessage(`No matches found for “${query}”.`, "warning");
      return;
    }

    matches.forEach((id) => getColumnElement(id)?.classList.add("search-hit"));
    document.querySelectorAll(".column-row:not(.search-hit)").forEach((row) => row.classList.add("dimmed"));
    showMessage(`${matches.length} matches highlighted.`, "info");
  }

  function resetView() {
    clearHighlights();
    resetInspector();
    refs.searchInput.value = "";
    showMessage("", "info");
    drawConnections();
  }

  function clearHighlights() {
    document.querySelectorAll(".column-row").forEach((row) =>
      row.classList.remove("selected", "upstream", "downstream", "dimmed", "search-hit")
    );
    document.querySelectorAll(".link-path").forEach((path) =>
      path.classList.remove("edge-selected", "edge-upstream", "edge-downstream", "dimmed")
    );
  }

  function populateInspector(column, expressionsOverride = null) {
    refs.inspectorEmpty.classList.add("hidden");
    refs.inspector.classList.remove("hidden");
    refs.infoColumn.textContent = column.displayName || column.column || "—";
    const group = state.groups.find((g) => g.id === column.groupId);
    refs.infoTable.textContent = group ? group.label : column.table || "—";
    const files = new Set();
    if (column.file) files.add(column.file);
    const expressions = new Set();
    state.edges.forEach((edge) => {
      if ((edge.source === column.id || edge.target === column.id) && edge.file) {
        files.add(edge.file);
      }
      if ((edge.source === column.id || edge.target === column.id) && edge.expression) {
        expressions.add(edge.expression);
      }
    });
    const expressionList = expressionsOverride && expressionsOverride.size ? expressionsOverride : expressions;

    const fileText = files.size ? Array.from(files).join("\n") : column.file || currentFileName || "—";
    refs.fileView.classList.remove("hidden");
    refs.infoFile.textContent = fileText;

    refs.expressionView.classList.remove("hidden");
    refs.expressionContent.textContent = expressionList && expressionList.size ? Array.from(expressionList).join("\n\n") : "—";
  }

  function resetInspector() {
    refs.inspector.classList.add("hidden");
    refs.inspectorEmpty.classList.remove("hidden");
    refs.infoColumn.textContent = "";
    refs.infoTable.textContent = "";
    refs.infoFile.textContent = "";
    refs.fileView.classList.add("hidden");
    refs.expressionView.classList.add("hidden");
    refs.expressionContent.textContent = "";
  }

  function toggleEmptyState(isEmpty) {
    refs.emptyState.classList.toggle("hidden", !isEmpty);
    refs.graphWrapper.classList.toggle("hidden", isEmpty);
  }

  function updateStats(columns, edges, filename) {
    refs.nodeCount.textContent = columns.toLocaleString();
    refs.edgeCount.textContent = edges.toLocaleString();
    refs.fileName.textContent = filename || "No file loaded";
  }

  function showMessage(text, tone = "info") {
    if (!text) {
      refs.message.classList.add("hidden");
      refs.message.textContent = "";
      return;
    }
    refs.message.textContent = text;
    refs.message.classList.remove("hidden", "info", "success", "warning", "error");
    refs.message.classList.add(tone);
  }

  function downloadImage() {
    if (!state.columnCount) {
      showMessage("Load lineage data before downloading.", "warning");
      return;
    }
    showMessage("Rendering image…", "info");
    html2canvas(refs.graphWrapper, { backgroundColor: "#0b1120", scale: 2 })
      .then((canvas) => {
        const link = document.createElement("a");
        link.download = `${slugify(currentFileName || "lineage-view")}.png`;
        link.href = canvas.toDataURL("image/png");
        link.click();
        showMessage("Download ready.", "success");
      })
      .catch((error) => {
        console.error(error);
        showMessage("Unable to produce image snapshot.", "error");
      });
  }

  function getColumnElement(nodeId) {
    return document.querySelector(`.column-row[data-node-id="${CSS.escape(nodeId)}"]`);
  }

  function getEdgeElement(edgeId) {
    return document.querySelector(`.link-path[data-edge-id="${CSS.escape(edgeId)}"]`);
  }

  function buildNodeId(table, column) {
    const tableKey = table ? table.toLowerCase() : "";
    const columnKey = column ? column.toLowerCase() : "";
    if (tableKey && columnKey) {
      return `table::${tableKey}::${columnKey}`;
    }
    return `col::${hash(`${table || "unknown"}::${column || "value"}`)}`;
  }

  function truncate(value, limit) {
    if (!value || value.length <= limit) return value;
    return `${value.slice(0, limit - 1)}…`;
  }

  function slugify(value) {
    if (!value) return "lineage";
    return value.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 64) || "lineage";
  }

  function hash(value) {
    let h = 0;
    for (let i = 0; i < value.length; i += 1) {
      h = (h << 5) - h + value.charCodeAt(i);
      h |= 0;
    }
    return `h${Math.abs(h)}`;
  }

  function debounce(fn, delay) {
    let timer;
    return (...args) => {
      clearTimeout(timer);
      timer = setTimeout(() => fn(...args), delay);
    };
  }
})();

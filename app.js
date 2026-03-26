
const state = {
  config: {
    allow_all_ids: [],
    high_priority_ids: [],
  },
  candidateIds: new Set(),
  idStats: new Map(),
};

const headerTargets = {
  a: "BridgeIdConfig_A.h",
  b: "BridgeIdConfig_B.h",
};

const elements = {
  statusBar: document.getElementById("statusBar"),
  sortMode: document.getElementById("sortMode"),
  importMode: document.getElementById("importMode"),
  availableFilter: document.getElementById("availableFilter"),
  allowFilter: document.getElementById("allowFilter"),
  highFilter: document.getElementById("highFilter"),
  availableList: document.getElementById("availableList"),
  allowList: document.getElementById("allowList"),
  highList: document.getElementById("highList"),
  availableTitle: document.getElementById("availableTitle"),
  allowTitle: document.getElementById("allowTitle"),
  highTitle: document.getElementById("highTitle"),
  headerPreview: document.getElementById("headerPreview"),
  jsonFileInput: document.getElementById("jsonFileInput"),
  traceFileInput: document.getElementById("traceFileInput"),
};

const hexPrefixPattern = /\b0x([0-9a-f]{1,8})\b/gi;
const hexSuffixPattern = /\b([0-9a-f]{3,8})h\b/gi;

function setStatus(message) {
  elements.statusBar.textContent = message;
}

function parseCanId(raw) {
  const text = String(raw).trim().toUpperCase();
  if (!text) {
    throw new Error("CAN IDが空です");
  }
  if (text.startsWith("0X")) {
    return Number.parseInt(text.slice(2), 16);
  }
  if (/^[0-9A-F]+$/.test(text)) {
    return Number.parseInt(text, 16);
  }
  throw new Error(`CAN IDの形式が正しくありません: ${raw}`);
}

function formatCanId(value) {
  return `0x${value.toString(16).toUpperCase()}`;
}

function uniqueSorted(values) {
  return [...new Set(values)].sort((a, b) => a - b);
}

function ensureValidConfig() {
  state.config.allow_all_ids = uniqueSorted(state.config.allow_all_ids);
  state.config.high_priority_ids = uniqueSorted(
    state.config.high_priority_ids.filter((id) => state.config.allow_all_ids.includes(id)),
  );
}

function emptyStat() {
  return {
    count: 0,
    rx_count: 0,
    tx_count: 0,
    timed_count: 0,
    duration_ms: 0,
    first_ms: null,
    last_ms: null,
  };
}

function mergeStat(target, extra) {
  target.count += extra.count;
  target.rx_count += extra.rx_count || 0;
  target.tx_count += extra.tx_count || 0;
  target.timed_count += extra.timed_count || 0;
  target.duration_ms += extra.duration_ms || 0;
}

function estimateHz(stat) {
  if (!stat || stat.timed_count <= 1 || stat.duration_ms <= 0) {
    return null;
  }
  return (stat.timed_count - 1) / (stat.duration_ms / 1000);
}

function updateStat(map, canId, offsetMs, direction) {
  const stat = map.get(canId) || emptyStat();
  stat.count += 1;
  if (direction === "Rx") {
    stat.rx_count += 1;
  } else if (direction === "Tx") {
    stat.tx_count += 1;
  }
  if (offsetMs !== null && Number.isFinite(offsetMs)) {
    stat.timed_count += 1;
    stat.first_ms = stat.first_ms === null ? offsetMs : Math.min(stat.first_ms, offsetMs);
    stat.last_ms = stat.last_ms === null ? offsetMs : Math.max(stat.last_ms, offsetMs);
  }
  map.set(canId, stat);
}

function finalizeStats(map) {
  for (const stat of map.values()) {
    if (stat.first_ms !== null && stat.last_ms !== null && stat.last_ms >= stat.first_ms) {
      stat.duration_ms = stat.last_ms - stat.first_ms;
    }
  }
}

function directionMatches(mode, direction) {
  if (mode === "両方") return true;
  if (mode === "Rxのみ") return direction === "Rx";
  if (mode === "Txのみ") return direction === "Tx";
  return true;
}

function parseTraceText(text, importMode) {
  const fileStats = new Map();
  const lines = text.split(/\r?\n/);

  for (const line of lines) {
    const parts = line.trim().split(/\s+/);
    let matchedStandard = false;

    if (line.includes(":") && parts.length >= 5 && (parts[2] === "Rx" || parts[2] === "Tx")) {
      try {
        const direction = parts[2];
        if (!directionMatches(importMode, direction)) {
          matchedStandard = true;
          continue;
        }
        const canId = parseCanId(parts[3]);
        const offsetMs = Number.parseFloat(parts[1]);
        updateStat(fileStats, canId, Number.isFinite(offsetMs) ? offsetMs : null, direction);
        matchedStandard = true;
      } catch {
        matchedStandard = false;
      }
    }

    if (matchedStandard) {
      continue;
    }

    if (importMode !== "両方") {
      continue;
    }

    const found = new Set();
    for (const match of line.matchAll(hexPrefixPattern)) {
      try {
        found.add(parseCanId(`0x${match[1]}`));
      } catch {}
    }
    for (const match of line.matchAll(hexSuffixPattern)) {
      try {
        found.add(parseCanId(match[1]));
      } catch {}
    }
    for (const canId of found) {
      updateStat(fileStats, canId, null, null);
    }
  }

  finalizeStats(fileStats);
  return fileStats;
}

function formatLabel(canId) {
  const label = formatCanId(canId);
  const stat = state.idStats.get(canId);
  if (!stat) return label;

  const extras = [`${stat.count}回`];
  if (stat.rx_count && stat.tx_count) {
    extras.push(`Rx${stat.rx_count}/Tx${stat.tx_count}`);
  } else if (stat.rx_count) {
    extras.push(`Rx${stat.rx_count}`);
  } else if (stat.tx_count) {
    extras.push(`Tx${stat.tx_count}`);
  }
  const hz = estimateHz(stat);
  if (hz !== null) {
    extras.push(`${hz.toFixed(1)}Hz`);
  }
  return `${label}  (${extras.join(", ")})`;
}

function sortValues(values) {
  const mode = elements.sortMode.value;
  const sorted = [...values];
  if (mode === "出現回数順") {
    sorted.sort((a, b) => {
      const ac = state.idStats.get(a)?.count || 0;
      const bc = state.idStats.get(b)?.count || 0;
      return bc - ac || a - b;
    });
    return sorted;
  }
  if (mode === "推定周波数順") {
    sorted.sort((a, b) => {
      const ah = estimateHz(state.idStats.get(a));
      const bh = estimateHz(state.idStats.get(b));
      if (ah === null && bh !== null) return 1;
      if (ah !== null && bh === null) return -1;
      if (ah !== null && bh !== null && ah !== bh) return bh - ah;
      const ac = state.idStats.get(a)?.count || 0;
      const bc = state.idStats.get(b)?.count || 0;
      return bc - ac || a - b;
    });
    return sorted;
  }
  sorted.sort((a, b) => a - b);
  return sorted;
}

function filterValues(values, text) {
  const needle = text.trim().toLowerCase();
  if (!needle) return values;
  return values.filter((id) => formatLabel(id).toLowerCase().includes(needle));
}

function fillSelect(select, values) {
  select.innerHTML = "";
  for (const value of values) {
    const option = document.createElement("option");
    option.value = String(value);
    option.textContent = formatLabel(value);
    select.appendChild(option);
  }
}

function selectedIds(select) {
  return [...select.selectedOptions].map((option) => Number.parseInt(option.value, 10));
}

function updateTitles(availableAll, availableFiltered, allowAll, allowFiltered, highAll, highFiltered) {
  elements.availableTitle.textContent = `候補ID (${availableFiltered.length} / ${availableAll.length})`;
  elements.allowTitle.textContent = `通すID (${allowFiltered.length} / ${allowAll.length})`;
  elements.highTitle.textContent = `高優先ID (${highFiltered.length} / ${highAll.length})`;
}

function renderHeaderText() {
  ensureValidConfig();
  const allow = uniqueSorted(state.config.allow_all_ids);
  const high = uniqueSorted(state.config.high_priority_ids.filter((id) => allow.includes(id)));
  return [
    "#pragma once",
    "",
    "// Auto-generated by BridgeConfigToolWeb/app.js",
    "// Do not edit manually; regenerate from bridge_ids.json.",
    "",
    "#include <stdint.h>",
    "",
    `constexpr uint32_t HIGH_PRIORITY_IDS[] = {${high.map(formatCanId).join(", ")}};`,
    `constexpr uint32_t ALLOW_ALL_IDS[] = {${allow.map(formatCanId).join(", ")}};`,
    "",
  ].join("\n");
}

function refreshUi() {
  ensureValidConfig();

  const availableAll = sortValues(uniqueSorted([...state.candidateIds]));
  const allowAll = sortValues(uniqueSorted(state.config.allow_all_ids));
  const highAll = sortValues(uniqueSorted(state.config.high_priority_ids));

  const availableFiltered = filterValues(availableAll, elements.availableFilter.value);
  const allowFiltered = filterValues(allowAll, elements.allowFilter.value);
  const highFiltered = filterValues(highAll, elements.highFilter.value);

  fillSelect(elements.availableList, availableFiltered);
  fillSelect(elements.allowList, allowFiltered);
  fillSelect(elements.highList, highFiltered);
  updateTitles(availableAll, availableFiltered, allowAll, allowFiltered, highAll, highFiltered);
  elements.headerPreview.value = renderHeaderText();
}

function saveBlob(filename, content, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function currentJsonText() {
  ensureValidConfig();
  return `${JSON.stringify({
    allow_all_ids: state.config.allow_all_ids.map(formatCanId),
    high_priority_ids: state.config.high_priority_ids.map(formatCanId),
  }, null, 2)}\n`;
}

async function handleJsonLoad(file) {
  const text = await file.text();
  const data = JSON.parse(text);
  state.config.allow_all_ids = (data.allow_all_ids || []).map(parseCanId);
  state.config.high_priority_ids = (data.high_priority_ids || []).map(parseCanId);
  for (const id of state.config.allow_all_ids) state.candidateIds.add(id);
  for (const id of state.config.high_priority_ids) state.candidateIds.add(id);
  refreshUi();
  setStatus(`読込完了: ${file.name}`);
}

async function handleTraceImport(files) {
  const merged = new Map();
  for (const file of files) {
    const text = await file.text();
    const stats = parseTraceText(text, elements.importMode.value);
    for (const [canId, stat] of stats) {
      const current = merged.get(canId) || emptyStat();
      mergeStat(current, stat);
      merged.set(canId, current);
    }
  }

  for (const [canId, stat] of merged) {
    state.candidateIds.add(canId);
    const current = state.idStats.get(canId) || emptyStat();
    mergeStat(current, stat);
    state.idStats.set(canId, current);
  }

  refreshUi();
  setStatus(`${elements.importMode.value}で ${files.length}個のファイルから ${merged.size} 個のIDを取り込みました`);
}

function moveToAllow() {
  const ids = selectedIds(elements.availableList);
  state.config.allow_all_ids = uniqueSorted([...state.config.allow_all_ids, ...ids]);
  refreshUi();
}

function removeFromAllow() {
  const removeSet = new Set(selectedIds(elements.allowList));
  state.config.allow_all_ids = state.config.allow_all_ids.filter((id) => !removeSet.has(id));
  state.config.high_priority_ids = state.config.high_priority_ids.filter((id) => !removeSet.has(id));
  refreshUi();
}

function moveToHigh() {
  const ids = selectedIds(elements.allowList).filter((id) => state.config.allow_all_ids.includes(id));
  state.config.high_priority_ids = uniqueSorted([...state.config.high_priority_ids, ...ids]);
  refreshUi();
}

function removeFromHigh() {
  const removeSet = new Set(selectedIds(elements.highList));
  state.config.high_priority_ids = state.config.high_priority_ids.filter((id) => !removeSet.has(id));
  refreshUi();
}

function addCustomId() {
  const raw = window.prompt("16進数で入力してください。例: 4E0 または 0x4E0");
  if (!raw) return;
  try {
    const canId = parseCanId(raw);
    state.candidateIds.add(canId);
    refreshUi();
    setStatus(`${formatCanId(canId)} を候補IDに追加しました`);
  } catch (error) {
    window.alert(error.message);
  }
}

function bindEvents() {
  document.getElementById("loadJsonButton").addEventListener("click", () => elements.jsonFileInput.click());
  document.getElementById("saveJsonButton").addEventListener("click", () => {
    saveBlob("bridge_ids.json", currentJsonText(), "application/json");
    setStatus("bridge_ids.json を保存しました");
  });
  document.getElementById("importTraceButton").addEventListener("click", () => elements.traceFileInput.click());
  document.getElementById("addCustomIdButton").addEventListener("click", addCustomId);
  document.getElementById("generateHeaderButton").addEventListener("click", () => {
    elements.headerPreview.value = renderHeaderText();
    setStatus("ヘッダを生成しました");
  });
  document.getElementById("downloadAHeaderButton").addEventListener("click", () => {
    saveBlob(headerTargets.a, renderHeaderText(), "text/plain;charset=utf-8");
    setStatus(`${headerTargets.a} を保存しました`);
  });
  document.getElementById("downloadBHeaderButton").addEventListener("click", () => {
    saveBlob(headerTargets.b, renderHeaderText(), "text/plain;charset=utf-8");
    setStatus(`${headerTargets.b} を保存しました`);
  });

  document.getElementById("toAllowButton").addEventListener("click", moveToAllow);
  document.getElementById("fromAllowButton").addEventListener("click", removeFromAllow);
  document.getElementById("toHighButton").addEventListener("click", moveToHigh);
  document.getElementById("fromHighButton").addEventListener("click", removeFromHigh);

  elements.sortMode.addEventListener("change", refreshUi);
  elements.availableFilter.addEventListener("input", refreshUi);
  elements.allowFilter.addEventListener("input", refreshUi);
  elements.highFilter.addEventListener("input", refreshUi);

  for (const button of document.querySelectorAll("[data-clear-target]")) {
    button.addEventListener("click", () => {
      document.getElementById(button.dataset.clearTarget).value = "";
      refreshUi();
    });
  }

  elements.jsonFileInput.addEventListener("change", async (event) => {
    const [file] = event.target.files;
    if (file) {
      try {
        await handleJsonLoad(file);
      } catch (error) {
        window.alert(`JSON読込に失敗しました: ${error.message}`);
      }
    }
    event.target.value = "";
  });

  elements.traceFileInput.addEventListener("change", async (event) => {
    const files = [...event.target.files];
    if (files.length) {
      try {
        await handleTraceImport(files);
      } catch (error) {
        window.alert(`実データ取込に失敗しました: ${error.message}`);
      }
    }
    event.target.value = "";
  });
}

refreshUi();
bindEvents();
setStatus("準備完了");

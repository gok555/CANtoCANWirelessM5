const state = {
  config: {
    filter_mode: "ALLOW",
    allow_all_ids: [],
    high_priority_ids: [],
  },
  candidateIds: new Set(),
  idStats: new Map(),
  lastTraceStats: new Map(),
};

const headerTargets = {
  a: "BridgeIdConfig_A.h",
  b: "BridgeIdConfig_B.h",
};

const elements = {
  statusBar: document.getElementById("statusBar"),
  sortMode: document.getElementById("sortMode"),
  importMode: document.getElementById("importMode"),
  filterMode: document.getElementById("filterMode"),
  autoHighCount: document.getElementById("autoHighCount"),
  autoAllowCount: document.getElementById("autoAllowCount"),
  autoAssignTrafficButton: document.getElementById("autoAssignTrafficButton"),
  autoAssignRateButton: document.getElementById("autoAssignRateButton"),
  buildStrictFromTraceButton: document.getElementById("buildStrictFromTraceButton"),
  usbConnectButton: document.getElementById("usbConnectButton"),
  usbReadButton: document.getElementById("usbReadButton"),
  usbWriteButton: document.getElementById("usbWriteButton"),
  usbClearLiveButton: document.getElementById("usbClearLiveButton"),
  usbImportLiveButton: document.getElementById("usbImportLiveButton"),
  bleConnectButton: document.getElementById("bleConnectButton"),
  bleReadButton: document.getElementById("bleReadButton"),
  bleWriteButton: document.getElementById("bleWriteButton"),
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
  pmkPasswordInput: document.getElementById("pmkPasswordInput"),
  pmkDerivedKeyDisplay: document.getElementById("pmkDerivedKeyDisplay"),
  pmkSetButton: document.getElementById("pmkSetButton"),
  pmkApplyButton: document.getElementById("pmkApplyButton"),
  pmkCancelButton: document.getElementById("pmkCancelButton"),
  pmkGetStateButton: document.getElementById("pmkGetStateButton"),
  pmkClearButton: document.getElementById("pmkClearButton"),
  pmkStatusDisplay: document.getElementById("pmkStatusDisplay"),
};

const bleState = {
  device: null,
  server: null,
  service: null,
  rxChar: null,
  txChar: null,
  connected: false,
  role: "",
};

const serialState = {
  port: null,
  reader: null,
  writer: null,
  readableClosed: null,
  writableClosed: null,
  connected: false,
  role: "",
};

let ignoreIncomingConfigSnapshot = false;
const pendingDeviceResponses = [];

const pmkUiState = {
  stage: "idle",    // idle / awaiting_ack / ready / applying / switching / verify
  activeKey: null,  // 現在有効なキー (hex文字列 or "NONE" or null=不明)
  pendingKey: null, // ステージング中のキー
};
let pendingObservedStats = null;

const BLE_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e";
const BLE_TX_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e";
const BLE_RX_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e";
const FILTER_MODES = new Set(["ALLOW", "ALL"]);
const DEVICE_MAX_CONFIG_IDS = 128;
const SERIAL_TX_CHUNK_BYTES = 48;
const SERIAL_TX_CHUNK_DELAY_MS = 4;
const hexPrefixPattern = /\b0x([0-9a-f]{1,8})\b/gi;
const hexSuffixPattern = /\b([0-9a-f]{3,8})h\b/gi;
const textEncoder = new TextEncoder();

function setStatus(message) {
  elements.statusBar.textContent = message;
}

function notifyDeviceResponse(message) {
  for (let i = pendingDeviceResponses.length - 1; i >= 0; i -= 1) {
    const entry = pendingDeviceResponses[i];
    if (!entry.matcher(message)) continue;
    pendingDeviceResponses.splice(i, 1);
    clearTimeout(entry.timerId);
    entry.resolve(message);
  }
}

function clearPendingDeviceResponses() {
  while (pendingDeviceResponses.length > 0) {
    const entry = pendingDeviceResponses.pop();
    clearTimeout(entry.timerId);
  }
}

function waitForDeviceResponse(label, matcher, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    const entry = {
      matcher,
      resolve,
      reject,
      timerId: setTimeout(() => {
        const index = pendingDeviceResponses.indexOf(entry);
        if (index >= 0) pendingDeviceResponses.splice(index, 1);
        reject(new Error(`${label} timed out waiting for M5 response`));
      }, timeoutMs),
    };
    pendingDeviceResponses.push(entry);
  });
}

function throwIfDeviceError(step, response) {
  if (response && response.startsWith("ERR=")) {
    throw new Error(`${step} failed: ${response.substring(4)}`);
  }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function setBleUiState() {
  const connected = bleState.connected;
  elements.bleConnectButton.textContent = connected ? "BLE Disconnect" : "BLE Connect";
  elements.bleReadButton.disabled = !connected;
  elements.bleWriteButton.disabled = !connected;
}

function setUsbUiState() {
  const connected = serialState.connected;
  elements.usbConnectButton.textContent = connected ? "USB Disconnect" : "USB Connect";
  elements.usbReadButton.disabled = !connected;
  elements.usbWriteButton.disabled = !connected;
  elements.usbClearLiveButton.disabled = !connected;
  elements.usbImportLiveButton.disabled = !connected;
  setPmkUiState();
}

async function passwordToHex(password) {
  if (!password) return "";
  const encoded = new TextEncoder().encode(password);
  const hashBuffer = await crypto.subtle.digest("SHA-256", encoded);
  const bytes = new Uint8Array(hashBuffer).slice(0, 16);
  return Array.from(bytes).map((b) => b.toString(16).padStart(2, "0")).join("").toUpperCase();
}

function setPmkUiState() {
  const connected = serialState.connected;
  const { stage, activeKey, pendingKey } = pmkUiState;
  const busy = stage === "switching" || stage === "verify" || stage === "applying";
  elements.pmkSetButton.disabled = !connected || busy;
  elements.pmkApplyButton.disabled = !connected || stage !== "ready";
  elements.pmkCancelButton.disabled = !connected || stage === "idle" || busy;
  elements.pmkGetStateButton.disabled = !connected;
  elements.pmkClearButton.disabled = !connected || stage !== "idle";

  const stageLabel = {
    idle: "IDLE",
    awaiting_ack: "STAGING... (B承認待ち)",
    ready: "STAGED ✓ 切替実行可能",
    applying: "切替コマンド送信済み",
    switching: "切替中...",
    verify: "検証中 (PING応答待ち)...",
  }[stage] || stage;

  let text = `状態: ${stageLabel}`;
  if (pendingKey) text += ` | 待機: ${pendingKey.slice(0, 8)}...`;
  if (activeKey && activeKey !== "NONE") text += ` | 有効: ${activeKey.slice(0, 8)}...`;
  else if (activeKey === "NONE") text += " | 暗号化: なし";
  if (!connected) text = "状態: IDLE (USB接続後に使用可)";
  elements.pmkStatusDisplay.textContent = text;
}

async function cmdSetPmkPending(hexKey) {
  const ack = waitForDeviceResponse(
    "SET_PMK_PENDING",
    (msg) => msg.startsWith("PMK_STAGING=") || msg === "PMK_STAGE_OK" || msg.startsWith("ERR="),
    8000,
  );
  await sendSerialCommand(`SET_PMK_PENDING=${hexKey}`);
  const response = await ack;
  throwIfDeviceError("SET_PMK_PENDING", response);
  return response;
}

async function cmdApplyPmk() {
  const ack = waitForDeviceResponse(
    "APPLY_PMK",
    (msg) => msg.startsWith("PMK_APPLY_SENT=") || msg.startsWith("ERR="),
    5000,
  );
  await sendSerialCommand("APPLY_PMK");
  const response = await ack;
  throwIfDeviceError("APPLY_PMK", response);
  return response;
}

async function cmdCancelPmk() {
  const ack = waitForDeviceResponse(
    "CANCEL_PMK",
    (msg) => msg === "PMK_CANCELLED" || msg.startsWith("ERR="),
    3000,
  );
  await sendSerialCommand("CANCEL_PMK");
  const response = await ack;
  throwIfDeviceError("CANCEL_PMK", response);
  return response;
}

async function cmdGetPmkState() {
  const ack = waitForDeviceResponse(
    "GET_PMK_STATE",
    (msg) => msg.startsWith("PMK_STATE=") || msg.startsWith("ERR="),
    3000,
  );
  await sendSerialCommand("GET_PMK_STATE");
  const response = await ack;
  throwIfDeviceError("GET_PMK_STATE", response);
  return response;
}

async function cmdClearPmk() {
  const ack = waitForDeviceResponse(
    "CLEAR_PMK",
    (msg) => msg.startsWith("PMK_CLEARED") || msg.startsWith("ERR="),
    5000,
  );
  await sendSerialCommand("CLEAR_PMK");
  const response = await ack;
  throwIfDeviceError("CLEAR_PMK", response);
  return response;
}

function parseCanId(raw) {
  const text = String(raw).trim().toUpperCase();
  if (!text) throw new Error("CAN ID is empty");
  if (text.startsWith("0X")) return Number.parseInt(text.slice(2), 16);
  if (/^[0-9A-F]+$/.test(text)) return Number.parseInt(text, 16);
  throw new Error(`Bad CAN ID format: ${raw}`);
}

function formatCanId(value) {
  return `0x${value.toString(16).toUpperCase()}`;
}

function uniqueSorted(values) {
  return [...new Set(values)].sort((a, b) => a - b);
}

function normalizeFilterMode(raw) {
  const mode = String(raw || "").trim().toUpperCase();
  return FILTER_MODES.has(mode) ? mode : "ALLOW";
}

function ensureValidConfig() {
  state.config.filter_mode = normalizeFilterMode(state.config.filter_mode);
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
  target.count += extra.count || 0;
  target.rx_count += extra.rx_count || 0;
  target.tx_count += extra.tx_count || 0;
  target.timed_count += extra.timed_count || 0;
  if (extra.first_ms !== null && Number.isFinite(extra.first_ms)) {
    target.first_ms = target.first_ms === null ? extra.first_ms : Math.min(target.first_ms, extra.first_ms);
  }
  if (extra.last_ms !== null && Number.isFinite(extra.last_ms)) {
    target.last_ms = target.last_ms === null ? extra.last_ms : Math.max(target.last_ms, extra.last_ms);
  }
  if ((extra.duration_ms || 0) > 0) {
    target.duration_ms = Math.max(target.duration_ms, extra.duration_ms || 0);
  }
}

function estimateHz(stat) {
  if (!stat || stat.timed_count <= 1 || stat.duration_ms <= 0) return null;
  return (stat.timed_count - 1) / (stat.duration_ms / 1000);
}

function updateStat(map, canId, offsetMs, direction) {
  const stat = map.get(canId) || emptyStat();
  stat.count += 1;
  if (direction === "Rx") stat.rx_count += 1;
  if (direction === "Tx") stat.tx_count += 1;
  if (offsetMs !== null && Number.isFinite(offsetMs)) {
    stat.timed_count += 1;
    stat.first_ms = stat.first_ms === null ? offsetMs : Math.min(stat.first_ms, offsetMs);
    stat.last_ms = stat.last_ms === null ? offsetMs : Math.max(stat.last_ms, offsetMs);
  }
  map.set(canId, stat);
}

function finalizeStats(map) {
  for (const stat of map.values()) {
    if (stat.first_ms !== null && stat.last_ms !== null && stat.last_ms > stat.first_ms) {
      stat.duration_ms = stat.last_ms - stat.first_ms;
    }
  }
}

function directionMatches(mode, direction) {
  if (mode === "both") return true;
  if (mode === "rx") return direction === "Rx";
  if (mode === "tx") return direction === "Tx";
  return true;
}

function extractDirection(line) {
  if (/\bRx\b/i.test(line)) return "Rx";
  if (/\bTx\b/i.test(line)) return "Tx";
  return null;
}

function parseTimestampMs(line) {
  const bracketMatch = line.match(/\[(\d+(?:\.\d+)?)\s*(ms|s)?\]/i);
  if (bracketMatch) {
    const value = Number.parseFloat(bracketMatch[1]);
    if (!Number.isFinite(value)) return null;
    return bracketMatch[2]?.toLowerCase() === "s" ? value * 1000 : value;
  }

  const prefixMatch = line.match(/^\s*(\d+(?:\.\d+)?)\s*(ms|s)?(?:\s|,|;)/i);
  if (!prefixMatch) return null;

  const value = Number.parseFloat(prefixMatch[1]);
  if (!Number.isFinite(value)) return null;
  if (prefixMatch[2]?.toLowerCase() === "s") return value * 1000;
  if (prefixMatch[2]?.toLowerCase() === "ms") return value;
  if (prefixMatch[1].includes(".")) return value * 1000;
  return value;
}

function extractCanIds(line) {
  const sanitized = line.replace(/^\s*\d+:\s+/, "");
  const ids = [];
  for (const match of sanitized.matchAll(hexPrefixPattern)) ids.push(Number.parseInt(match[1], 16));
  for (const match of sanitized.matchAll(hexSuffixPattern)) ids.push(Number.parseInt(match[1], 16));
  if (ids.length > 0) return ids;

  const bareMatches = sanitized.match(/\b[0-9A-F]{3,8}\b/gi) || [];
  for (const text of bareMatches) ids.push(Number.parseInt(text, 16));
  return ids;
}

function parseStructuredTraceLine(line) {
  const ecumasterMatch = line.match(/^\s*\d+:\s+(\d+(?:\.\d+)?)\s+(Rx|Tx)\s+([0-9A-F]{3,8})\s+\d+\b/i);
  if (ecumasterMatch) {
    return {
      timestampMs: Number.parseFloat(ecumasterMatch[1]),
      direction: ecumasterMatch[2],
      ids: [Number.parseInt(ecumasterMatch[3], 16)],
    };
  }

  const genericMatch = line.match(/\b(Rx|Tx)\b.*?\b(?:0x)?([0-9A-F]{3,8})\b/i);
  if (genericMatch) {
    return {
      timestampMs: parseTimestampMs(line),
      direction: genericMatch[1],
      ids: [Number.parseInt(genericMatch[2], 16)],
    };
  }

  return null;
}

function parseTraceText(text, importMode) {
  const fileStats = new Map();
  for (const rawLine of String(text).split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    if (line.startsWith(";")) continue;

    const structured = parseStructuredTraceLine(line);
    const ids = structured?.ids || extractCanIds(line);
    if (!ids.length) continue;

    const direction = structured?.direction || extractDirection(line);
    if (!direction) continue;
    if (direction && !directionMatches(importMode, direction)) continue;

    const timestampMs = structured?.timestampMs ?? parseTimestampMs(line);
    for (const canId of ids) updateStat(fileStats, canId, timestampMs, direction);
  }
  finalizeStats(fileStats);
  return fileStats;
}

function formatLabel(canId) {
  const label = formatCanId(canId);
  const stat = state.idStats.get(canId);
  if (!stat) return label;

  const extras = [`${stat.count} cnt`];
  if (stat.rx_count && stat.tx_count) extras.push(`Rx${stat.rx_count}/Tx${stat.tx_count}`);
  else if (stat.rx_count) extras.push(`Rx${stat.rx_count}`);
  else if (stat.tx_count) extras.push(`Tx${stat.tx_count}`);

  const hz = estimateHz(stat);
  if (hz !== null) extras.push(`${hz.toFixed(1)}Hz`);
  return `${label} (${extras.join(", ")})`;
}

function sortValues(values) {
  const mode = elements.sortMode.value;
  const sorted = [...values];

  if (mode === "traffic") {
    sorted.sort((a, b) => {
      const ac = state.idStats.get(a)?.count || 0;
      const bc = state.idStats.get(b)?.count || 0;
      return bc - ac || a - b;
    });
    return sorted;
  }

  if (mode === "rate") {
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

function rankIdsByTraffic(values) {
  return [...values].sort((a, b) => {
    const ac = state.idStats.get(a)?.count || 0;
    const bc = state.idStats.get(b)?.count || 0;
    return bc - ac || a - b;
  });
}

function rankIdsByRate(values) {
  return [...values].sort((a, b) => {
    const ah = estimateHz(state.idStats.get(a));
    const bh = estimateHz(state.idStats.get(b));
    if (ah === null && bh !== null) return 1;
    if (ah !== null && bh === null) return -1;
    if (ah !== null && bh !== null && ah !== bh) return bh - ah;
    const ac = state.idStats.get(a)?.count || 0;
    const bc = state.idStats.get(b)?.count || 0;
    return bc - ac || a - b;
  });
}

function filterValues(values, text) {
  const needle = String(text || "").trim().toLowerCase();
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
  elements.availableTitle.textContent = `Candidates (${availableFiltered.length} / ${availableAll.length})`;
  elements.allowTitle.textContent = `Allow (${allowFiltered.length} / ${allowAll.length})`;
  elements.highTitle.textContent = `High (${highFiltered.length} / ${highAll.length})`;
}

function renderHeaderText() {
  ensureValidConfig();
  const allow = uniqueSorted(state.config.allow_all_ids);
  const high = uniqueSorted(state.config.high_priority_ids.filter((id) => allow.includes(id)));
  return [
    "#pragma once",
    "",
    "// Auto-generated by BridgeConfigToolWeb/app.js",
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
  elements.filterMode.value = state.config.filter_mode;

  const allowAll = sortValues(uniqueSorted(state.config.allow_all_ids));
  const highAll = sortValues(uniqueSorted(state.config.high_priority_ids));
  const hiddenIds = new Set([...state.config.allow_all_ids, ...state.config.high_priority_ids]);
  const availableAll = sortValues(
    uniqueSorted([...state.candidateIds].filter((id) => !hiddenIds.has(id))),
  );

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
    filter_mode: state.config.filter_mode,
    allow_all_ids: state.config.allow_all_ids.map(formatCanId),
    high_priority_ids: state.config.high_priority_ids.map(formatCanId),
  }, null, 2)}\n`;
}

function parseIdArrayText(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) return [];
  return trimmed.split(",").map((token) => token.trim()).filter(Boolean).map(parseCanId);
}

function parseObservedStatMessage(message) {
  const payload = String(message || "").substring(4);
  const parts = payload.split(",").map((part) => part.trim());
  if (parts.length < 4) throw new Error(`Bad OBS message: ${message}`);

  const canId = parseCanId(parts[0]);
  const count = Number.parseInt(parts[1], 10);
  const firstMs = Number.parseInt(parts[2], 10);
  const lastMs = Number.parseInt(parts[3], 10);
  if (!Number.isFinite(count) || count <= 0) throw new Error(`Bad OBS count: ${message}`);

  const stat = emptyStat();
  stat.count = count;
  stat.rx_count = count;
  if (Number.isFinite(firstMs) && Number.isFinite(lastMs) && lastMs >= firstMs) {
    stat.first_ms = firstMs;
    stat.last_ms = lastMs;
    stat.timed_count = count;
    stat.duration_ms = Math.max(0, lastMs - firstMs);
  }
  return { canId, stat };
}

async function handleJsonLoad(file) {
  const data = JSON.parse(await file.text());
  state.config.filter_mode = normalizeFilterMode(data.filter_mode || "ALLOW");
  state.config.allow_all_ids = (data.allow_all_ids || []).map(parseCanId);
  state.config.high_priority_ids = (data.high_priority_ids || []).map(parseCanId);

  for (const id of state.config.allow_all_ids) state.candidateIds.add(id);
  for (const id of state.config.high_priority_ids) state.candidateIds.add(id);

  refreshUi();
  setStatus(`JSON loaded: ${file.name}`);
}

async function handleTraceImport(files) {
  const merged = new Map();
  for (const file of files) {
    const text = await file.text();
    const stats = parseTraceText(text, elements.importMode.value);
    for (const [canId, stat] of stats.entries()) {
      const current = merged.get(canId) || emptyStat();
      mergeStat(current, stat);
      merged.set(canId, current);
    }
  }

  finalizeStats(merged);
  state.lastTraceStats = new Map();

  for (const [canId, stat] of merged.entries()) {
    state.candidateIds.add(canId);
    state.lastTraceStats.set(canId, { ...stat });
    const current = state.idStats.get(canId) || emptyStat();
    mergeStat(current, stat);
    state.idStats.set(canId, current);
  }

  refreshUi();
  setStatus(`Imported ${merged.size} IDs from ${files.length} file(s). Next: Auto By Count / Auto By Rate.`);
}

function moveToAllow() {
  const ids = selectedIds(elements.availableList);
  state.config.allow_all_ids = uniqueSorted([...state.config.allow_all_ids, ...ids]);
  for (const id of ids) {
    state.candidateIds.delete(id);
  }
  refreshUi();
}

function removeFromAllow() {
  const removeSet = new Set(selectedIds(elements.allowList));
  state.config.allow_all_ids = state.config.allow_all_ids.filter((id) => !removeSet.has(id));
  state.config.high_priority_ids = state.config.high_priority_ids.filter((id) => !removeSet.has(id));
  for (const id of removeSet) {
    state.candidateIds.add(id);
  }
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
  const raw = window.prompt("Enter CAN ID in hex. Example: 4E0 or 0x4E0");
  if (!raw) return;
  try {
    const canId = parseCanId(raw);
    state.candidateIds.add(canId);
    refreshUi();
    setStatus(`Added ${formatCanId(canId)} to candidates`);
  } catch (error) {
    window.alert(error.message);
  }
}

function autoAssignByMetric(metric) {
  const sourceIds = uniqueSorted([...state.candidateIds]);
  const ranked = metric === "traffic"
    ? rankIdsByTraffic(sourceIds)
    : rankIdsByRate(sourceIds);

  if (!ranked.length) {
    window.alert("Import trace first, or add candidate IDs.");
    return;
  }

  const allowCount = Math.max(
    1,
    Math.min(
      DEVICE_MAX_CONFIG_IDS,
      Number.parseInt(elements.autoAllowCount.value, 10) || DEVICE_MAX_CONFIG_IDS,
    ),
  );
  const highCount = Math.max(0, Number.parseInt(elements.autoHighCount.value, 10) || 0);
  const allowIds = ranked.slice(0, Math.min(allowCount, ranked.length));
  state.config.allow_all_ids = uniqueSorted(allowIds);
  state.config.high_priority_ids = uniqueSorted(allowIds.slice(0, Math.min(highCount, allowIds.length)));
  state.candidateIds = new Set();
  refreshUi();

  const label = metric === "traffic" ? "COUNT" : "RATE";
  setStatus(`Auto assign by ${label}: allow ${state.config.allow_all_ids.length}/${DEVICE_MAX_CONFIG_IDS}, high ${state.config.high_priority_ids.length}`);
}

function buildStrictFromTrace() {
  const traceIds = uniqueSorted([...state.lastTraceStats.keys()]);
  if (!traceIds.length) {
    window.alert("Import trace first.");
    return;
  }

  const allowCount = Math.max(
    1,
    Math.min(
      DEVICE_MAX_CONFIG_IDS,
      Number.parseInt(elements.autoAllowCount.value, 10) || DEVICE_MAX_CONFIG_IDS,
    ),
  );
  const highCount = Math.max(0, Number.parseInt(elements.autoHighCount.value, 10) || 0);
  const rankedTraceIds = [...traceIds].sort((a, b) => {
    const ac = state.lastTraceStats.get(a)?.count || 0;
    const bc = state.lastTraceStats.get(b)?.count || 0;
    return bc - ac || a - b;
  });
  const allowIds = rankedTraceIds.slice(0, Math.min(allowCount, rankedTraceIds.length));

  state.config.allow_all_ids = uniqueSorted(allowIds);
  state.config.high_priority_ids = uniqueSorted(
    allowIds.slice(0, Math.min(highCount, allowIds.length)),
  );
  state.candidateIds = new Set();

  refreshUi();
  setStatus(`Trace -> Strict: allow ${state.config.allow_all_ids.length}/${DEVICE_MAX_CONFIG_IDS}, high ${state.config.high_priority_ids.length}`);
}

function validateConfigCounts(target) {
  if (state.config.allow_all_ids.length === 0 && state.candidateIds.size > 0) {
    throw new Error("Imported IDs are still in Candidates. Run Auto By Count / Auto By Rate, or move them to Allow first.");
  }
  if (state.config.allow_all_ids.length > DEVICE_MAX_CONFIG_IDS) {
    throw new Error(`ALLOW is ${state.config.allow_all_ids.length}. ${target} limit is ${DEVICE_MAX_CONFIG_IDS}.`);
  }
  if (state.config.high_priority_ids.length > DEVICE_MAX_CONFIG_IDS) {
    throw new Error(`HIGH is ${state.config.high_priority_ids.length}. ${target} limit is ${DEVICE_MAX_CONFIG_IDS}.`);
  }
}

async function sendBleCommand(command) {
  if (!bleState.connected || !bleState.rxChar) throw new Error("BLE is not connected");
  await bleState.rxChar.writeValue(textEncoder.encode(command));
}

async function sendSerialCommand(command, options = {}) {
  if (!serialState.connected || !serialState.writer) throw new Error("USB is not connected");
  const payload = textEncoder.encode(`${command}\n`);
  const paced = Boolean(options.paced);
  if (!paced || payload.length <= SERIAL_TX_CHUNK_BYTES) {
    await serialState.writer.write(payload);
    return;
  }

  for (let offset = 0; offset < payload.length; offset += SERIAL_TX_CHUNK_BYTES) {
    const chunk = payload.slice(offset, Math.min(offset + SERIAL_TX_CHUNK_BYTES, payload.length));
    await serialState.writer.write(chunk);
    await delay(SERIAL_TX_CHUNK_DELAY_MS);
  }
}

function shouldIgnoreDeviceMessage(message) {
  if (!message) return true;
  if (message.startsWith("[BLE] cmd:")) return true;
  if (message.startsWith("[BOOT]")) return true;
  if (message.startsWith("ESP-ROM:")) return true;
  if (message.startsWith("auto detect board:")) return true;
  if (message.includes(" can>")) return true;
  if (/^(?:[0-9A-F]{2}:){3,}[0-9A-F]{2}$/i.test(message)) return true;
  return false;
}

async function closeSerialPort() {
  if (serialState.reader) {
    try { await serialState.reader.cancel(); } catch {}
    serialState.reader.releaseLock();
    serialState.reader = null;
  }
  if (serialState.writer) {
    try { await serialState.writer.close(); } catch {}
    serialState.writer.releaseLock();
    serialState.writer = null;
  }
  if (serialState.readableClosed) {
    try { await serialState.readableClosed; } catch {}
    serialState.readableClosed = null;
  }
  if (serialState.writableClosed) {
    try { await serialState.writableClosed; } catch {}
    serialState.writableClosed = null;
  }
  if (serialState.port) {
    try { await serialState.port.close(); } catch {}
    serialState.port = null;
  }
}

function handleBleDisconnect() {
  bleState.device = null;
  bleState.server = null;
  bleState.service = null;
  bleState.rxChar = null;
  bleState.txChar = null;
  bleState.connected = false;
  bleState.role = "";
  setBleUiState();
  setStatus("BLE disconnected");
}

async function handleUsbDisconnect() {
  serialState.connected = false;
  serialState.role = "";
  ignoreIncomingConfigSnapshot = false;
  clearPendingDeviceResponses();
  setUsbUiState();
  await closeSerialPort();
  setStatus("USB disconnected");
}

function applyDeviceMessage(message) {
  if (!message || shouldIgnoreDeviceMessage(message)) return;
  notifyDeviceResponse(message);

  if (ignoreIncomingConfigSnapshot) {
    if (
      message.startsWith("ROLE=") ||
      message.startsWith("FILTER=") ||
      message.startsWith("ALLOW=") ||
      message.startsWith("HIGH=")
    ) {
      return;
    }
    if (message === "CFG_DONE") {
      ignoreIncomingConfigSnapshot = false;
      setStatus("Write complete. Use USB Read / BLE Read to verify.");
      return;
    }
  }

  if (message.startsWith("ROLE=")) {
    const role = message.substring(5);
    bleState.role = role;
    serialState.role = role;
    setStatus(`Target: ${role || "M5"}`);
    return;
  }
  if (message.startsWith("FILTER=")) {
    state.config.filter_mode = normalizeFilterMode(message.substring(7));
    refreshUi();
    return;
  }
  if (message.startsWith("ALLOW=")) {
    state.config.allow_all_ids = parseIdArrayText(message.substring(6));
    for (const id of state.config.allow_all_ids) state.candidateIds.add(id);
    refreshUi();
    return;
  }
  if (message.startsWith("HIGH=")) {
    state.config.high_priority_ids = parseIdArrayText(message.substring(5));
    for (const id of state.config.high_priority_ids) state.candidateIds.add(id);
    refreshUi();
    return;
  }
  if (message.startsWith("OBS_BEGIN=") || message === "OBS_CLEAR") {
    return;
  }
  if (message.startsWith("OBS=")) {
    if (pendingObservedStats) {
      const { canId, stat } = parseObservedStatMessage(message);
      pendingObservedStats.set(canId, stat);
    }
    return;
  }
  if (message.startsWith("OBS_DONE=")) {
    return;
  }
  if (message === "CFG_DONE") {
    refreshUi();
    const role = serialState.role || bleState.role;
    setStatus(`Read config from M5${role ? ` (${role})` : ""}`);
    return;
  }
  if (message === "CFG_SAVED") {
    setStatus("Config saved to M5");
    return;
  }
  if (message === "CFG_RESET") {
    setStatus("Config reset on M5");
    return;
  }
  if (message.startsWith("ERR=")) {
    setStatus(`M5 error: ${message.substring(4)}`);
    return;
  }
  if (message.startsWith("PMK_STAGING=")) {
    pmkUiState.pendingKey = message.substring(12);
    pmkUiState.stage = "awaiting_ack";
    setPmkUiState();
    setStatus("PMK: Bへステージング送信中...");
    return;
  }
  if (message === "PMK_STAGE_OK") {
    pmkUiState.stage = "ready";
    setPmkUiState();
    setStatus("PMK: BがOK。「2. 切替実行」を押してください。");
    return;
  }
  if (message.startsWith("PMK_APPLY_SENT=")) {
    pmkUiState.stage = "applying";
    setPmkUiState();
    setStatus(`PMK: 切替コマンド送信済み (${message.substring(15)}ms後に切替)`);
    return;
  }
  if (message === "PMK_SWITCHING") {
    pmkUiState.stage = "switching";
    setPmkUiState();
    setStatus("PMK: 切替中...");
    return;
  }
  if (message.startsWith("PMK_ACTIVE=")) {
    const key = message.substring(11);
    pmkUiState.activeKey = key;
    pmkUiState.pendingKey = null;
    pmkUiState.stage = "idle";
    setPmkUiState();
    setStatus(`PMK: 切替完了 (${key === "NONE" ? "暗号化なし" : key.slice(0, 8) + "..."})`);
    return;
  }
  if (message.startsWith("PMK_REVERT=")) {
    pmkUiState.stage = "idle";
    pmkUiState.pendingKey = null;
    setPmkUiState();
    setStatus(`PMK: 切替失敗・元に戻しました (${message.substring(11)})`);
    return;
  }
  if (message.startsWith("SYNC_") || message === "PONG") return;
}

async function pumpSerialReader() {
  const decoder = new TextDecoder();
  let pending = "";
  while (serialState.connected && serialState.reader) {
    const { value, done } = await serialState.reader.read();
    if (done) break;
    pending += decoder.decode(value, { stream: true });
    const parts = pending.split(/\r?\n/);
    pending = parts.pop() || "";
    for (const part of parts) applyDeviceMessage(part.trim());
  }
}

async function connectUsbSerial() {
  if (serialState.connected) {
    await handleUsbDisconnect();
    return;
  }
  if (!("serial" in navigator)) {
    window.alert("Web Serial is not supported in this browser. Use Chrome or Edge.");
    return;
  }

  setStatus("USB: selecting port");
  const port = await navigator.serial.requestPort();

  setStatus("USB: opening port");
  await port.open({ baudRate: 115200 });

  if (!port.readable) {
    throw new Error("USB open ok, but port.readable is missing");
  }
  if (!port.writable) {
    throw new Error("USB open ok, but port.writable is missing");
  }

  serialState.port = port;
  setStatus("USB: creating reader");
  serialState.reader = port.readable.getReader();
  setStatus("USB: creating writer");
  serialState.writer = port.writable.getWriter();
  serialState.connected = true;
  serialState.readableClosed = null;
  serialState.writableClosed = null;
  setUsbUiState();
  setStatus("USB connected");

  (async () => {
    try {
      await pumpSerialReader();
    } catch {
      if (serialState.connected) {
        await handleUsbDisconnect();
      }
    }
  })();

  await delay(80);
  await sendSerialCommand("PING");
}

async function connectBle() {
  if (bleState.connected) {
    if (bleState.device?.gatt?.connected) bleState.device.gatt.disconnect();
    handleBleDisconnect();
    return;
  }
  if (!navigator.bluetooth) {
    window.alert("Web Bluetooth is not supported in this browser. Use Chrome or Edge.");
    return;
  }

  const device = await navigator.bluetooth.requestDevice({
    filters: [{ services: [BLE_SERVICE_UUID] }],
    optionalServices: [BLE_SERVICE_UUID],
  });
  device.addEventListener("gattserverdisconnected", handleBleDisconnect);

  const server = await device.gatt.connect();
  const service = await server.getPrimaryService(BLE_SERVICE_UUID);
  const txChar = await service.getCharacteristic(BLE_TX_UUID);
  const rxChar = await service.getCharacteristic(BLE_RX_UUID);

  await txChar.startNotifications();
  txChar.addEventListener("characteristicvaluechanged", (event) => {
    const value = new TextDecoder().decode(event.target.value);
    for (const line of value.split(/\r?\n/)) applyDeviceMessage(line.trim());
  });

  bleState.device = device;
  bleState.server = server;
  bleState.service = service;
  bleState.txChar = txChar;
  bleState.rxChar = rxChar;
  bleState.connected = true;
  setBleUiState();
  setStatus("BLE connected");
}

async function readConfigFromBle() {
  await sendBleCommand("REQUEST_CFG");
  setStatus("Reading M5 config over BLE");
}

async function writeConfigToBle() {
  validateConfigCounts("M5");
  ignoreIncomingConfigSnapshot = true;
  clearPendingDeviceResponses();
  try {
    const allowLine = state.config.allow_all_ids.map(formatCanId).join(",");
    const highLine = state.config.high_priority_ids.map(formatCanId).join(",");
    const filterAck = waitForDeviceResponse("SET_FILTER", (msg) => msg.startsWith("FILTER_OK=") || msg.startsWith("ERR="), 4000);
    await sendBleCommand(`SET_FILTER=${state.config.filter_mode}`);
    throwIfDeviceError("SET_FILTER", await filterAck);
    const allowAck = waitForDeviceResponse("SET_ALLOW", (msg) => msg.startsWith("ALLOW_OK=") || msg.startsWith("ERR="), 10000);
    await sendBleCommand(`SET_ALLOW=${allowLine}`);
    throwIfDeviceError("SET_ALLOW", await allowAck);
    const highAck = waitForDeviceResponse("SET_HIGH", (msg) => msg.startsWith("HIGH_OK=") || msg.startsWith("ERR="), 6000);
    await sendBleCommand(`SET_HIGH=${highLine}`);
    throwIfDeviceError("SET_HIGH", await highAck);
    const saveAck = waitForDeviceResponse("SAVE_CFG", (msg) => msg === "CFG_SAVED" || msg.startsWith("ERR="), 10000);
    await sendBleCommand("SAVE_CFG");
    throwIfDeviceError("SAVE_CFG", await saveAck);
    setStatus("Config saved to M5 over BLE. Use BLE Read to verify.");
  } catch (error) {
    ignoreIncomingConfigSnapshot = false;
    clearPendingDeviceResponses();
    throw error;
  }
}

async function readConfigFromUsb() {
  await sendSerialCommand("REQUEST_CFG");
  setStatus("Reading M5 config over USB");
}

async function writeConfigToUsb() {
  validateConfigCounts("M5");
  ignoreIncomingConfigSnapshot = true;
  clearPendingDeviceResponses();
  try {
    const allowLine = state.config.allow_all_ids.map(formatCanId).join(",");
    const highLine = state.config.high_priority_ids.map(formatCanId).join(",");
    setStatus("USB write: SET_FILTER");
    const filterAck = waitForDeviceResponse("SET_FILTER", (msg) => msg.startsWith("FILTER_OK=") || msg.startsWith("ERR="), 4000);
    await sendSerialCommand(`SET_FILTER=${state.config.filter_mode}`);
    throwIfDeviceError("SET_FILTER", await filterAck);
    setStatus("USB write: SET_ALLOW");
    const allowAck = waitForDeviceResponse("SET_ALLOW", (msg) => msg.startsWith("ALLOW_OK=") || msg.startsWith("ERR="), 10000);
    await sendSerialCommand(`SET_ALLOW=${allowLine}`, { paced: true });
    throwIfDeviceError("SET_ALLOW", await allowAck);
    setStatus("USB write: SET_HIGH");
    const highAck = waitForDeviceResponse("SET_HIGH", (msg) => msg.startsWith("HIGH_OK=") || msg.startsWith("ERR="), 6000);
    await sendSerialCommand(`SET_HIGH=${highLine}`, { paced: true });
    throwIfDeviceError("SET_HIGH", await highAck);
    setStatus("USB write: SAVE_CFG");
    const saveAck = waitForDeviceResponse("SAVE_CFG", (msg) => msg === "CFG_SAVED" || msg.startsWith("ERR="), 10000);
    await sendSerialCommand("SAVE_CFG");
    throwIfDeviceError("SAVE_CFG", await saveAck);
    setStatus("Config saved to M5 over USB. Use USB Read to verify.");
  } catch (error) {
    ignoreIncomingConfigSnapshot = false;
    clearPendingDeviceResponses();
    throw error;
  }
}

async function clearObservedLiveUsb() {
  const ack = waitForDeviceResponse("OBS_CLEAR", (msg) => msg === "OBS_CLEAR" || msg.startsWith("ERR="), 3000);
  await sendSerialCommand("OBS_CLEAR");
  throwIfDeviceError("OBS_CLEAR", await ack);
  pendingObservedStats = null;
  setStatus("Live capture cleared. Run with FILTER=ALL, then USB Import Live.");
}

async function importObservedLiveUsb() {
  pendingObservedStats = new Map();
  const ack = waitForDeviceResponse("OBS_DUMP", (msg) => msg.startsWith("OBS_DONE=") || msg.startsWith("ERR="), 15000);
  await sendSerialCommand("OBS_DUMP");
  const response = await ack;
  throwIfDeviceError("OBS_DUMP", response);

  const observed = pendingObservedStats || new Map();
  pendingObservedStats = null;
  if (observed.size === 0) {
    throw new Error("No live IDs captured. Use FILTER=ALL, let traffic flow, then try again.");
  }

  state.lastTraceStats = new Map();
  for (const [canId, stat] of observed.entries()) {
    state.candidateIds.add(canId);
    state.lastTraceStats.set(canId, { ...stat });
    const current = state.idStats.get(canId) || emptyStat();
    mergeStat(current, stat);
    state.idStats.set(canId, current);
  }

  refreshUi();
  setStatus(`Imported ${observed.size} live IDs from M5. Next: Auto By Count / Trace -> Strict.`);
}

function bindEvents() {
  document.getElementById("loadJsonButton").addEventListener("click", () => elements.jsonFileInput.click());
  document.getElementById("saveJsonButton").addEventListener("click", () => {
    saveBlob("bridge_ids.json", currentJsonText(), "application/json");
    setStatus("Saved bridge_ids.json");
  });
  document.getElementById("importTraceButton").addEventListener("click", () => elements.traceFileInput.click());
  document.getElementById("addCustomIdButton").addEventListener("click", addCustomId);
  document.getElementById("generateHeaderButton").addEventListener("click", () => {
    elements.headerPreview.value = renderHeaderText();
    setStatus("Header generated");
  });
  document.getElementById("downloadAHeaderButton").addEventListener("click", () => {
    saveBlob(headerTargets.a, renderHeaderText(), "text/plain;charset=utf-8");
    setStatus(`Saved ${headerTargets.a}`);
  });
  document.getElementById("downloadBHeaderButton").addEventListener("click", () => {
    saveBlob(headerTargets.b, renderHeaderText(), "text/plain;charset=utf-8");
    setStatus(`Saved ${headerTargets.b}`);
  });

  document.getElementById("toAllowButton").addEventListener("click", moveToAllow);
  document.getElementById("fromAllowButton").addEventListener("click", removeFromAllow);
  document.getElementById("toHighButton").addEventListener("click", moveToHigh);
  document.getElementById("fromHighButton").addEventListener("click", removeFromHigh);

  elements.autoAssignTrafficButton.addEventListener("click", () => autoAssignByMetric("traffic"));
  elements.autoAssignRateButton.addEventListener("click", () => autoAssignByMetric("rate"));
  elements.buildStrictFromTraceButton.addEventListener("click", buildStrictFromTrace);

  elements.usbConnectButton.addEventListener("click", async () => {
    try {
      await connectUsbSerial();
    } catch (error) {
      setStatus(`USB error: ${error.message}`);
      window.alert(`USB connect failed: ${error && (error.stack || error.message || String(error))}`);
    }
  });
  elements.usbReadButton.addEventListener("click", async () => {
    try {
      await readConfigFromUsb();
    } catch (error) {
      setStatus(`USB read error: ${error.message}`);
      window.alert(`USB read failed: ${error.message}`);
    }
  });
  elements.usbWriteButton.addEventListener("click", async () => {
    try {
      setStatus("USB write clicked");
      await writeConfigToUsb();
    } catch (error) {
      setStatus(`USB write error: ${error.message}`);
      window.alert(`USB write failed: ${error.message}`);
    }
  });
  elements.usbClearLiveButton.addEventListener("click", async () => {
    try {
      await clearObservedLiveUsb();
    } catch (error) {
      setStatus(`USB live clear error: ${error.message}`);
      window.alert(`USB live clear failed: ${error.message}`);
    }
  });
  elements.usbImportLiveButton.addEventListener("click", async () => {
    try {
      await importObservedLiveUsb();
    } catch (error) {
      setStatus(`USB live import error: ${error.message}`);
      window.alert(`USB live import failed: ${error.message}`);
    }
  });

  elements.bleConnectButton.addEventListener("click", async () => {
    try {
      await connectBle();
    } catch (error) {
      setStatus(`BLE error: ${error.message}`);
      window.alert(`BLE connect failed: ${error.message}`);
    }
  });
  elements.bleReadButton.addEventListener("click", async () => {
    try {
      await readConfigFromBle();
    } catch (error) {
      setStatus(`BLE read error: ${error.message}`);
      window.alert(`BLE read failed: ${error.message}`);
    }
  });
  elements.bleWriteButton.addEventListener("click", async () => {
    try {
      await writeConfigToBle();
    } catch (error) {
      setStatus(`BLE write error: ${error.message}`);
      window.alert(`BLE write failed: ${error.message}`);
    }
  });

  elements.sortMode.addEventListener("change", refreshUi);
  elements.filterMode.addEventListener("change", () => {
    state.config.filter_mode = normalizeFilterMode(elements.filterMode.value);
    refreshUi();
  });
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
        window.alert(`JSON load failed: ${error.message}`);
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
        window.alert(`Trace import failed: ${error.message}`);
      }
    }
    event.target.value = "";
  });

  // PMK パスワード入力 → SHA-256 変換して表示
  elements.pmkPasswordInput.addEventListener("input", async () => {
    const pw = elements.pmkPasswordInput.value;
    elements.pmkDerivedKeyDisplay.value = pw ? await passwordToHex(pw) : "";
  });

  elements.pmkSetButton.addEventListener("click", async () => {
    const hexKey = elements.pmkDerivedKeyDisplay.value.trim().toUpperCase();
    if (hexKey.length !== 32) {
      window.alert("パスワードを入力してください。");
      return;
    }
    try {
      setStatus("PMK: ステージング送信中...");
      pmkUiState.pendingKey = hexKey;
      await cmdSetPmkPending(hexKey);
    } catch (error) {
      pmkUiState.stage = "idle";
      setPmkUiState();
      setStatus(`PMK エラー: ${error.message}`);
      window.alert(`PMK ステージング失敗:\n${error.message}`);
    }
  });

  elements.pmkApplyButton.addEventListener("click", async () => {
    if (!window.confirm("ESP-NOW パスワードを切替します。\n2秒後にA・B両ノードが同時に切替わります。\n\n続けますか？")) return;
    try {
      setStatus("PMK: 切替コマンド送信中...");
      await cmdApplyPmk();
    } catch (error) {
      pmkUiState.stage = "idle";
      setPmkUiState();
      setStatus(`PMK エラー: ${error.message}`);
      window.alert(`PMK 切替失敗:\n${error.message}`);
    }
  });

  elements.pmkCancelButton.addEventListener("click", async () => {
    try {
      await cmdCancelPmk();
      pmkUiState.stage = "idle";
      pmkUiState.pendingKey = null;
      setPmkUiState();
      setStatus("PMK: キャンセル済");
    } catch (error) {
      setStatus(`PMK エラー: ${error.message}`);
    }
  });

  elements.pmkGetStateButton.addEventListener("click", async () => {
    try {
      const response = await cmdGetPmkState();
      // PMK_STATE=IDLE,active=NONE などをパース
      const m = response.match(/PMK_STATE=(\w+),active=(\S+)/);
      if (m) {
        pmkUiState.stage = m[1].toLowerCase();
        pmkUiState.activeKey = m[2];
      }
      setPmkUiState();
      setStatus(`PMK 状態: ${response}`);
    } catch (error) {
      setStatus(`PMK エラー: ${error.message}`);
    }
  });

  elements.pmkClearButton.addEventListener("click", async () => {
    if (!window.confirm("ESP-NOW 暗号化を無効化します（パスワード削除）。\n続けますか？")) return;
    try {
      await cmdClearPmk();
      pmkUiState.activeKey = "NONE";
      pmkUiState.pendingKey = null;
      pmkUiState.stage = "idle";
      elements.pmkPasswordInput.value = "";
      elements.pmkDerivedKeyDisplay.value = "";
      setPmkUiState();
      setStatus("PMK: パスワード削除済 (暗号化なし)");
    } catch (error) {
      setStatus(`PMK エラー: ${error.message}`);
      window.alert(`PMK 削除失敗:\n${error.message}`);
    }
  });
}

bindEvents();
refreshUi();
setBleUiState();
setUsbUiState();
setPmkUiState();
setStatus("READY v20260329f");

const state = {
  config: {
    filter_mode: "ALLOW",
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
  filterMode: document.getElementById("filterMode"),
  autoHighCount: document.getElementById("autoHighCount"),
  autoAssignTrafficButton: document.getElementById("autoAssignTrafficButton"),
  autoAssignRateButton: document.getElementById("autoAssignRateButton"),
  usbConnectButton: document.getElementById("usbConnectButton"),
  usbReadButton: document.getElementById("usbReadButton"),
  usbWriteButton: document.getElementById("usbWriteButton"),
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

const BLE_SERVICE_UUID = "6e400001-b5a3-f393-e0a9-e50e24dcca9e";
const BLE_TX_UUID = "6e400003-b5a3-f393-e0a9-e50e24dcca9e";
const BLE_RX_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e";
const FILTER_MODES = new Set(["ALLOW", "ALL"]);

const hexPrefixPattern = /\b0x([0-9a-f]{1,8})\b/gi;
const hexSuffixPattern = /\b([0-9a-f]{3,8})h\b/gi;

function setStatus(message) {
  elements.statusBar.textContent = message;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function setBleUiState() {
  const connected = bleState.connected;
  elements.bleConnectButton.textContent = connected ? "BLE切断" : "BLE接続";
  elements.bleReadButton.disabled = !connected;
  elements.bleWriteButton.disabled = !connected;
}

function setUsbUiState() {
  const connected = serialState.connected;
  elements.usbConnectButton.textContent = connected ? "USB切断" : "USB接続";
  elements.usbReadButton.disabled = !connected;
  elements.usbWriteButton.disabled = !connected;
}

function parseCanId(raw) {
  const text = String(raw).trim().toUpperCase();
  if (!text) {
    throw new Error("CAN ID が空です");
  }
  if (text.startsWith("0X")) {
    return Number.parseInt(text.slice(2), 16);
  }
  if (/^[0-9A-F]+$/.test(text)) {
    return Number.parseInt(text, 16);
  }
  throw new Error(`CAN ID の形式が不正です: ${raw}`);
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
    if (stat.first_ms !== null && stat.last_ms !== null && stat.last_ms > stat.first_ms) {
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

function extractDirection(line) {
  if (/\bRx\b/i.test(line)) return "Rx";
  if (/\bTx\b/i.test(line)) return "Tx";
  if (/\bRX\b/.test(line)) return "Rx";
  if (/\bTX\b/.test(line)) return "Tx";
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
  if (!prefixMatch) {
    return null;
  }

  const value = Number.parseFloat(prefixMatch[1]);
  if (!Number.isFinite(value)) {
    return null;
  }
  if (prefixMatch[2]?.toLowerCase() === "s") {
    return value * 1000;
  }
  if (prefixMatch[2]?.toLowerCase() === "ms") {
    return value;
  }
  if (prefixMatch[1].includes(".")) {
    return value * 1000;
  }
  return value;
}

function extractCanIds(line) {
  const ids = [];

  for (const match of line.matchAll(hexPrefixPattern)) {
    ids.push(Number.parseInt(match[1], 16));
  }
  for (const match of line.matchAll(hexSuffixPattern)) {
    ids.push(Number.parseInt(match[1], 16));
  }

  if (ids.length > 0) {
    return ids;
  }

  const bareMatches = line.match(/\b[0-9A-F]{3,4}\b/gi) || [];
  for (const text of bareMatches) {
    ids.push(Number.parseInt(text, 16));
  }
  return ids;
}

function parseTraceText(text, importMode) {
  const fileStats = new Map();

  for (const rawLine of String(text).split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) {
      continue;
    }

    const ids = extractCanIds(line);
    if (!ids.length) {
      continue;
    }

    const direction = extractDirection(line);
    if (direction && !directionMatches(importMode, direction)) {
      continue;
    }

    const timestampMs = parseTimestampMs(line);
    for (const canId of ids) {
      updateStat(fileStats, canId, timestampMs, direction);
    }
  }

  finalizeStats(fileStats);
  return fileStats;
}

function formatLabel(canId) {
  const label = formatCanId(canId);
  const stat = state.idStats.get(canId);
  if (!stat) {
    return label;
  }

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

  return `${label} (${extras.join(", ")})`;
}

function sortValues(values) {
  const mode = elements.sortMode.value;
  const sorted = [...values];

  if (mode === "通信回数順") {
    sorted.sort((a, b) => {
      const ac = state.idStats.get(a)?.count || 0;
      const bc = state.idStats.get(b)?.count || 0;
      return bc - ac || a - b;
    });
    return sorted;
  }

  if (mode === "推定Hz順") {
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
  const needle = text.trim().toLowerCase();
  if (!needle) {
    return values;
  }
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
  elements.filterMode.value = state.config.filter_mode;

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
  return `${JSON.stringify(
    {
      filter_mode: state.config.filter_mode,
      allow_all_ids: state.config.allow_all_ids.map(formatCanId),
      high_priority_ids: state.config.high_priority_ids.map(formatCanId),
    },
    null,
    2,
  )}\n`;
}

function parseIdArrayText(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) {
    return [];
  }
  return trimmed
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map(parseCanId);
}

async function handleJsonLoad(file) {
  const text = await file.text();
  const data = JSON.parse(text);
  state.config.filter_mode = normalizeFilterMode(data.filter_mode);
  state.config.allow_all_ids = (data.allow_all_ids || []).map(parseCanId);
  state.config.high_priority_ids = (data.high_priority_ids || []).map(parseCanId);

  for (const id of state.config.allow_all_ids) {
    state.candidateIds.add(id);
  }
  for (const id of state.config.high_priority_ids) {
    state.candidateIds.add(id);
  }

  refreshUi();
  setStatus(`JSONを読み込みました: ${file.name}`);
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
  setStatus(`${elements.importMode.value}で ${files.length} ファイルから ${merged.size} 件のIDを取り込みました`);
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
  if (!raw) {
    return;
  }
  try {
    const canId = parseCanId(raw);
    state.candidateIds.add(canId);
    refreshUi();
    setStatus(`${formatCanId(canId)} を候補IDに追加しました`);
  } catch (error) {
    window.alert(error.message);
  }
}

function autoAssignByMetric(metric) {
  const ranked =
    metric === "traffic"
      ? rankIdsByTraffic(uniqueSorted([...state.candidateIds]))
      : rankIdsByRate(uniqueSorted([...state.candidateIds]));

  if (!ranked.length) {
    window.alert("先に実データを取り込むか、候補IDを追加してください。");
    return;
  }

  const highCount = Math.max(0, Number.parseInt(elements.autoHighCount.value, 10) || 0);
  state.config.allow_all_ids = uniqueSorted(ranked);
  state.config.high_priority_ids = uniqueSorted(ranked.slice(0, Math.min(highCount, ranked.length)));
  refreshUi();

  const label = metric === "traffic" ? "通信量順" : "速度順";
  setStatus(`${label}で自動振分しました。通すID ${state.config.allow_all_ids.length}件 / 高優先 ${state.config.high_priority_ids.length}件`);
}

async function sendBleCommand(command) {
  if (!bleState.connected || !bleState.rxChar) {
    throw new Error("BLE接続されていません");
  }
  await bleState.rxChar.writeValue(new TextEncoder().encode(command));
}

async function sendSerialCommand(command) {
  if (!serialState.connected || !serialState.writer) {
    throw new Error("USB接続されていません");
  }
  await serialState.writer.write(`${command}\n`);
}

function shouldIgnoreDeviceMessage(message) {
  if (!message) {
    return true;
  }
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
    try {
      await serialState.reader.cancel();
    } catch {}
    serialState.reader.releaseLock();
    serialState.reader = null;
  }

  if (serialState.writer) {
    try {
      await serialState.writer.close();
    } catch {}
    serialState.writer.releaseLock();
    serialState.writer = null;
  }

  if (serialState.readableClosed) {
    try {
      await serialState.readableClosed;
    } catch {}
    serialState.readableClosed = null;
  }

  if (serialState.writableClosed) {
    try {
      await serialState.writableClosed;
    } catch {}
    serialState.writableClosed = null;
  }

  if (serialState.port) {
    try {
      await serialState.port.close();
    } catch {}
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
  setStatus("BLE接続が切れました");
}

async function handleUsbDisconnect() {
  serialState.connected = false;
  serialState.role = "";
  setUsbUiState();
  await closeSerialPort();
  setStatus("USB接続が切れました");
}

function applyDeviceMessage(message) {
  if (!message || shouldIgnoreDeviceMessage(message)) {
    return;
  }

  if (message.startsWith("ROLE=")) {
    const role = message.substring(5);
    bleState.role = role;
    serialState.role = role;
    setStatus(`接続先: ${role || "M5"}`);
    return;
  }

  if (message.startsWith("FILTER=")) {
    state.config.filter_mode = normalizeFilterMode(message.substring(7));
    refreshUi();
    return;
  }

  if (message.startsWith("ALLOW=")) {
    state.config.allow_all_ids = parseIdArrayText(message.substring(6));
    for (const id of state.config.allow_all_ids) {
      state.candidateIds.add(id);
    }
    refreshUi();
    return;
  }

  if (message.startsWith("HIGH=")) {
    state.config.high_priority_ids = parseIdArrayText(message.substring(5));
    for (const id of state.config.high_priority_ids) {
      state.candidateIds.add(id);
    }
    refreshUi();
    return;
  }

  if (message === "CFG_DONE") {
    refreshUi();
    const role = serialState.role || bleState.role;
    setStatus(`M5${role ? `(${role})` : ""} の設定を読み込みました`);
    return;
  }

  if (message === "CFG_SAVED") {
    const role = serialState.role || bleState.role;
    setStatus(`M5${role ? `(${role})` : ""} に設定を保存しました`);
    return;
  }

  if (message === "CFG_RESET") {
    const role = serialState.role || bleState.role;
    setStatus(`M5${role ? `(${role})` : ""} を初期設定へ戻しました`);
    return;
  }

  if (
    message.startsWith("ALLOW_OK=") ||
    message.startsWith("HIGH_OK=") ||
    message.startsWith("FILTER_OK=") ||
    message.startsWith("SYNC_") ||
    message === "PONG"
  ) {
    return;
  }

  if (message.startsWith("ERR=")) {
    setStatus(`M5エラー: ${message}`);
    return;
  }

  setStatus(`受信: ${message}`);
}

async function readSerialLoop() {
  let buffer = "";
  try {
    while (serialState.connected && serialState.reader) {
      const { value, done } = await serialState.reader.read();
      if (done) {
        break;
      }
      buffer += value;
      const lines = buffer.split(/\r?\n/);
      buffer = lines.pop() || "";
      for (const line of lines) {
        applyDeviceMessage(line.trim());
      }
    }
  } catch (error) {
    if (serialState.connected) {
      setStatus(`USB受信エラー: ${error.message}`);
    }
  } finally {
    if (serialState.connected) {
      await handleUsbDisconnect();
    }
  }
}

async function connectUsbSerial() {
  if (!("serial" in navigator)) {
    window.alert("このブラウザは Web Serial に対応していません。Chrome か Edge を使ってください。");
    return;
  }

  if (serialState.connected) {
    await handleUsbDisconnect();
    return;
  }

  const port = await navigator.serial.requestPort();
  await port.open({ baudRate: 115200 });

  const decoder = new TextDecoderStream();
  serialState.readableClosed = port.readable.pipeTo(decoder.writable);
  serialState.reader = decoder.readable.getReader();

  const encoder = new TextEncoderStream();
  serialState.writableClosed = encoder.readable.pipeTo(port.writable);
  serialState.writer = encoder.writable.getWriter();

  serialState.port = port;
  serialState.connected = true;
  setUsbUiState();
  setStatus("USB接続しました");
  readSerialLoop();
  await delay(80);
  await sendSerialCommand("PING");
}

function handleBleNotification(event) {
  const value = new TextDecoder().decode(event.target.value);
  applyDeviceMessage(value.trim());
}

async function connectBle() {
  if (!("bluetooth" in navigator)) {
    window.alert("このブラウザは Web Bluetooth に対応していません。Chrome か Edge を使ってください。");
    return;
  }

  if (bleState.connected) {
    if (bleState.device?.gatt?.connected) {
      bleState.device.gatt.disconnect();
    }
    handleBleDisconnect();
    return;
  }

  const device = await navigator.bluetooth.requestDevice({
    filters: [{ namePrefix: "M5" }],
    optionalServices: [BLE_SERVICE_UUID],
  });

  device.addEventListener("gattserverdisconnected", handleBleDisconnect);

  let server = null;
  let service = null;
  let txChar = null;
  let rxChar = null;
  let lastError = null;

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      server = await device.gatt.connect();
      await delay(500);
      service = await server.getPrimaryService(BLE_SERVICE_UUID);
      await delay(150);
      txChar = await service.getCharacteristic(BLE_TX_UUID);
      rxChar = await service.getCharacteristic(BLE_RX_UUID);
      lastError = null;
      break;
    } catch (error) {
      lastError = error;
      if (device.gatt.connected) {
        device.gatt.disconnect();
      }
      await delay(600);
    }
  }

  if (lastError) {
    throw new Error(`GATT接続に失敗しました: ${lastError.message}`);
  }

  await txChar.startNotifications();
  txChar.addEventListener("characteristicvaluechanged", handleBleNotification);

  bleState.device = device;
  bleState.server = server;
  bleState.service = service;
  bleState.txChar = txChar;
  bleState.rxChar = rxChar;
  bleState.connected = true;
  setBleUiState();
  setStatus(`BLE接続しました: ${device.name || "M5"}`);
}

async function readConfigFromBle() {
  await delay(500);
  await sendBleCommand("REQUEST_CFG");
  setStatus("BLE経由でM5設定を読込中です");
}

async function writeConfigToBle() {
  ensureValidConfig();
  const allowLine = state.config.allow_all_ids.map(formatCanId).join(",");
  const highLine = state.config.high_priority_ids.map(formatCanId).join(",");
  await sendBleCommand(`SET_FILTER=${state.config.filter_mode}`);
  await sendBleCommand(`SET_ALLOW=${allowLine}`);
  await sendBleCommand(`SET_HIGH=${highLine}`);
  await sendBleCommand("SAVE_CFG");
  await sendBleCommand("REQUEST_CFG");
  setStatus("BLE経由でM5へ設定を書込中です");
}

async function readConfigFromUsb() {
  await sendSerialCommand("REQUEST_CFG");
  setStatus("USB経由でM5設定を読込中です");
}

async function writeConfigToUsb() {
  ensureValidConfig();
  const allowLine = state.config.allow_all_ids.map(formatCanId).join(",");
  const highLine = state.config.high_priority_ids.map(formatCanId).join(",");
  await sendSerialCommand(`SET_FILTER=${state.config.filter_mode}`);
  await delay(40);
  await sendSerialCommand(`SET_ALLOW=${allowLine}`);
  await delay(40);
  await sendSerialCommand(`SET_HIGH=${highLine}`);
  await delay(40);
  await sendSerialCommand("SAVE_CFG");
  await delay(120);
  await sendSerialCommand("REQUEST_CFG");
  setStatus("USB経由でM5へ設定を書込中です");
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

  elements.autoAssignTrafficButton.addEventListener("click", () => autoAssignByMetric("traffic"));
  elements.autoAssignRateButton.addEventListener("click", () => autoAssignByMetric("rate"));

  elements.usbConnectButton.addEventListener("click", async () => {
    try {
      await connectUsbSerial();
    } catch (error) {
      setStatus(`USB接続エラー: ${error.message}`);
      window.alert(`USB接続に失敗しました: ${error.message}`);
    }
  });

  elements.usbReadButton.addEventListener("click", async () => {
    try {
      await readConfigFromUsb();
    } catch (error) {
      setStatus(`USB読込エラー: ${error.message}`);
      window.alert(`USBでのM5読込に失敗しました: ${error.message}`);
    }
  });

  elements.usbWriteButton.addEventListener("click", async () => {
    try {
      await writeConfigToUsb();
    } catch (error) {
      setStatus(`USB書込エラー: ${error.message}`);
      window.alert(`USBでのM5書込に失敗しました: ${error.message}`);
    }
  });

  elements.bleConnectButton.addEventListener("click", async () => {
    try {
      await connectBle();
    } catch (error) {
      setStatus(`BLE接続エラー: ${error.message}`);
      window.alert(`BLE接続に失敗しました: ${error.message}`);
    }
  });

  elements.bleReadButton.addEventListener("click", async () => {
    try {
      await readConfigFromBle();
    } catch (error) {
      setStatus(`BLE読込エラー: ${error.message}`);
      window.alert(`BLEでのM5読込に失敗しました: ${error.message}`);
    }
  });

  elements.bleWriteButton.addEventListener("click", async () => {
    try {
      await writeConfigToBle();
    } catch (error) {
      setStatus(`BLE書込エラー: ${error.message}`);
      window.alert(`BLEでのM5書込に失敗しました: ${error.message}`);
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

  setUsbUiState();
  setBleUiState();
}

refreshUi();
bindEvents();
setStatus("準備完了");

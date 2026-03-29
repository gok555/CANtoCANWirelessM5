#include <M5Unified.h>
#include <Preferences.h>
#include <WiFi.h>
#include <algorithm>
#include <NimBLEDevice.h>
#include <driver/twai.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include "BridgeIdConfig.h"

namespace {
constexpr gpio_num_t CAN_TX_PIN = GPIO_NUM_5;
constexpr gpio_num_t CAN_RX_PIN = GPIO_NUM_6;
constexpr uint16_t BRIDGE_MAGIC = 0x4342;
constexpr uint16_t CONFIG_MAGIC = 0x4346;
constexpr uint8_t BRIDGE_VERSION = 1;
constexpr uint8_t NODE_ID = 0xA1;
constexpr uint32_t PAIR_TIMEOUT_MS = 30000;
constexpr uint32_t DISPLAY_INTERVAL_MS = 250;
constexpr uint32_t STATS_INTERVAL_MS = 1000;
constexpr uint32_t LED_REFRESH_MS = 50;
constexpr uint32_t HIGH_FLUSH_MS = 10;
constexpr uint32_t LOW_FLUSH_MS = 100;
constexpr uint8_t HIGH_BURST_TRIGGER = 8;
constexpr uint8_t LOW_BURST_TRIGGER = 15;
constexpr uint16_t ECHO_SUPPRESS_MS = 8;
constexpr uint32_t LED_PULSE_MS = 120;
constexpr uint32_t USB_ACTIVITY_HOLD_MS = 60000;
constexpr uint32_t UI_STATUS_HOLD_MS = 2500;
constexpr uint32_t CAN_RECOVERY_RETRY_MS = 250;
constexpr uint32_t CAN_RECOVERY_TIMEOUT_MS = 3000;
constexpr uint32_t CAN_READY_WAIT_MS = 1500;
constexpr uint32_t CAN_PASSIVE_BACKOFF_MS = 200;
constexpr uint32_t CAN_QUEUE_PURGE_MS = 1200;
constexpr size_t CAN_QUEUE_PURGE_THRESHOLD = 32;
constexpr uint32_t CAN_ERROR_LOG_INTERVAL_MS = 500;
constexpr uint32_t CAN_TX_RESUME_DELAY_MS = 300;
constexpr size_t MAX_CONFIG_IDS = 128;
constexpr size_t MAX_SERIAL_COMMAND_LEN = 2048;
constexpr size_t MAX_OBSERVED_IDS = 256;
constexpr size_t CONFIG_IDS_PER_PACKET = 48;
constexpr size_t BLE_COMMAND_QUEUE_CAPACITY = 16;
constexpr size_t BLE_NOTIFY_QUEUE_CAPACITY = 24;
constexpr size_t HIGH_QUEUE_CAPACITY = 1024;
constexpr size_t LOW_QUEUE_CAPACITY = 2048;
constexpr size_t CAN_TX_QUEUE_CAPACITY = 512;
constexpr uint8_t MAX_FRAMES_PER_PACKET = 15;
constexpr uint8_t BCAST_MAC[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
constexpr char BLE_DEVICE_NAME[] = "M5CAN-A";
constexpr char BLE_SERVICE_UUID[] = "6e400001-b5a3-f393-e0a9-e50e24dcca9e";
constexpr char BLE_TX_UUID[] = "6e400003-b5a3-f393-e0a9-e50e24dcca9e";
constexpr char BLE_RX_UUID[] = "6e400002-b5a3-f393-e0a9-e50e24dcca9e";
constexpr char CONFIG_PREFS_NAMESPACE[] = "bridgecfg";
constexpr uint32_t CONFIG_MODE_HOLD_MS = 1200;
constexpr uint8_t CONFIG_TYPE_ALLOW_CHUNK = 1;
constexpr uint8_t CONFIG_TYPE_HIGH_CHUNK = 2;
constexpr uint8_t CONFIG_TYPE_COMMIT = 3;
constexpr uint8_t CONFIG_TYPE_ACK = 4;
constexpr uint8_t CONFIG_ACK_OK = 1;
constexpr uint8_t CONFIG_ACK_INCOMPLETE = 2;
constexpr uint8_t CONFIG_ACK_INVALID = 3;
constexpr uint8_t FILTER_MODE_ALLOW_LIST = 0;
constexpr uint8_t FILTER_MODE_ALL = 1;
constexpr uint16_t ROLE_HEADER_COLOR = TFT_CYAN;
constexpr uint16_t ROLE_TEXT_COLOR = TFT_BLACK;
constexpr char ROLE_LABEL[] = "A";

struct BridgeFrame {
  uint32_t id;
  uint8_t dlc;
  uint8_t flags;
  uint8_t data[8];
  uint16_t stamp_ms;
} __attribute__((packed));

struct BridgePacket {
  uint16_t magic;
  uint8_t version;
  uint8_t source;
  uint16_t sequence;
  uint8_t count;
  BridgeFrame frames[MAX_FRAMES_PER_PACKET];
} __attribute__((packed));

struct ConfigChunkPacket {
  uint16_t magic;
  uint8_t version;
  uint8_t source;
  uint8_t type;
  uint8_t chunk_index;
  uint8_t chunk_count;
  uint8_t item_count;
  uint32_t ids[CONFIG_IDS_PER_PACKET];
} __attribute__((packed));

struct ConfigCommitPacket {
  uint16_t magic;
  uint8_t version;
  uint8_t source;
  uint8_t type;
  uint8_t filter_mode;
  uint16_t allow_count;
  uint16_t high_count;
} __attribute__((packed));

struct ConfigAckPacket {
  uint16_t magic;
  uint8_t version;
  uint8_t source;
  uint8_t type;
  uint8_t status;
  uint8_t filter_mode;
  uint16_t allow_count;
  uint16_t high_count;
} __attribute__((packed));

struct FrameRing {
  BridgeFrame* data = nullptr;
  size_t capacity = 0;
  volatile size_t head = 0;
  volatile size_t tail = 0;
  portMUX_TYPE mux = portMUX_INITIALIZER_UNLOCKED;

  bool begin(size_t wanted, bool usePsram = false) {
    capacity = wanted;
    if (usePsram && psramFound()) {
      data = static_cast<BridgeFrame*>(ps_malloc(sizeof(BridgeFrame) * capacity));
    } else {
      data = static_cast<BridgeFrame*>(malloc(sizeof(BridgeFrame) * capacity));
    }
    return data != nullptr;
  }

  bool push(const BridgeFrame& frame) {
    portENTER_CRITICAL(&mux);
    const size_t next = (head + 1) % capacity;
    if (next == tail) {
      portEXIT_CRITICAL(&mux);
      return false;
    }
    data[head] = frame;
    head = next;
    portEXIT_CRITICAL(&mux);
    return true;
  }

  bool pop(BridgeFrame& frame) {
    portENTER_CRITICAL(&mux);
    if (head == tail) {
      portEXIT_CRITICAL(&mux);
      return false;
    }
    frame = data[tail];
    tail = (tail + 1) % capacity;
    portEXIT_CRITICAL(&mux);
    return true;
  }

  size_t count() const {
    return (head + capacity - tail) % capacity;
  }
};

struct BridgeStats {
  volatile uint32_t can_rx_total = 0;
  volatile uint32_t can_tx_total = 0;
  volatile uint32_t now_rx_total = 0;
  volatile uint32_t now_tx_total = 0;
  volatile uint32_t can_drop_total = 0;
  volatile uint32_t now_drop_total = 0;
  volatile uint32_t echo_drop_total = 0;
  volatile uint32_t can_rx_sec = 0;
  volatile uint32_t can_tx_sec = 0;
  volatile uint32_t now_rx_sec = 0;
  volatile uint32_t now_tx_sec = 0;
};

struct EchoEntry {
  uint32_t signature = 0;
  uint32_t expires_at = 0;
};

struct ObservedIdStat {
  uint32_t id = 0;
  uint32_t count = 0;
  uint32_t first_ms = 0;
  uint32_t last_ms = 0;
};

FrameRing highQueue;
FrameRing lowQueue;
FrameRing canTxQueue;
BridgeStats stats;
EchoEntry echoCache[32];
ObservedIdStat observedIdStats[MAX_OBSERVED_IDS] = {};
size_t observedIdCount = 0;
bool observedIdOverflow = false;
portMUX_TYPE observedMux = portMUX_INITIALIZER_UNLOCKED;

Preferences prefs;
Preferences configPrefs;
uint8_t peerMac[6] = {};
bool hasPeer = false;
bool pairMode = false;
uint32_t pairStartMs = 0;
uint32_t lastPairBroadcastMs = 0;
uint32_t buttonPressMs = 0;
uint16_t sequenceNumber = 0;
uint32_t lastHighSendMs = 0;
uint32_t lastLowSendMs = 0;
uint32_t lastStatsMs = 0;
uint32_t lastDisplayMs = 0;
uint32_t lastLedMs = 0;
uint32_t ledSuccessUntilMs = 0;
uint32_t ledErrorUntilMs = 0;
uint32_t canRxPerSec = 0;
uint32_t canTxPerSec = 0;
volatile bool canRecoveryActive = false;
uint32_t canRecoveryStartedMs = 0;
uint32_t lastCanRecoveryAttemptMs = 0;
uint32_t canRecoveryCount = 0;
uint32_t canRestartCount = 0;
uint32_t canFaultSinceMs = 0;
uint32_t lastCanErrorLogMs = 0;
uint32_t canQueuePurgeCount = 0;
uint32_t canTxResumeAtMs = 0;
uint32_t runtimeAllowIds[MAX_CONFIG_IDS] = {};
uint32_t runtimeHighIds[MAX_CONFIG_IDS] = {};
size_t runtimeAllowCount = 0;
size_t runtimeHighCount = 0;
uint8_t runtimeFilterMode = FILTER_MODE_ALLOW_LIST;
uint32_t scratchAllowIds[MAX_CONFIG_IDS] = {};
uint32_t scratchHighIds[MAX_CONFIG_IDS] = {};
size_t scratchAllowCount = 0;
size_t scratchHighCount = 0;
uint8_t scratchFilterMode = FILTER_MODE_ALLOW_LIST;
portMUX_TYPE configMux = portMUX_INITIALIZER_UNLOCKED;
uint32_t pendingAllowIds[MAX_CONFIG_IDS] = {};
uint32_t pendingHighIds[MAX_CONFIG_IDS] = {};
size_t pendingAllowCount = 0;
size_t pendingHighCount = 0;
uint8_t pendingAllowChunkCount = 0;
uint8_t pendingHighChunkCount = 0;
uint32_t pendingAllowChunkMask = 0;
uint32_t pendingHighChunkMask = 0;
String serialCommandBuffer;
String uiStatusText = "READY";
uint16_t uiStatusTextColor = TFT_BLACK;
uint16_t uiStatusBgColor = TFT_DARKGREY;
uint32_t uiStatusUntilMs = 0;
uint32_t usbActivityUntilMs = 0;

BLEServer* bleServer = nullptr;
BLECharacteristic* bleTxCharacteristic = nullptr;
BLECharacteristic* bleRxCharacteristic = nullptr;
bool bleClientConnected = false;
bool bleConfigOnlyMode = false;
String bleCommandQueue[BLE_COMMAND_QUEUE_CAPACITY];
size_t bleCommandHead = 0;
size_t bleCommandTail = 0;
String bleNotifyQueue[BLE_NOTIFY_QUEUE_CAPACITY];
size_t bleNotifyHead = 0;
size_t bleNotifyTail = 0;
portMUX_TYPE bleQueueMux = portMUX_INITIALIZER_UNLOCKED;

String macToString(const uint8_t* mac) {
  char buf[18];
  snprintf(buf, sizeof(buf), "%02X:%02X:%02X:%02X:%02X:%02X",
           mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  return String(buf);
}

bool isZeroMac(const uint8_t* mac) {
  for (int i = 0; i < 6; ++i) {
    if (mac[i] != 0) {
      return false;
    }
  }
  return true;
}

void setUiStatus(const String& text, uint16_t bgColor, uint16_t textColor, uint32_t holdMs = UI_STATUS_HOLD_MS) {
  uiStatusText = text;
  uiStatusBgColor = bgColor;
  uiStatusTextColor = textColor;
  uiStatusUntilMs = millis() + holdMs;
}

void noteUsbActivity(const String& text = "USB OK") {
  usbActivityUntilMs = millis() + USB_ACTIVITY_HOLD_MS;
  setUiStatus(text, TFT_CYAN, TFT_BLACK, 1800);
}

bool usbRecentlyActive() {
  return millis() < usbActivityUntilMs;
}

void updateUiStatusFromMessage(const String& message) {
  if (message == "PONG") {
    noteUsbActivity("USB OK");
    return;
  }
  if (message == "CFG_SAVED") {
    setUiStatus("SAVE OK", TFT_GREEN, TFT_BLACK, 3000);
    return;
  }
  if (message == "CFG_RESET") {
    setUiStatus("RESET OK", TFT_YELLOW, TFT_BLACK, 3000);
    return;
  }
  if (message.startsWith("SYNC_APPLIED=") || message.startsWith("SYNC_OK=")) {
    setUiStatus("SYNC OK", TFT_GREEN, TFT_BLACK, 3500);
    return;
  }
  if (message.startsWith("SYNC_SENT=")) {
    setUiStatus("SYNC SEND", TFT_BLUE, TFT_WHITE, 2500);
    return;
  }
  if (message.startsWith("ALLOW_OK=") || message.startsWith("HIGH_OK=") || message.startsWith("FILTER_OK=")) {
    setUiStatus("EDIT OK", TFT_GREEN, TFT_BLACK, 1800);
    return;
  }
  if (message.startsWith("ERR=") || message.startsWith("SYNC_ERR=")) {
    setUiStatus("CFG ERR", TFT_RED, TFT_WHITE, 4000);
  }
}

bool pushBleCommand(const String& command) {
  bool pushed = false;
  portENTER_CRITICAL(&bleQueueMux);
  const size_t next = (bleCommandHead + 1) % BLE_COMMAND_QUEUE_CAPACITY;
  if (next != bleCommandTail) {
    bleCommandQueue[bleCommandHead] = command;
    bleCommandHead = next;
    pushed = true;
  }
  portEXIT_CRITICAL(&bleQueueMux);
  return pushed;
}

bool popBleCommand(String& command) {
  bool popped = false;
  portENTER_CRITICAL(&bleQueueMux);
  if (bleCommandHead != bleCommandTail) {
    command = bleCommandQueue[bleCommandTail];
    bleCommandTail = (bleCommandTail + 1) % BLE_COMMAND_QUEUE_CAPACITY;
    popped = true;
  }
  portEXIT_CRITICAL(&bleQueueMux);
  return popped;
}

bool pushBleNotify(const String& message) {
  bool pushed = false;
  portENTER_CRITICAL(&bleQueueMux);
  const size_t next = (bleNotifyHead + 1) % BLE_NOTIFY_QUEUE_CAPACITY;
  if (next != bleNotifyTail) {
    bleNotifyQueue[bleNotifyHead] = message;
    bleNotifyHead = next;
    pushed = true;
  }
  portEXIT_CRITICAL(&bleQueueMux);
  return pushed;
}

bool popBleNotify(String& message) {
  bool popped = false;
  portENTER_CRITICAL(&bleQueueMux);
  if (bleNotifyHead != bleNotifyTail) {
    message = bleNotifyQueue[bleNotifyTail];
    bleNotifyTail = (bleNotifyTail + 1) % BLE_NOTIFY_QUEUE_CAPACITY;
    popped = true;
  }
  portEXIT_CRITICAL(&bleQueueMux);
  return popped;
}

bool allConfigChunksReceived(uint32_t mask, uint8_t chunkCount) {
  if (chunkCount == 0) {
    return true;
  }
  if (chunkCount >= 32) {
    return false;
  }
  const uint32_t expectedMask = (1UL << chunkCount) - 1UL;
  return (mask & expectedMask) == expectedMask;
}

const char* filterModeName(uint8_t mode) {
  return mode == FILTER_MODE_ALL ? "ALL" : "ALLOW";
}

size_t normalizeIds(uint32_t* ids, size_t count) {
  if (count == 0) {
    return 0;
  }

  for (size_t i = 0; i < count; ++i) {
    for (size_t j = i + 1; j < count; ++j) {
      if (ids[j] < ids[i]) {
        const uint32_t tmp = ids[i];
        ids[i] = ids[j];
        ids[j] = tmp;
      }
    }
  }

  size_t uniqueCount = 0;
  for (size_t i = 0; i < count; ++i) {
    if (uniqueCount == 0 || ids[i] != ids[uniqueCount - 1]) {
      ids[uniqueCount++] = ids[i];
    }
  }
  return uniqueCount;
}

bool containsRuntimeId(const uint32_t* ids, size_t count, uint32_t id) {
  for (size_t i = 0; i < count; ++i) {
    if (ids[i] == id) {
      return true;
    }
  }
  return false;
}

void copyDefaultConfigToRuntime() {
  runtimeFilterMode = FILTER_MODE_ALLOW_LIST;
  runtimeAllowCount = std::min(sizeof(ALLOW_ALL_IDS) / sizeof(ALLOW_ALL_IDS[0]), MAX_CONFIG_IDS);
  for (size_t i = 0; i < runtimeAllowCount; ++i) {
    runtimeAllowIds[i] = ALLOW_ALL_IDS[i];
  }
  runtimeAllowCount = normalizeIds(runtimeAllowIds, runtimeAllowCount);

  runtimeHighCount = 0;
  const size_t defaultHighCount = sizeof(HIGH_PRIORITY_IDS) / sizeof(HIGH_PRIORITY_IDS[0]);
  for (size_t i = 0; i < defaultHighCount && runtimeHighCount < MAX_CONFIG_IDS; ++i) {
    if (containsRuntimeId(runtimeAllowIds, runtimeAllowCount, HIGH_PRIORITY_IDS[i])) {
      runtimeHighIds[runtimeHighCount++] = HIGH_PRIORITY_IDS[i];
    }
  }
  runtimeHighCount = normalizeIds(runtimeHighIds, runtimeHighCount);
}

void setRuntimeConfig(const uint32_t* allowIds, size_t allowCount, const uint32_t* highIds, size_t highCount, uint8_t filterMode) {
  portENTER_CRITICAL(&configMux);
  runtimeFilterMode = (filterMode == FILTER_MODE_ALL) ? FILTER_MODE_ALL : FILTER_MODE_ALLOW_LIST;
  runtimeAllowCount = std::min(allowCount, MAX_CONFIG_IDS);
  for (size_t i = 0; i < runtimeAllowCount; ++i) {
    runtimeAllowIds[i] = allowIds[i];
  }
  runtimeAllowCount = normalizeIds(runtimeAllowIds, runtimeAllowCount);

  runtimeHighCount = 0;
  for (size_t i = 0; i < highCount && runtimeHighCount < MAX_CONFIG_IDS; ++i) {
    if (containsRuntimeId(runtimeAllowIds, runtimeAllowCount, highIds[i])) {
      runtimeHighIds[runtimeHighCount++] = highIds[i];
    }
  }
  runtimeHighCount = normalizeIds(runtimeHighIds, runtimeHighCount);
  portEXIT_CRITICAL(&configMux);
}

void copyRuntimeConfig(uint32_t* allowIds, size_t& allowCount, uint32_t* highIds, size_t& highCount, uint8_t& filterMode) {
  portENTER_CRITICAL(&configMux);
  filterMode = runtimeFilterMode;
  allowCount = runtimeAllowCount;
  highCount = runtimeHighCount;
  for (size_t i = 0; i < allowCount; ++i) {
    allowIds[i] = runtimeAllowIds[i];
  }
  for (size_t i = 0; i < highCount; ++i) {
    highIds[i] = runtimeHighIds[i];
  }
  portEXIT_CRITICAL(&configMux);
}

bool parseIdList(const String& listText, uint32_t* outIds, size_t& outCount) {
  outCount = 0;
  String text = listText;
  text.trim();
  if (text.isEmpty()) {
    return true;
  }

  int start = 0;
  while (start <= text.length()) {
    int comma = text.indexOf(',', start);
    String token = comma >= 0 ? text.substring(start, comma) : text.substring(start);
    token.trim();
    if (!token.isEmpty()) {
      const char* begin = token.c_str();
      if (token.startsWith("0x") || token.startsWith("0X")) {
        begin += 2;
      }
      if (*begin == '\0') {
        return false;
      }
      char* end = nullptr;
      const unsigned long parsed = strtoul(begin, &end, 16);
      if (end == begin || *end != '\0') {
        return false;
      }
      const uint32_t id = static_cast<uint32_t>(parsed);
      if (outCount >= MAX_CONFIG_IDS) {
        return false;
      }
      outIds[outCount++] = id;
    }
    if (comma < 0) {
      break;
    }
    start = comma + 1;
  }

  outCount = normalizeIds(outIds, outCount);
  return true;
}

String buildIdList(const uint32_t* ids, size_t count) {
  String text;
  text.reserve(count * 11);
  for (size_t i = 0; i < count; ++i) {
    if (i > 0) {
      text += ",";
    }
    char buf[12];
    snprintf(buf, sizeof(buf), "0x%lX", static_cast<unsigned long>(ids[i]));
    text += buf;
  }
  return text;
}

bool loadRuntimeConfigFromPrefs() {
  copyDefaultConfigToRuntime();

  configPrefs.begin(CONFIG_PREFS_NAMESPACE, true);
  const size_t allowBytes = configPrefs.getBytesLength("allow_ids");
  const size_t highBytes = configPrefs.getBytesLength("high_ids");
  if (allowBytes == 0) {
    configPrefs.end();
    return false;
  }

  const size_t loadedAllowCount = std::min(allowBytes / sizeof(uint32_t), MAX_CONFIG_IDS);
  const size_t loadedHighCount = std::min(highBytes / sizeof(uint32_t), MAX_CONFIG_IDS);
  const uint8_t filterMode = configPrefs.getUChar("filter_mode", FILTER_MODE_ALLOW_LIST);
  memset(scratchAllowIds, 0, sizeof(scratchAllowIds));
  memset(scratchHighIds, 0, sizeof(scratchHighIds));
  configPrefs.getBytes("allow_ids", scratchAllowIds, loadedAllowCount * sizeof(uint32_t));
  if (loadedHighCount > 0) {
    configPrefs.getBytes("high_ids", scratchHighIds, loadedHighCount * sizeof(uint32_t));
  }
  configPrefs.end();

  setRuntimeConfig(scratchAllowIds, loadedAllowCount, scratchHighIds, loadedHighCount, filterMode);
  return true;
}

void saveRuntimeConfigToPrefs() {
  copyRuntimeConfig(scratchAllowIds, scratchAllowCount, scratchHighIds, scratchHighCount, scratchFilterMode);

  configPrefs.begin(CONFIG_PREFS_NAMESPACE, false);
  configPrefs.putBytes("allow_ids", scratchAllowIds, scratchAllowCount * sizeof(uint32_t));
  configPrefs.putBytes("high_ids", scratchHighIds, scratchHighCount * sizeof(uint32_t));
  configPrefs.putUChar("filter_mode", scratchFilterMode);
  configPrefs.end();
}

void pushConfigResponse(const String& message);

void clearObservedIds() {
  portENTER_CRITICAL(&observedMux);
  observedIdCount = 0;
  observedIdOverflow = false;
  memset(observedIdStats, 0, sizeof(observedIdStats));
  portEXIT_CRITICAL(&observedMux);
}

void noteObservedId(uint32_t id, uint32_t stampMs) {
  portENTER_CRITICAL(&observedMux);
  for (size_t i = 0; i < observedIdCount; ++i) {
    if (observedIdStats[i].id == id) {
      observedIdStats[i].count++;
      observedIdStats[i].last_ms = stampMs;
      portEXIT_CRITICAL(&observedMux);
      return;
    }
  }

  if (observedIdCount < MAX_OBSERVED_IDS) {
    ObservedIdStat& item = observedIdStats[observedIdCount++];
    item.id = id;
    item.count = 1;
    item.first_ms = stampMs;
    item.last_ms = stampMs;
  } else {
    observedIdOverflow = true;
  }
  portEXIT_CRITICAL(&observedMux);
}

void queueObservedIdDump() {
  ObservedIdStat snapshot[MAX_OBSERVED_IDS] = {};
  size_t snapshotCount = 0;
  bool overflow = false;

  portENTER_CRITICAL(&observedMux);
  snapshotCount = observedIdCount;
  overflow = observedIdOverflow;
  for (size_t i = 0; i < snapshotCount; ++i) {
    snapshot[i] = observedIdStats[i];
  }
  portEXIT_CRITICAL(&observedMux);

  for (size_t i = 0; i < snapshotCount; ++i) {
    for (size_t j = i + 1; j < snapshotCount; ++j) {
      if (snapshot[j].count > snapshot[i].count ||
          (snapshot[j].count == snapshot[i].count && snapshot[j].id < snapshot[i].id)) {
        const ObservedIdStat tmp = snapshot[i];
        snapshot[i] = snapshot[j];
        snapshot[j] = tmp;
      }
    }
  }

  pushConfigResponse(String("OBS_BEGIN=") + String(snapshotCount) + "," + String(overflow ? 1 : 0));
  for (size_t i = 0; i < snapshotCount; ++i) {
    char line[80];
    snprintf(line, sizeof(line), "OBS=0x%lX,%lu,%lu,%lu",
             static_cast<unsigned long>(snapshot[i].id),
             static_cast<unsigned long>(snapshot[i].count),
             static_cast<unsigned long>(snapshot[i].first_ms),
             static_cast<unsigned long>(snapshot[i].last_ms));
    pushConfigResponse(String(line));
  }
  pushConfigResponse(String("OBS_DONE=") + String(snapshotCount));
}

void resetPendingConfigSync() {
  pendingAllowCount = 0;
  pendingHighCount = 0;
  pendingAllowChunkCount = 0;
  pendingHighChunkCount = 0;
  pendingAllowChunkMask = 0;
  pendingHighChunkMask = 0;
  memset(pendingAllowIds, 0, sizeof(pendingAllowIds));
  memset(pendingHighIds, 0, sizeof(pendingHighIds));
}

void queueBleConfigDump() {
  copyRuntimeConfig(scratchAllowIds, scratchAllowCount, scratchHighIds, scratchHighCount, scratchFilterMode);
  pushConfigResponse(String("ROLE=A"));
  pushConfigResponse(String("FILTER=") + filterModeName(scratchFilterMode));
  pushConfigResponse(String("ALLOW=") + buildIdList(scratchAllowIds, scratchAllowCount));
  pushConfigResponse(String("HIGH=") + buildIdList(scratchHighIds, scratchHighCount));
  pushConfigResponse(String("CFG_DONE"));
}

void pushConfigResponse(const String& message) {
  updateUiStatusFromMessage(message);
  pushBleNotify(message);
  Serial.println(message);
}

bool storeConfigChunk(const ConfigChunkPacket* packet, int len) {
  const size_t headerSize = offsetof(ConfigChunkPacket, ids);
  if (len < static_cast<int>(headerSize + packet->item_count * sizeof(uint32_t))) {
    return false;
  }
  if (packet->chunk_count == 0 || packet->chunk_count >= 32 || packet->chunk_index >= packet->chunk_count) {
    return false;
  }

  uint32_t* targetIds = nullptr;
  size_t* targetCount = nullptr;
  uint8_t* targetChunkCount = nullptr;
  uint32_t* targetChunkMask = nullptr;

  if (packet->type == CONFIG_TYPE_ALLOW_CHUNK) {
    targetIds = pendingAllowIds;
    targetCount = &pendingAllowCount;
    targetChunkCount = &pendingAllowChunkCount;
    targetChunkMask = &pendingAllowChunkMask;
  } else if (packet->type == CONFIG_TYPE_HIGH_CHUNK) {
    targetIds = pendingHighIds;
    targetCount = &pendingHighCount;
    targetChunkCount = &pendingHighChunkCount;
    targetChunkMask = &pendingHighChunkMask;
  } else {
    return false;
  }

  if (*targetChunkCount == 0) {
    *targetChunkCount = packet->chunk_count;
  } else if (*targetChunkCount != packet->chunk_count) {
    return false;
  }

  const size_t offset = static_cast<size_t>(packet->chunk_index) * CONFIG_IDS_PER_PACKET;
  if (offset + packet->item_count > MAX_CONFIG_IDS) {
    return false;
  }

  memcpy(targetIds + offset, packet->ids, packet->item_count * sizeof(uint32_t));
  *targetCount = std::max(*targetCount, offset + packet->item_count);
  *targetChunkMask |= (1UL << packet->chunk_index);
  return true;
}

void sendConfigAck(uint8_t status, uint8_t filterMode, uint16_t allowCount, uint16_t highCount) {
  if (!hasPeer) {
    return;
  }
  ConfigAckPacket packet = {};
  packet.magic = CONFIG_MAGIC;
  packet.version = BRIDGE_VERSION;
  packet.source = NODE_ID;
  packet.type = CONFIG_TYPE_ACK;
  packet.status = status;
  packet.filter_mode = filterMode;
  packet.allow_count = allowCount;
  packet.high_count = highCount;
  esp_now_send(peerMac, reinterpret_cast<const uint8_t*>(&packet), sizeof(packet));
}

bool sendConfigChunks(uint8_t type, const uint32_t* ids, size_t count) {
  if (count == 0) {
    return true;
  }

  const uint8_t chunkCount = static_cast<uint8_t>((count + CONFIG_IDS_PER_PACKET - 1) / CONFIG_IDS_PER_PACKET);
  for (uint8_t chunkIndex = 0; chunkIndex < chunkCount; ++chunkIndex) {
    const size_t offset = static_cast<size_t>(chunkIndex) * CONFIG_IDS_PER_PACKET;
    const size_t remaining = count - offset;
    const size_t itemCount = std::min(remaining, CONFIG_IDS_PER_PACKET);
    ConfigChunkPacket packet = {};
    packet.magic = CONFIG_MAGIC;
    packet.version = BRIDGE_VERSION;
    packet.source = NODE_ID;
    packet.type = type;
    packet.chunk_index = chunkIndex;
    packet.chunk_count = chunkCount;
    packet.item_count = static_cast<uint8_t>(itemCount);
    memcpy(packet.ids, ids + offset, itemCount * sizeof(uint32_t));
    const size_t packetSize = offsetof(ConfigChunkPacket, ids) + itemCount * sizeof(uint32_t);
    if (esp_now_send(peerMac, reinterpret_cast<const uint8_t*>(&packet), packetSize) != ESP_OK) {
      return false;
    }
    delay(8);
  }
  return true;
}

bool syncRuntimeConfigToPeer() {
  if (!hasPeer || bleConfigOnlyMode) {
    pushConfigResponse("SYNC_SKIP=NO_PEER");
    return false;
  }

  uint32_t allowIds[MAX_CONFIG_IDS] = {};
  uint32_t highIds[MAX_CONFIG_IDS] = {};
  size_t allowCount = 0;
  size_t highCount = 0;
  uint8_t filterMode = FILTER_MODE_ALLOW_LIST;
  copyRuntimeConfig(allowIds, allowCount, highIds, highCount, filterMode);

  if (!sendConfigChunks(CONFIG_TYPE_ALLOW_CHUNK, allowIds, allowCount)) {
    pushConfigResponse("SYNC_ERR=ALLOW_SEND");
    return false;
  }
  if (!sendConfigChunks(CONFIG_TYPE_HIGH_CHUNK, highIds, highCount)) {
    pushConfigResponse("SYNC_ERR=HIGH_SEND");
    return false;
  }

  ConfigCommitPacket commit = {};
  commit.magic = CONFIG_MAGIC;
  commit.version = BRIDGE_VERSION;
  commit.source = NODE_ID;
  commit.type = CONFIG_TYPE_COMMIT;
  commit.filter_mode = filterMode;
  commit.allow_count = static_cast<uint16_t>(allowCount);
  commit.high_count = static_cast<uint16_t>(highCount);
  if (esp_now_send(peerMac, reinterpret_cast<const uint8_t*>(&commit), sizeof(commit)) != ESP_OK) {
    pushConfigResponse("SYNC_ERR=COMMIT_SEND");
    return false;
  }

  pushConfigResponse(String("SYNC_SENT=") + String(filterModeName(filterMode)) + "," + String(allowCount) + "," + String(highCount));
  return true;
}

class ConfigServerCallbacks : public BLEServerCallbacks {
 public:
  void onConnect(BLEServer* server, NimBLEConnInfo& connInfo) override {
    bleClientConnected = true;
    Serial.println("[BLE] client connected");
    if (server != nullptr) {
      server->updateConnParams(connInfo.getConnHandle(), 24, 48, 0, 180);
    }
  }

  void onDisconnect(BLEServer* server, NimBLEConnInfo&, int) override {
    bleClientConnected = false;
    Serial.println("[BLE] client disconnected");
    NimBLEDevice::startAdvertising();
  }
};

class ConfigRxCallbacks : public BLECharacteristicCallbacks {
 public:
  void onWrite(BLECharacteristic* characteristic, NimBLEConnInfo&) override {
    String value = characteristic->getValue();
    if (value.length() == 0) {
      return;
    }
    String command(value);
    command.trim();
    if (!command.isEmpty()) {
      pushBleCommand(command);
    }
  }
};

void processSerialCommands() {
  while (Serial.available() > 0) {
    const char c = static_cast<char>(Serial.read());
    if (c == '\r') {
      continue;
    }
    if (c == '\n') {
      serialCommandBuffer.trim();
      if (!serialCommandBuffer.isEmpty()) {
        noteUsbActivity("USB CMD");
        pushBleCommand(serialCommandBuffer);
      }
      serialCommandBuffer = "";
      continue;
    }
    if (serialCommandBuffer.length() < MAX_SERIAL_COMMAND_LEN) {
      serialCommandBuffer += c;
    }
  }
}

bool containsId(const uint32_t* ids, size_t count, uint32_t id) {
  if (count == 1 && ids[0] == 0x0) {
    return true;
  }
  for (size_t i = 0; i < count; ++i) {
    if (ids[i] == id) {
      return true;
    }
  }
  return false;
}

bool shouldBridgeId(uint32_t id) {
  bool matched = false;
  portENTER_CRITICAL(&configMux);
  matched = (runtimeFilterMode == FILTER_MODE_ALL) ||
            containsRuntimeId(runtimeAllowIds, runtimeAllowCount, id);
  portEXIT_CRITICAL(&configMux);
  return matched;
}

bool isHighPriorityId(uint32_t id) {
  bool matched = false;
  portENTER_CRITICAL(&configMux);
  matched = containsRuntimeId(runtimeHighIds, runtimeHighCount, id);
  portEXIT_CRITICAL(&configMux);
  return matched;
}

uint32_t frameSignature(const BridgeFrame& frame) {
  uint32_t sig = 2166136261u;
  const uint8_t* raw = reinterpret_cast<const uint8_t*>(&frame);
  for (size_t i = 0; i < sizeof(BridgeFrame) - sizeof(frame.stamp_ms); ++i) {
    sig ^= raw[i];
    sig *= 16777619u;
  }
  return sig;
}

void rememberEchoFrame(const BridgeFrame& frame) {
  static uint8_t slot = 0;
  echoCache[slot].signature = frameSignature(frame);
  echoCache[slot].expires_at = millis() + ECHO_SUPPRESS_MS;
  slot = (slot + 1) % (sizeof(echoCache) / sizeof(echoCache[0]));
}

bool isEchoedFrame(const BridgeFrame& frame) {
  const uint32_t sig = frameSignature(frame);
  const uint32_t now = millis();
  for (auto& entry : echoCache) {
    if (entry.signature == sig && static_cast<int32_t>(entry.expires_at - now) >= 0) {
      return true;
    }
  }
  return false;
}

void setLedColor(uint8_t r, uint8_t g, uint8_t b) {
  (void)r;
  (void)g;
  (void)b;
}

void refreshLed() {
  // S3R A-side uses the display for status; LED feedback is intentionally disabled.
}

void pulseSuccess() {
  ledSuccessUntilMs = millis() + LED_PULSE_MS;
}

void pulseError() {
  ledErrorUntilMs = millis() + LED_PULSE_MS;
}

void loadPeerMac() {
  prefs.begin("wirecan", true);
  const size_t len = prefs.getBytes("peer_mac", peerMac, sizeof(peerMac));
  prefs.end();
  hasPeer = (len == sizeof(peerMac) && !isZeroMac(peerMac));
}

void savePeerMac(const uint8_t* mac) {
  memcpy(peerMac, mac, sizeof(peerMac));
  hasPeer = true;
  prefs.begin("wirecan", false);
  prefs.putBytes("peer_mac", peerMac, sizeof(peerMac));
  prefs.end();
}

void ensurePeer(const uint8_t* mac) {
  esp_now_del_peer(mac);
  esp_now_peer_info_t peerInfo = {};
  memcpy(peerInfo.peer_addr, mac, 6);
  peerInfo.channel = 0;
  peerInfo.encrypt = false;
  esp_now_add_peer(&peerInfo);
}

void startPairMode() {
  if (pairMode) {
    return;
  }
  pairMode = true;
  pairStartMs = millis();
  lastPairBroadcastMs = 0;
  ensurePeer(BCAST_MAC);
  Serial.println("[PAIR] mode on");
}

void stopPairMode() {
  esp_now_del_peer(BCAST_MAC);
  pairMode = false;
  Serial.println("[PAIR] mode off");
}

void sendPairBroadcast() {
  if (!pairMode) {
    return;
  }
  if (millis() - pairStartMs > PAIR_TIMEOUT_MS) {
    stopPairMode();
    Serial.println("[PAIR] timeout");
    return;
  }
  if (millis() - lastPairBroadcastMs < 1000) {
    return;
  }
  lastPairBroadcastMs = millis();

  uint8_t myMac[6];
  esp_wifi_get_mac(WIFI_IF_STA, myMac);
  uint8_t packet[21];
  memcpy(packet, "CANBRIDGE_PAIR:", 15);
  memcpy(packet + 15, myMac, 6);
  esp_now_send(BCAST_MAC, packet, sizeof(packet));
}

void onDataSent(const wifi_tx_info_t*, esp_now_send_status_t status) {
  if (status == ESP_NOW_SEND_SUCCESS) {
    pulseSuccess();
  }
}

bool enqueueCanTx(const BridgeFrame& frame) {
  if (!canTxQueue.push(frame)) {
    stats.now_drop_total++;
    pulseError();
    return false;
  }
  stats.now_rx_total++;
  stats.now_rx_sec++;
  pulseSuccess();
  return true;
}

void onDataRecv(const esp_now_recv_info_t*, const uint8_t* data, int len) {
  if (len >= 20 && memcmp(data, "CANBRIDGE_ACK:", 14) == 0 && pairMode) {
    savePeerMac(data + 14);
    stopPairMode();
    ensurePeer(peerMac);
    Serial.printf("[PAIR] ack from %s\n", macToString(peerMac).c_str());
    pulseSuccess();
    return;
  }

  if (len >= 21 && memcmp(data, "CANBRIDGE_PAIR:", 15) == 0 && pairMode) {
    savePeerMac(data + 15);
    stopPairMode();
    ensurePeer(peerMac);
    uint8_t myMac[6];
    esp_wifi_get_mac(WIFI_IF_STA, myMac);
    uint8_t ack[20];
    memcpy(ack, "CANBRIDGE_ACK:", 14);
    memcpy(ack + 14, myMac, 6);
    esp_now_send(peerMac, ack, sizeof(ack));
    Serial.printf("[PAIR] paired with %s\n", macToString(peerMac).c_str());
    pulseSuccess();
    return;
  }

  if (len >= static_cast<int>(sizeof(uint16_t))) {
    const uint16_t magic = *reinterpret_cast<const uint16_t*>(data);
    if (magic == CONFIG_MAGIC) {
      if (len < 5) {
        return;
      }

      const uint8_t type = data[4];
      if ((type == CONFIG_TYPE_ALLOW_CHUNK || type == CONFIG_TYPE_HIGH_CHUNK) &&
          len >= static_cast<int>(offsetof(ConfigChunkPacket, ids))) {
        const auto* packet = reinterpret_cast<const ConfigChunkPacket*>(data);
        if (packet->version != BRIDGE_VERSION || packet->source == NODE_ID) {
          return;
        }
        if (!storeConfigChunk(packet, len)) {
          sendConfigAck(CONFIG_ACK_INVALID, FILTER_MODE_ALLOW_LIST, 0, 0);
          pushConfigResponse("SYNC_ERR=CHUNK_INVALID");
          resetPendingConfigSync();
          return;
        }
        return;
      }

      if (type == CONFIG_TYPE_COMMIT && len >= static_cast<int>(sizeof(ConfigCommitPacket))) {
        const auto* packet = reinterpret_cast<const ConfigCommitPacket*>(data);
        if (packet->version != BRIDGE_VERSION || packet->source == NODE_ID) {
          return;
        }

        const bool allowReady = (packet->allow_count == 0) ||
                                (pendingAllowChunkCount > 0 && allConfigChunksReceived(pendingAllowChunkMask, pendingAllowChunkCount));
        const bool highReady = (packet->high_count == 0) ||
                               (pendingHighChunkCount > 0 && allConfigChunksReceived(pendingHighChunkMask, pendingHighChunkCount));
        if (!allowReady || !highReady || pendingAllowCount < packet->allow_count || pendingHighCount < packet->high_count) {
          sendConfigAck(CONFIG_ACK_INCOMPLETE, packet->filter_mode, static_cast<uint16_t>(pendingAllowCount), static_cast<uint16_t>(pendingHighCount));
          pushConfigResponse("SYNC_ERR=INCOMPLETE");
          resetPendingConfigSync();
          return;
        }

        setRuntimeConfig(pendingAllowIds, packet->allow_count, pendingHighIds, packet->high_count, packet->filter_mode);
        saveRuntimeConfigToPrefs();
        sendConfigAck(CONFIG_ACK_OK, packet->filter_mode, packet->allow_count, packet->high_count);
        pushConfigResponse(String("SYNC_APPLIED=") + String(filterModeName(packet->filter_mode)) + "," + String(packet->allow_count) + "," + String(packet->high_count));
        resetPendingConfigSync();
        queueBleConfigDump();
        return;
      }

      if (type == CONFIG_TYPE_ACK && len >= static_cast<int>(sizeof(ConfigAckPacket))) {
        const auto* packet = reinterpret_cast<const ConfigAckPacket*>(data);
        if (packet->version != BRIDGE_VERSION || packet->source == NODE_ID) {
          return;
        }
        if (packet->status == CONFIG_ACK_OK) {
          pushConfigResponse(String("SYNC_OK=") + String(filterModeName(packet->filter_mode)) + "," + String(packet->allow_count) + "," + String(packet->high_count));
        } else if (packet->status == CONFIG_ACK_INCOMPLETE) {
          pushConfigResponse(String("SYNC_ERR=REMOTE_INCOMPLETE:") + String(filterModeName(packet->filter_mode)) + "," + String(packet->allow_count) + "," + String(packet->high_count));
        } else {
          pushConfigResponse("SYNC_ERR=REMOTE_INVALID");
        }
        return;
      }
      return;
    }
  }

  if (len < static_cast<int>(offsetof(BridgePacket, frames))) {
    return;
  }

  const auto* packet = reinterpret_cast<const BridgePacket*>(data);
  if (packet->magic != BRIDGE_MAGIC || packet->version != BRIDGE_VERSION || packet->source == NODE_ID) {
    return;
  }
  if (packet->count == 0 || packet->count > MAX_FRAMES_PER_PACKET) {
    return;
  }
  if (len < static_cast<int>(offsetof(BridgePacket, frames) + packet->count * sizeof(BridgeFrame))) {
    return;
  }

  for (uint8_t i = 0; i < packet->count; ++i) {
    enqueueCanTx(packet->frames[i]);
  }
}

bool getCanStatus(twai_status_info_t& info) {
  memset(&info, 0, sizeof(info));
  return twai_get_status_info(&info) == ESP_OK;
}

const char* busStateName() {
  twai_status_info_t info = {};
  if (!getCanStatus(info)) {
    return "INIT";
  }
  if (info.state == TWAI_STATE_BUS_OFF) return "BUS-OFF";
  if (info.state == TWAI_STATE_RECOVERING) return "RECOVER";
  if (info.tx_error_counter >= 128 || info.rx_error_counter >= 128) return "PASSIVE";
  if (info.tx_error_counter >= 96 || info.rx_error_counter >= 96) return "WARN";
  return "OK";
}

void displayStatus() {
  auto& disp = M5.Display;
  disp.fillScreen(TFT_BLACK);
  disp.fillRect(0, 0, 128, 30, ROLE_HEADER_COLOR);
  disp.setTextSize(3);
  disp.setTextColor(ROLE_TEXT_COLOR, ROLE_HEADER_COLOR);
  disp.setCursor(6, 4);
  disp.print(ROLE_LABEL);
  disp.setTextSize(2);
  disp.setCursor(38, 7);
  disp.print(hasPeer ? "LINK" : "WAIT");
  disp.setCursor(88, 7);
  disp.print(runtimeFilterMode == FILTER_MODE_ALL ? "ALL" : "ALW");

  uint16_t bannerBg = usbRecentlyActive() ? TFT_CYAN : TFT_DARKGREY;
  uint16_t bannerFg = usbRecentlyActive() ? TFT_BLACK : TFT_WHITE;
  String bannerText = usbRecentlyActive() ? "USB ONLINE" : "USB ----";
  if (millis() < uiStatusUntilMs) {
    bannerBg = uiStatusBgColor;
    bannerFg = uiStatusTextColor;
    bannerText = uiStatusText;
  }
  disp.fillRect(0, 34, 128, 18, bannerBg);
  disp.setTextSize(2);
  disp.setTextColor(bannerFg, bannerBg);
  disp.setCursor(4, 36);
  disp.print(bannerText);

  disp.setTextSize(1);
  disp.setTextColor(TFT_WHITE, TFT_BLACK);
  disp.setCursor(0, 58);
  disp.printf("BUS:%-7s DROP:%lu/%lu    ", busStateName(), stats.can_drop_total, stats.now_drop_total);
  disp.setCursor(0, 72);
  disp.printf("CAN>NW:%4lu/s            ", canRxPerSec);
  disp.setCursor(0, 86);
  disp.printf("NW>CAN:%4lu/s            ", canTxPerSec);
  disp.setCursor(0, 100);
  disp.printf("Q:%u/%u/%u               ",
              static_cast<unsigned>(highQueue.count()),
              static_cast<unsigned>(lowQueue.count()),
              static_cast<unsigned>(canTxQueue.count()));
  disp.setCursor(0, 114);
  disp.setTextColor(hasPeer ? TFT_GREEN : TFT_YELLOW, TFT_BLACK);
  disp.printf("PAIR:%s  BLE:%s          ", hasPeer ? "OK" : "WAIT", bleClientConnected ? "ON" : "OFF");
}

void displayConfigMode() {
  auto& disp = M5.Display;
  disp.fillScreen(TFT_BLACK);
  disp.setCursor(0, 0);
  disp.setTextColor(TFT_CYAN, TFT_BLACK);
  disp.printf("A BLE CFG MODE");
  disp.setCursor(0, 18);
  disp.setTextColor(TFT_WHITE, TFT_BLACK);
  disp.printf("NAME:%s", BLE_DEVICE_NAME);
  disp.setCursor(0, 34);
  disp.printf("WEBからBLE接続");
  disp.setCursor(0, 50);
  disp.printf("M5読込 / M5書込");
  disp.setCursor(0, 66);
  disp.setTextColor(bleClientConnected ? TFT_GREEN : TFT_YELLOW, TFT_BLACK);
  disp.printf("BLE:%s", bleClientConnected ? "CONNECTED" : "WAIT");
  disp.setCursor(0, 82);
  disp.setTextColor(TFT_WHITE, TFT_BLACK);
  disp.printf("再起動で通常復帰");
}

bool installAndStartCan() {
  twai_general_config_t gConfig = TWAI_GENERAL_CONFIG_DEFAULT(CAN_TX_PIN, CAN_RX_PIN, TWAI_MODE_NORMAL);
  twai_timing_config_t tConfig = TWAI_TIMING_CONFIG_1MBITS();
  twai_filter_config_t fConfig = TWAI_FILTER_CONFIG_ACCEPT_ALL();
  gConfig.tx_queue_len = 64;
  gConfig.rx_queue_len = 128;

  if (twai_driver_install(&gConfig, &tConfig, &fConfig) != ESP_OK) {
    return false;
  }
  if (twai_start() != ESP_OK) {
    twai_driver_uninstall();
    return false;
  }
  return true;
}

void restartCanDriver() {
  twai_stop();
  twai_driver_uninstall();
  delay(10);

  if (installAndStartCan()) {
    canRecoveryActive = false;
    canRecoveryStartedMs = 0;
    lastCanRecoveryAttemptMs = millis();
    canRestartCount++;
    canTxResumeAtMs = millis() + CAN_TX_RESUME_DELAY_MS;
    Serial.println("[CAN] driver restart OK");
  } else {
    Serial.println("[CAN] driver restart FAIL");
  }
}

void requestCanRecovery(const char* reason) {
  const uint32_t now = millis();
  if (canRecoveryActive || now - lastCanRecoveryAttemptMs < CAN_RECOVERY_RETRY_MS) {
    return;
  }

  lastCanRecoveryAttemptMs = now;
  canRecoveryStartedMs = now;
  canRecoveryActive = true;
  canRecoveryCount++;
  Serial.printf("[CAN] recovery start (%s)\n", reason);

  if (twai_initiate_recovery() != ESP_OK) {
    Serial.println("[CAN] recovery request failed, restarting driver");
    restartCanDriver();
  }
}

bool waitForCanReady(uint32_t timeoutMs) {
  const uint32_t start = millis();
  while (millis() - start < timeoutMs) {
    twai_status_info_t info = {};
    if (getCanStatus(info)) {
      const bool ready = info.state != TWAI_STATE_BUS_OFF &&
                         info.state != TWAI_STATE_RECOVERING &&
                         info.tx_error_counter < 128 &&
                         info.rx_error_counter < 128;
      if (ready) {
        if (millis() < canTxResumeAtMs) {
          vTaskDelay(pdMS_TO_TICKS(10));
          continue;
        }
        canFaultSinceMs = 0;
        return true;
      }
      if (canFaultSinceMs == 0) {
        canFaultSinceMs = millis();
      }
      if (info.state == TWAI_STATE_BUS_OFF) {
        requestCanRecovery("tx wait");
      }
    }
    vTaskDelay(pdMS_TO_TICKS(10));
  }
  return false;
}

bool shouldLogCanError() {
  const uint32_t now = millis();
  if (now - lastCanErrorLogMs < CAN_ERROR_LOG_INTERVAL_MS) {
    return false;
  }
  lastCanErrorLogMs = now;
  return true;
}

void purgeCanTxQueue() {
  BridgeFrame dropped = {};
  size_t purged = 0;
  while (canTxQueue.pop(dropped)) {
    ++purged;
  }
  if (purged > 0) {
    stats.can_drop_total += purged;
    canQueuePurgeCount += purged;
    Serial.printf("[CAN] purged %u queued frames\n", static_cast<unsigned>(purged));
  }
}

BridgeFrame makeBridgeFrame(const twai_message_t& msg) {
  BridgeFrame frame = {};
  frame.id = msg.identifier;
  frame.dlc = msg.data_length_code;
  if (msg.extd) frame.flags |= 0x01;
  if (msg.rtr) frame.flags |= 0x02;
  memcpy(frame.data, msg.data, msg.data_length_code);
  frame.stamp_ms = static_cast<uint16_t>(millis());
  return frame;
}

bool sendPacket(FrameRing& queue, bool highPriority) {
  if (!hasPeer) {
    return false;
  }

  BridgePacket packet = {};
  packet.magic = BRIDGE_MAGIC;
  packet.version = BRIDGE_VERSION;
  packet.source = NODE_ID;
  packet.sequence = sequenceNumber++;

  while (packet.count < MAX_FRAMES_PER_PACKET) {
    BridgeFrame frame;
    if (!queue.pop(frame)) {
      break;
    }
    packet.frames[packet.count++] = frame;
  }
  if (packet.count == 0) {
    return false;
  }

  const size_t packetSize = offsetof(BridgePacket, frames) + packet.count * sizeof(BridgeFrame);
  const esp_err_t result = esp_now_send(peerMac, reinterpret_cast<const uint8_t*>(&packet), packetSize);
  if (result == ESP_OK) {
    stats.now_tx_total += packet.count;
    stats.now_tx_sec += packet.count;
    pulseSuccess();
  } else {
    stats.now_drop_total += packet.count;
    pulseError();
  }

  if (highPriority) lastHighSendMs = millis();
  else lastLowSendMs = millis();
  return result == ESP_OK;
}

void taskCanRx(void*) {
  twai_message_t msg;
  while (true) {
    if (twai_receive(&msg, pdMS_TO_TICKS(1)) != ESP_OK) {
      continue;
    }

    BridgeFrame frame = makeBridgeFrame(msg);
    if (isEchoedFrame(frame)) {
      stats.echo_drop_total++;
      continue;
    }
    noteObservedId(frame.id, millis());
    if (!shouldBridgeId(frame.id)) continue;

    FrameRing& target = isHighPriorityId(frame.id) ? highQueue : lowQueue;
    if (!target.push(frame)) {
      stats.can_drop_total++;
      pulseError();
      continue;
    }
    stats.can_rx_total++;
    stats.can_rx_sec++;
  }
}

void taskWirelessTx(void*) {
  while (true) {
    const uint32_t now = millis();
    if (highQueue.count() >= HIGH_BURST_TRIGGER || (highQueue.count() > 0 && now - lastHighSendMs >= HIGH_FLUSH_MS)) {
      sendPacket(highQueue, true);
    }
    if (lowQueue.count() >= LOW_BURST_TRIGGER || (lowQueue.count() > 0 && now - lastLowSendMs >= LOW_FLUSH_MS)) {
      sendPacket(lowQueue, false);
    }
    vTaskDelay(pdMS_TO_TICKS(2));
  }
}

void taskCanTx(void*) {
  while (true) {
    BridgeFrame frame;
    if (!canTxQueue.pop(frame)) {
      vTaskDelay(pdMS_TO_TICKS(1));
      continue;
    }

    twai_message_t msg = {};
    msg.identifier = frame.id;
    msg.extd = (frame.flags & 0x01) != 0;
    msg.rtr = (frame.flags & 0x02) != 0;
    msg.data_length_code = frame.dlc;
    memcpy(msg.data, frame.data, frame.dlc);

    bool sent = false;
    for (uint8_t attempt = 0; attempt < 3 && !sent; ++attempt) {
      if (!waitForCanReady(CAN_READY_WAIT_MS)) {
        if (canFaultSinceMs != 0 && millis() - canFaultSinceMs >= CAN_QUEUE_PURGE_MS &&
            canTxQueue.count() >= CAN_QUEUE_PURGE_THRESHOLD) {
          purgeCanTxQueue();
        }
        break;
      }

      rememberEchoFrame(frame);
      if (twai_transmit(&msg, pdMS_TO_TICKS(5)) == ESP_OK) {
        stats.can_tx_total++;
        stats.can_tx_sec++;
        pulseSuccess();
        sent = true;
        break;
      }

      twai_status_info_t info = {};
      getCanStatus(info);
      const char* state = busStateName();
      if (shouldLogCanError()) {
        Serial.printf("[CAN-TX-ERR] id=0x%03lX state=%s retry=%u\n", frame.id, state, attempt + 1);
      }
      if (strcmp(state, "BUS-OFF") == 0 || strcmp(state, "RECOVER") == 0) {
        requestCanRecovery("tx error");
      }
      if (info.tx_error_counter >= 128 || info.rx_error_counter >= 128) {
        vTaskDelay(pdMS_TO_TICKS(CAN_PASSIVE_BACKOFF_MS));
      } else {
        vTaskDelay(pdMS_TO_TICKS(20));
      }
    }

    if (!sent) {
      stats.can_drop_total++;
      pulseError();
    }
  }
}

void taskCanHealth(void*) {
  while (true) {
    twai_status_info_t info = {};
    if (getCanStatus(info)) {
      const bool faulted = info.state == TWAI_STATE_BUS_OFF || info.state == TWAI_STATE_RECOVERING ||
                           info.tx_error_counter >= 128 || info.rx_error_counter >= 128;
      if (faulted && canFaultSinceMs == 0) {
        canFaultSinceMs = millis();
      }
      if (!faulted) {
        canFaultSinceMs = 0;
      }

      if (info.state == TWAI_STATE_BUS_OFF) {
        requestCanRecovery("bus off");
      } else if (canRecoveryActive && info.state == TWAI_STATE_STOPPED) {
        if (twai_start() == ESP_OK) {
          canRecoveryActive = false;
          canRecoveryStartedMs = 0;
          canRestartCount++;
          canFaultSinceMs = 0;
          canTxResumeAtMs = millis() + CAN_TX_RESUME_DELAY_MS;
          Serial.println("[CAN] recovery complete, bus restarted");
        } else {
          Serial.println("[CAN] twai_start failed after recovery");
          restartCanDriver();
        }
      } else if (canRecoveryActive && millis() - canRecoveryStartedMs > CAN_RECOVERY_TIMEOUT_MS) {
        Serial.println("[CAN] recovery timeout, forcing driver restart");
        restartCanDriver();
      }

      if (canFaultSinceMs != 0 && millis() - canFaultSinceMs >= CAN_QUEUE_PURGE_MS &&
          canTxQueue.count() >= CAN_QUEUE_PURGE_THRESHOLD) {
        purgeCanTxQueue();
      }
    }
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

void initEspNow() {
  WiFi.mode(WIFI_STA);
  WiFi.disconnect();
  esp_wifi_set_promiscuous(false);
  esp_wifi_set_channel(1, WIFI_SECOND_CHAN_NONE);
  esp_now_init();
  esp_now_register_send_cb(onDataSent);
  esp_now_register_recv_cb(onDataRecv);
  loadPeerMac();
  if (hasPeer) {
    ensurePeer(peerMac);
  }
}

void initCan() {
  if (!installAndStartCan()) {
    Serial.println("[CAN] init failed");
  } else {
    canTxResumeAtMs = millis() + CAN_TX_RESUME_DELAY_MS;
  }
}

void initBleConfig() {
  BLEDevice::init(BLE_DEVICE_NAME);
  bleServer = BLEDevice::createServer();
  bleServer->setCallbacks(new ConfigServerCallbacks());

  BLEService* service = bleServer->createService(BLE_SERVICE_UUID);
  bleTxCharacteristic = service->createCharacteristic(BLE_TX_UUID, NIMBLE_PROPERTY::NOTIFY);

  bleRxCharacteristic = service->createCharacteristic(BLE_RX_UUID, NIMBLE_PROPERTY::WRITE);
  bleRxCharacteristic->setCallbacks(new ConfigRxCallbacks());

  service->start();
  BLEAdvertising* advertising = BLEDevice::getAdvertising();
  advertising->setName(BLE_DEVICE_NAME);
  advertising->addServiceUUID(BLE_SERVICE_UUID);
  advertising->enableScanResponse(true);
  advertising->start();
  Serial.printf("[BLE] config ready: %s\n", BLE_DEVICE_NAME);
}

void processBleNotifications() {
  if (!bleClientConnected || bleTxCharacteristic == nullptr) {
    return;
  }

  String message;
  if (!popBleNotify(message)) {
    return;
  }

  bleTxCharacteristic->setValue(message.c_str());
  bleTxCharacteristic->notify();
}

void processBleCommands() {
  String command;
  if (!popBleCommand(command)) {
    return;
  }

  Serial.printf("[BLE] cmd: %s\n", command.c_str());

  if (command.equalsIgnoreCase("PING")) {
    pushConfigResponse("PONG");
    return;
  }

  if (command.equalsIgnoreCase("REQUEST_CFG") || command.equalsIgnoreCase("REQUEST_STATE")) {
    queueBleConfigDump();
    if (command.equalsIgnoreCase("REQUEST_STATE")) {
      pushConfigResponse(String("STATE=peer:") + (hasPeer ? macToString(peerMac) : String("--")) +
                         ",bus:" + String(busStateName()) + ",filter:" + String(filterModeName(runtimeFilterMode)));
    }
    return;
  }

  if (command.equalsIgnoreCase("OBS_CLEAR")) {
    clearObservedIds();
    pushConfigResponse("OBS_CLEAR");
    return;
  }

  if (command.equalsIgnoreCase("OBS_DUMP")) {
    queueObservedIdDump();
    return;
  }

  if (command.equalsIgnoreCase("SAVE_CFG")) {
    saveRuntimeConfigToPrefs();
    pushConfigResponse("CFG_SAVED");
    queueBleConfigDump();
    syncRuntimeConfigToPeer();
    return;
  }

  if (command.equalsIgnoreCase("RESET_CFG")) {
    copyDefaultConfigToRuntime();
    saveRuntimeConfigToPrefs();
    pushConfigResponse("CFG_RESET");
    queueBleConfigDump();
    syncRuntimeConfigToPeer();
    return;
  }

  if (command.startsWith("SET_ALLOW=")) {
    uint32_t allowIds[MAX_CONFIG_IDS] = {};
    uint32_t highIds[MAX_CONFIG_IDS] = {};
    size_t allowCount = 0;
    size_t highCount = 0;
    if (!parseIdList(command.substring(10), allowIds, allowCount)) {
      pushConfigResponse("ERR=ALLOW_PARSE");
      return;
    }
    uint32_t currentAllowIds[MAX_CONFIG_IDS] = {};
    size_t currentAllowCount = 0;
    uint8_t currentFilterMode = FILTER_MODE_ALLOW_LIST;
    copyRuntimeConfig(currentAllowIds, currentAllowCount, highIds, highCount, currentFilterMode);
    setRuntimeConfig(allowIds, allowCount, highIds, highCount, currentFilterMode);
    pushConfigResponse(String("ALLOW_OK=") + String(allowCount));
    return;
  }

  if (command.startsWith("SET_HIGH=")) {
    uint32_t allowIds[MAX_CONFIG_IDS] = {};
    uint32_t highIds[MAX_CONFIG_IDS] = {};
    size_t allowCount = 0;
    size_t highCount = 0;
    uint8_t currentFilterMode = FILTER_MODE_ALLOW_LIST;
    copyRuntimeConfig(allowIds, allowCount, highIds, highCount, currentFilterMode);
    if (!parseIdList(command.substring(9), highIds, highCount)) {
      pushConfigResponse("ERR=HIGH_PARSE");
      return;
    }
    setRuntimeConfig(allowIds, allowCount, highIds, highCount, currentFilterMode);
    pushConfigResponse(String("HIGH_OK=") + String(highCount));
    return;
  }

  if (command.startsWith("SET_FILTER=")) {
    String modeText = command.substring(11);
    modeText.trim();
    modeText.toUpperCase();
    uint32_t allowIds[MAX_CONFIG_IDS] = {};
    uint32_t highIds[MAX_CONFIG_IDS] = {};
    size_t allowCount = 0;
    size_t highCount = 0;
    uint8_t currentFilterMode = FILTER_MODE_ALLOW_LIST;
    copyRuntimeConfig(allowIds, allowCount, highIds, highCount, currentFilterMode);

    uint8_t nextMode = currentFilterMode;
    if (modeText == "ALL" || modeText == "OFF") {
      nextMode = FILTER_MODE_ALL;
    } else if (modeText == "ALLOW" || modeText == "ALLOW_LIST" || modeText == "ON") {
      nextMode = FILTER_MODE_ALLOW_LIST;
    } else {
      pushConfigResponse("ERR=FILTER_PARSE");
      return;
    }

    setRuntimeConfig(allowIds, allowCount, highIds, highCount, nextMode);
    pushConfigResponse(String("FILTER_OK=") + filterModeName(nextMode));
    return;
  }

  pushConfigResponse(String("ERR=UNKNOWN:") + command);
}

}  // namespace

void setup() {
  auto cfg = M5.config();
  M5.begin(cfg);
  Serial.begin(115200);
  M5.Display.setTextSize(1);
  M5.Display.clear(TFT_BLACK);
  setLedColor(0, 0, 0);

  loadRuntimeConfigFromPrefs();

  const uint32_t detectStart = millis();
  while (millis() - detectStart < CONFIG_MODE_HOLD_MS) {
    M5.update();
    if (!M5.BtnA.isPressed()) {
      break;
    }
    delay(10);
  }
  M5.update();
  bleConfigOnlyMode = M5.BtnA.isPressed() && (millis() - detectStart >= CONFIG_MODE_HOLD_MS);

  if (bleConfigOnlyMode) {
    initBleConfig();
    Serial.println("[BOOT] BLE config-only mode");
    displayConfigMode();
    setLedColor(0, 0, 32);
    return;
  }

  highQueue.begin(HIGH_QUEUE_CAPACITY, true);
  lowQueue.begin(LOW_QUEUE_CAPACITY, true);
  canTxQueue.begin(CAN_TX_QUEUE_CAPACITY, true);
  initEspNow();
  initCan();

  Serial.println("[BOOT] S3R A bridge start");
  Serial.printf("[BOOT] peer=%s\n", hasPeer ? macToString(peerMac).c_str() : "--");

  xTaskCreatePinnedToCore(taskCanRx, "can_rx", 4096, nullptr, 5, nullptr, 0);
  xTaskCreatePinnedToCore(taskWirelessTx, "now_tx", 4096, nullptr, 4, nullptr, 1);
  xTaskCreatePinnedToCore(taskCanTx, "can_tx", 4096, nullptr, 4, nullptr, 1);
  xTaskCreatePinnedToCore(taskCanHealth, "can_health", 4096, nullptr, 3, nullptr, 1);
}

void loop() {
  M5.update();
  const uint32_t now = millis();

  if (bleConfigOnlyMode) {
    processSerialCommands();
    processBleCommands();
    processBleNotifications();
    if (now - lastDisplayMs >= DISPLAY_INTERVAL_MS) {
      displayConfigMode();
      lastDisplayMs = now;
    }
    delay(5);
    return;
  }

  if (M5.BtnA.isPressed()) {
    if (buttonPressMs == 0) buttonPressMs = now;
    else if (now - buttonPressMs > 3000) {
      buttonPressMs = 0;
      startPairMode();
    }
  } else {
    buttonPressMs = 0;
  }

  sendPairBroadcast();
  processSerialCommands();
  processBleCommands();
  processBleNotifications();
  refreshLed();

  if (now - lastStatsMs >= STATS_INTERVAL_MS) {
    canRxPerSec = stats.can_rx_sec;
    canTxPerSec = stats.can_tx_sec;
    stats.can_rx_sec = 0;
    stats.can_tx_sec = 0;
    stats.now_rx_sec = 0;
    stats.now_tx_sec = 0;
    lastStatsMs = now;
    Serial.printf("A can>now=%lu now>can=%lu drop(can=%lu now=%lu echo=%lu purge=%lu) q=%u/%u/%u bus=%s rec=%lu rst=%lu peer=%s\n",
                  canRxPerSec,
                  canTxPerSec,
                  stats.can_drop_total,
                  stats.now_drop_total,
                  stats.echo_drop_total,
                  canQueuePurgeCount,
                  static_cast<unsigned>(highQueue.count()),
                  static_cast<unsigned>(lowQueue.count()),
                  static_cast<unsigned>(canTxQueue.count()),
                  busStateName(),
                  canRecoveryCount,
                  canRestartCount,
                  hasPeer ? macToString(peerMac).c_str() : "--");
  }

  if (now - lastDisplayMs >= DISPLAY_INTERVAL_MS) {
    displayStatus();
    lastDisplayMs = now;
  }

  delay(5);
}









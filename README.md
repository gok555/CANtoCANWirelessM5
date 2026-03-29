
[README.md](https://github.com/user-attachments/files/26327570/README.md)
# CAN Bridge Config Web

Browser tool for the M5 AtomS3R A/B CAN wireless bridge.

This app is intended for:

- importing IDs from trace files
- importing IDs from live CAN traffic through unit A over USB
- building `Allow` and `High` lists
- saving and loading JSON settings
- writing settings to unit A over USB
- syncing the same settings from A to B

## Files

- `index.html`
  - main web app
- `app.js`
  - UI logic, import logic, USB/BLE commands
- `styles.css`
  - page styling
- `manual.html`
  - browser manual for GitHub Pages
- `bridge_ids.sample.json`
  - sample config file
- `.nojekyll`
  - GitHub Pages support

## Recommended workflow

1. Connect unit A by USB.
2. Set filter mode to `ALL` when you want to capture real traffic.
3. Import IDs from one of these sources:
   - `Import Trace`
   - `USB Import Live`
4. Build the actual config:
   - `Trace -> Strict` for exact IDs from the latest capture
   - `Auto By Count` to rank by frame count
   - `Auto By Rate` to rank by update rate
5. Review `Allow` and `High`.
6. Use `USB Write` to write the config to unit A.
7. Let unit A sync the config to unit B.

## Live import notes

- `USB Clear Live` clears the observed ID buffer inside unit A.
- `USB Import Live` imports only IDs observed after the last clear.
- If the app shows `No live IDs captured`, unit A did not observe any CAN frames in that capture window.
- For reliable live capture, connect unit A directly to the same CAN point you want to observe.

## Current scope

The basic workflow is already in place:

- trace import
- live import
- strict config generation
- USB read/write
- A to B config sync

Wireless security settings are still future work:

- channel
- network ID
- shared key

## Publish to GitHub Pages

This folder is ready to publish as a static site.

Typical publish flow:

1. Put the contents of `BridgeConfigToolWeb` into the GitHub Pages repo root, or into a `docs/` folder.
2. Commit and push.
3. In GitHub Pages settings, publish from the selected branch and folder.

If `index.html` is the site entry point, `manual.html` will be available as a normal page under the same site.

## Manual

Open:

- `manual.html`

Or use the link shown at the top of the web app.

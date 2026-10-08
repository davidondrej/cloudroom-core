---
name: room-cli
description: 'Inspect and control the user’s Cloudroom threads, projects, and settings from a cloud thread. Use for Cloudroom CLI tasks, not official BB.'
---

# Cloudroom from the cloud

- Here, `cloudroom thread update --self --title TITLE` and `cloudroom thread archive --self` rename or archive this thread. `cloudroom thread update --self --reasoning-level LEVEL` changes this thread's effort from its next turn; its model stays fixed.
- Start a subagent with `cloudroom thread spawn --provider codex|claude-code|pi|opencode [--model MODEL] [--reasoning-level LEVEL] [--title TITLE] --prompt TEXT`. It runs in this sandbox and folder, and shows under this thread. Pi and OpenCode models are `provider/model`, e.g. `openrouter/deepseek/deepseek-v4-pro` or `opencode-go/kimi-k3`. Cloudroom messages you each time it finishes, so keep working. `cloudroom thread list`, `output CHILD_ID`, and `tell CHILD_ID TEXT` check on and steer your children; `stop CHILD_ID` stops one and `archive CHILD_ID` stops and archives it; `thread list` hides archived children unless you add `--include-archived`. Never start threads through Mac access.
- Everything else (other threads, projects, settings, plugins) lives in the Cloudroom app on the user’s Mac. Run `room-cli` there through Mac access (see the `cloud-mac` skill):

  ```sh
  cloudroom mac run 'ELECTRON_RUN_AS_NODE=1 /Applications/Cloudroom.app/Contents/MacOS/Cloudroom /Applications/Cloudroom.app/Contents/Resources/app.asar.unpacked/node_modules/bb-app/host-daemon/dist/room-cli thread list --json'
  ```

- Start with `room-cli --help` or `room-cli guide`. Prefer `--json`.
- `Mac unavailable` means the Mac is offline or Mac access is off. Tell the user.
- Never inspect, message, or launch other threads unless the user asks.

#!/usr/bin/env bash
# Installs the kavak-databricks plugin into Claude's plugin system.
# Run after editing any SKILL.md or after cloning/updating the repo.
set -e

BUNDLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_NAME="kavak-databricks"
MARKETPLACE="local"
VERSION="1.0.0"
INSTALL_PATH="$HOME/.claude/plugins/cache/$MARKETPLACE/$PLUGIN_NAME/$VERSION"
INSTALLED_JSON="$HOME/.claude/plugins/installed_plugins.json"

# Copy plugin files to cache path
rm -rf "$INSTALL_PATH"
mkdir -p "$INSTALL_PATH/.claude-plugin"
mkdir -p "$INSTALL_PATH/skills"

cp "$BUNDLE_DIR/.claude-plugin/plugin.json" "$INSTALL_PATH/.claude-plugin/plugin.json"

for skill_dir in "$BUNDLE_DIR/skills"/*/; do
    skill_name="$(basename "$skill_dir")"
    mkdir -p "$INSTALL_PATH/skills/$skill_name"
    cp "$skill_dir/SKILL.md" "$INSTALL_PATH/skills/$skill_name/SKILL.md"
    echo "Copied: $skill_name"
done

# Register in installed_plugins.json
python3 - <<PYEOF
import json, os, datetime

path = os.path.expanduser("$INSTALLED_JSON")
with open(path) as f:
    data = json.load(f)

key = "${PLUGIN_NAME}@${MARKETPLACE}"
entry = {
    "scope": "user",
    "installPath": "$INSTALL_PATH",
    "version": "$VERSION",
    "installedAt": datetime.datetime.utcnow().isoformat() + "Z",
    "lastUpdated": datetime.datetime.utcnow().isoformat() + "Z",
    "gitCommitSha": ""
}
data["plugins"][key] = [entry]

with open(path, "w") as f:
    json.dump(data, f, indent=4)

print("Registered in installed_plugins.json")
PYEOF

echo "Done. Restart Claude Code for the skills to appear."

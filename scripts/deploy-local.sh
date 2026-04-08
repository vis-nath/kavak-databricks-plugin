#!/usr/bin/env bash
# Installs the kavak-databricks plugin into Claude's plugin system.
# Run after cloning/updating the repo. Restart Claude Code after running.
set -e

BUNDLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_NAME="kavak-databricks"
MARKETPLACE="kavak-databricks-plugin"
VERSION="1.0.0"
INSTALL_PATH="$HOME/.claude/plugins/cache/$MARKETPLACE/$PLUGIN_NAME/$VERSION"
INSTALLED_JSON="$HOME/.claude/plugins/installed_plugins.json"
SETTINGS_JSON="$HOME/.claude/settings.json"

# 1. Copy plugin files into cache
rm -rf "$INSTALL_PATH"
mkdir -p "$INSTALL_PATH/.claude-plugin"
mkdir -p "$INSTALL_PATH/skills"

cp "$BUNDLE_DIR/.claude-plugin/plugin.json" "$INSTALL_PATH/.claude-plugin/plugin.json"
cp "$BUNDLE_DIR/.claude-plugin/marketplace.json" "$INSTALL_PATH/.claude-plugin/marketplace.json"

for skill_dir in "$BUNDLE_DIR/skills"/*/; do
    skill_name="$(basename "$skill_dir")"
    mkdir -p "$INSTALL_PATH/skills/$skill_name"
    cp "$skill_dir/SKILL.md" "$INSTALL_PATH/skills/$skill_name/SKILL.md"
    echo "Copied: $skill_name"
done

# 2. Register in installed_plugins.json and enable in settings.json
python3 - <<PYEOF
import json, os, datetime

now = datetime.datetime.now(datetime.timezone.utc).isoformat()
plugin_key = "${PLUGIN_NAME}@${MARKETPLACE}"
install_path = "$INSTALL_PATH"

# installed_plugins.json
installed_path = os.path.expanduser("$INSTALLED_JSON")
with open(installed_path) as f:
    installed = json.load(f)

installed["plugins"][plugin_key] = [{
    "scope": "user",
    "installPath": install_path,
    "version": "$VERSION",
    "installedAt": now,
    "lastUpdated": now,
    "gitCommitSha": ""
}]

with open(installed_path, "w") as f:
    json.dump(installed, f, indent=4)
print("Registered in installed_plugins.json")

# settings.json — add marketplace + enable plugin
settings_path = os.path.expanduser("$SETTINGS_JSON")
with open(settings_path) as f:
    settings = json.load(f)

if "extraKnownMarketplaces" not in settings:
    settings["extraKnownMarketplaces"] = {}
settings["extraKnownMarketplaces"]["$MARKETPLACE"] = {
    "source": {
        "source": "github",
        "repo": "vis-nath/kavak-databricks-plugin"
    }
}

if "enabledPlugins" not in settings:
    settings["enabledPlugins"] = {}
settings["enabledPlugins"][plugin_key] = True

with open(settings_path, "w") as f:
    json.dump(settings, f, indent=4)
print("Enabled in settings.json")
PYEOF

echo ""
echo "Done. Restart Claude Code for the skills to appear."

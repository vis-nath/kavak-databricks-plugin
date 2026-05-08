#!/usr/bin/env bash
# Installs the kavak-databricks plugin into Claude's plugin system.
# Run after cloning/updating the repo. Restart Claude Code after running.
set -euo pipefail

BUNDLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_NAME="kavak-databricks"
MARKETPLACE="kavak-databricks-plugin"
VERSION="2.0.0"
INSTALL_PATH="$HOME/.claude/plugins/cache/$MARKETPLACE/$PLUGIN_NAME/$VERSION"
INSTALLED_JSON="$HOME/.claude/plugins/installed_plugins.json"
SETTINGS_JSON="$HOME/.claude/settings.json"
SKILLS=(kavak-index kavak-install kavak-query kavak-token-update analyst-agent)

echo "Deploying kavak-databricks-plugin v$VERSION..."

# 1. Copy plugin metadata
rm -rf "$INSTALL_PATH"
mkdir -p "$INSTALL_PATH/.claude-plugin"
mkdir -p "$INSTALL_PATH/skills"

cp "$BUNDLE_DIR/.claude-plugin/plugin.json" "$INSTALL_PATH/.claude-plugin/plugin.json"
cp "$BUNDLE_DIR/.claude-plugin/marketplace.json" "$INSTALL_PATH/.claude-plugin/marketplace.json"

# 2. Copy skills
for skill in "${SKILLS[@]}"; do
    mkdir -p "$INSTALL_PATH/skills/$skill"
    cp "$BUNDLE_DIR/skills/$skill/SKILL.md" "$INSTALL_PATH/skills/$skill/SKILL.md"
    echo "  Copied skill: $skill"
done

# 3. Copy knowledge files (if any)
mkdir -p "$INSTALL_PATH/knowledge"
knowledge_count=0
for f in "$BUNDLE_DIR/knowledge"/*.md; do
    [ -f "$f" ] && cp "$f" "$INSTALL_PATH/knowledge/" && knowledge_count=$((knowledge_count + 1))
done
if [ "$knowledge_count" -gt 0 ]; then
    echo "  Copied $knowledge_count knowledge file(s)."
else
    echo "  No knowledge files yet — add .md files to knowledge/ and re-deploy."
fi

# 4. Register in installed_plugins.json and enable in settings.json
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

settings.setdefault("extraKnownMarketplaces", {})["$MARKETPLACE"] = {
    "source": {"source": "github", "repo": "vis-nath/kavak-databricks-plugin"}
}
settings.setdefault("enabledPlugins", {})[plugin_key] = True

with open(settings_path, "w") as f:
    json.dump(settings, f, indent=4)
print("Enabled in settings.json")
PYEOF

echo ""
echo "Done. Restart Claude Code for the new skills to appear."
echo "Skills: kavak-index | kavak-install | kavak-query | kavak-token-update | analyst-agent"

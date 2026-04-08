#!/usr/bin/env bash
# Copies the canonical SKILL.md files from the bundle to .claude/plugins/skills/
# Run after editing any SKILL.md to update Claude's live skills.
set -e

BUNDLE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SKILLS_DIR="$HOME/.claude/plugins/skills"

for skill_dir in "$BUNDLE_DIR/skills"/*/; do
    skill_name="$(basename "$skill_dir")"
    cp "$skill_dir/SKILL.md" "$SKILLS_DIR/${skill_name}.md"
    echo "Deployed: ${skill_name}.md"
done

echo "Done. Claude will pick up the new skills on next Skill tool call."

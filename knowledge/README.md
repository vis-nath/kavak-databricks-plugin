# Knowledge Files

Each `.md` file in this directory defines a domain area the analyst-agent can answer questions about.
Add one file per domain (e.g., `eeas_metrics.md`, `dealers.md`, `pix.md`).

## File Convention

Each knowledge file must start with a YAML frontmatter block:

```yaml
---
domain: [domain name, e.g., "EaaS Metrics"]
topics: [comma-separated list of topics, e.g., "funnel, conversion, PIX, reservas"]
tables:
  databricks:
    - prd_refined.schema.tabla1
    - prd_refined.schema.tabla2
  redshift:
    - schema.tabla_en_migracion   # solo si aún no migrada a Databricks
---
```

The `analyst-agent` reads this frontmatter to build a quick scope index before reading file bodies.
This avoids loading all knowledge content on every request — only the relevant file is read in full.

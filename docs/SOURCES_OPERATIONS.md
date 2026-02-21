# Sources Operations

## Priority Model
Runtime source resolution follows:

1. DB source override (`sources` table)
2. Rules workspace source definition
3. YAML defaults (`rules/sources/*.yaml` / `sources_registry.v1.yaml`)

`effective_interval_minutes` is resolved as:

1. `sources.fetch_interval_minutes` (source-level override)
2. `source_groups.default_interval_minutes` (group default)
3. YAML/rules `fetch.interval_minutes`
4. scheduler fallback default `60` (with warning in run_meta)

`interval_source` returns `source|group|yaml|default`.

## Source Groups
Group keys:

- `regulatory`
- `media`
- `evidence`
- `company`
- `procurement`

Manage with:

- `GET /admin/api/source-groups`
- `PATCH /admin/api/source-groups/{group_key}`

Changing group default interval applies immediately to sources that have empty source-level interval.

## Soft Delete
Soft delete does not physically remove rows.

- Delete: `DELETE /admin/api/sources/{id}`
  - sets `deleted_at=now`
  - sets `enabled=false`
- Restore: `POST /admin/api/sources/{id}/restore`
  - clears `deleted_at`
  - keeps current `enabled` state (safe default)

List behavior:

- `GET /admin/api/sources?include_deleted=0` (default): hide deleted
- `GET /admin/api/sources?include_deleted=1`: include deleted

## YAML to DB Import
If DB `sources` is empty, bootstrap path can seed from YAML/rules sources.
For bulk refresh, use existing `RulesStore.upsert_sources(..., replace=True)` flow in admin/import scripts.

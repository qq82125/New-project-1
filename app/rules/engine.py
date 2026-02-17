from __future__ import annotations

import json
import uuid
from copy import deepcopy
import os
from pathlib import Path
from typing import Any

import yaml

from .errors import (
    RULES_001_SCHEMA_INVALID,
    RULES_002_PROFILE_NOT_FOUND,
    RULES_003_RULESET_MISMATCH,
    RULES_004_PARSE_FAILED,
    RULES_005_SCHEMA_NOT_FOUND,
    RULES_BOUNDARY_VIOLATION,
    RuleEngineError,
)
from .models import ExplainRecord, RuleSelection
from app.services.rules_store import RulesStore
from app.services.rules_versioning import get_workspace_rules_root


def _type_ok(expected: str, value: Any) -> bool:
    mapping = {
        "object": dict,
        "array": list,
        "string": str,
        "number": (int, float),
        "integer": int,
        "boolean": bool,
    }
    py_t = mapping.get(expected)
    if py_t is None:
        return True
    if expected == "number" and isinstance(value, bool):
        return False
    if expected == "integer" and isinstance(value, bool):
        return False
    return isinstance(value, py_t)


def _validate_schema(
    data: Any,
    schema: dict[str, Any],
    path: str = "$",
    root_schema: dict[str, Any] | None = None,
) -> list[str]:
    errors: list[str] = []
    root_schema = root_schema or schema

    expected_type = schema.get("type")
    if expected_type and not _type_ok(expected_type, data):
        errors.append(f"{path}: expected {expected_type}, got {type(data).__name__}")
        return errors

    if "const" in schema and data != schema["const"]:
        errors.append(f"{path}: expected const={schema['const']!r}, got {data!r}")
    if "enum" in schema and data not in schema["enum"]:
        errors.append(f"{path}: expected one of {schema['enum']!r}, got {data!r}")

    if isinstance(data, (int, float)) and not isinstance(data, bool):
        if "minimum" in schema and data < schema["minimum"]:
            errors.append(f"{path}: value {data} < minimum {schema['minimum']}")
        if "maximum" in schema and data > schema["maximum"]:
            errors.append(f"{path}: value {data} > maximum {schema['maximum']}")

    if isinstance(data, str):
        if "minLength" in schema and len(data) < schema["minLength"]:
            errors.append(f"{path}: string length < {schema['minLength']}")

    if isinstance(data, list):
        if "minItems" in schema and len(data) < schema["minItems"]:
            errors.append(f"{path}: array length < {schema['minItems']}")
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for idx, item in enumerate(data):
                errors.extend(_validate_schema(item, item_schema, f"{path}[{idx}]", root_schema))

    if isinstance(data, dict):
        required = schema.get("required", [])
        for k in required:
            if k not in data:
                errors.append(f"{path}: missing required key '{k}'")
        properties = schema.get("properties", {})
        defs = root_schema.get("$defs", {})
        for k, subschema in properties.items():
            if k not in data:
                continue
            if isinstance(subschema, dict) and "$ref" in subschema:
                ref = subschema["$ref"]
                if ref.startswith("#/$defs/"):
                    name = ref.split("/")[-1]
                    target = defs.get(name)
                    if isinstance(target, dict):
                        errors.extend(_validate_schema(data[k], target, f"{path}.{k}", root_schema))
                    else:
                        errors.append(f"{path}.{k}: unresolved $ref {ref}")
                else:
                    errors.append(f"{path}.{k}: unsupported $ref {ref}")
            elif isinstance(subschema, dict):
                errors.extend(_validate_schema(data[k], subschema, f"{path}.{k}", root_schema))

    return errors


def _deep_merge_dict(dst: dict[str, Any], src: dict[str, Any]) -> dict[str, Any]:
    out = deepcopy(dst)
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge_dict(out[k], v)
        else:
            out[k] = deepcopy(v)
    return out


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _set_path(obj: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    cur: Any = obj
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


def _get_path(obj: dict[str, Any], path: str) -> tuple[bool, Any]:
    parts = path.split(".")
    cur: Any = obj
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            return False, None
        cur = cur[p]
    return True, cur


class RuleEngine:
    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        env_workspace = os.environ.get("RULES_WORKSPACE_DIR", "").strip()
        self.use_db = not bool(env_workspace)
        if env_workspace:
            self.rules_root = Path(env_workspace)
        else:
            self.rules_root = get_workspace_rules_root(self.project_root)
        self.schemas_root = self.rules_root / "schemas"
        self.rules_store = RulesStore(self.project_root) if self.use_db else None

    def _rules_dir(self, ruleset: str) -> Path:
        if ruleset not in ("email_rules", "content_rules", "qc_rules", "output_rules"):
            raise RuleEngineError(RULES_003_RULESET_MISMATCH, f"unsupported ruleset={ruleset}")
        return self.rules_root / ruleset

    def _schema_path(self, ruleset: str) -> Path:
        name_map = {
            "email_rules": "email_rules.schema.json",
            "content_rules": "content_rules.schema.json",
            "qc_rules": "qc_rules.schema.json",
            "output_rules": "output_rules.schema.json",
        }
        name = name_map.get(ruleset, "")
        if not name:
            raise RuleEngineError(RULES_005_SCHEMA_NOT_FOUND, f"unsupported ruleset={ruleset}")
        # Prefer workspace (console published) schema, then fallback to repo schema.
        p = self.schemas_root / name
        if p.exists():
            return p
        fallback = self.project_root / "rules" / "schemas" / name
        if fallback.exists():
            return fallback
        raise RuleEngineError(RULES_005_SCHEMA_NOT_FOUND, str(p))

    def _profile_path(self, ruleset: str, profile: str) -> Path:
        # Prefer workspace (console published) rules, then fallback to repo rules.
        candidates: list[Path] = []
        for base in (self.rules_root, self.project_root / "rules"):
            d = base / ruleset
            candidates.extend([d / f"{profile}.yaml", d / f"{profile}.yml", d / f"{profile}.json"])
        for p in candidates:
            if p.exists():
                return p
        raise RuleEngineError(RULES_002_PROFILE_NOT_FOUND, f"{ruleset}:{profile}")

    def _find_forbidden_keys(self, obj: Any, forbidden: set[str], path: str = "$") -> list[str]:
        out: list[str] = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                kk = str(k)
                if kk in forbidden:
                    out.append(f"{path}.{kk}")
                out.extend(self._find_forbidden_keys(v, forbidden, f"{path}.{kk}"))
        elif isinstance(obj, list):
            for idx, it in enumerate(obj):
                out.extend(self._find_forbidden_keys(it, forbidden, f"{path}[{idx}]"))
        return out

    def _boundary_check(self, selection: RuleSelection) -> None:
        """
        Boundary assertion for a ruleset config (preflight).
        If violated, raise RULES_BOUNDARY_VIOLATION.
        """
        qc_forbid = {
            # email-ish keys (config or decision)
            "subject_template",
            "subject_prefix",
            "recipient",
            "recipients",
            "send_window",
            "schedule",
            "retry",
            "backup",
            "sender_env",
            "delivery_strategy",
            "content_trim",
            "sections",
        }
        content_forbid = {
            # content-ish keys (sources/filters/candidates)
            "sources",
            "allow_sources",
            "deny_sources",
            "keyword_sets",
            "include_keywords",
            "exclude_keywords",
            "categories_map",
            "lane_mapping",
            "platform_mapping",
            "event_mapping",
            "dedupe_window",
            "item_limit",
            "region_filter",
            "confidence",
            "source_priority",
            "dedupe_cluster",
            "content_sources",
        }

        ruleset = selection.ruleset
        if ruleset == "qc_rules":
            bad = self._find_forbidden_keys(selection.data, qc_forbid)
        elif ruleset == "output_rules":
            bad = self._find_forbidden_keys(selection.data, content_forbid)
        elif ruleset == "email_rules":
            bad = self._find_forbidden_keys(selection.data, content_forbid)
        elif ruleset == "content_rules":
            bad = self._find_forbidden_keys(selection.data, qc_forbid)
        else:
            bad = []

        if bad:
            raise RuleEngineError(
                RULES_BOUNDARY_VIOLATION,
                f"{ruleset}:{selection.profile} forbidden_keys={bad[:8]}",
            )

    def _load_file(self, path: Path) -> dict[str, Any]:
        try:
            text = path.read_text(encoding="utf-8")
            if path.suffix.lower() == ".json":
                obj = json.loads(text)
            else:
                obj = yaml.safe_load(text)
        except Exception as e:  # pragma: no cover - defensive
            raise RuleEngineError(RULES_004_PARSE_FAILED, f"{path}: {e}") from e
        if not isinstance(obj, dict):
            raise RuleEngineError(RULES_004_PARSE_FAILED, f"{path}: top-level must be object")
        return obj

    def validate(self, ruleset: str, data: dict[str, Any]) -> None:
        schema = json.loads(self._schema_path(ruleset).read_text(encoding="utf-8"))
        errors = _validate_schema(data, schema)
        if errors:
            raise RuleEngineError(RULES_001_SCHEMA_INVALID, "; ".join(errors[:10]))

    def load(self, ruleset: str, profile: str | None = None) -> RuleSelection:
        profile = profile or "default.v1"
        if self.use_db and self.rules_store is not None:
            db_data = self.rules_store.get_active_rules(ruleset, profile)
            if isinstance(db_data, dict):
                if db_data.get("ruleset") != ruleset:
                    raise RuleEngineError(
                        RULES_003_RULESET_MISMATCH,
                        f"db ruleset={db_data.get('ruleset')} expected={ruleset}",
                    )
                self.validate(ruleset, db_data)
                store_meta = db_data.get("_store_meta", {}) if isinstance(db_data.get("_store_meta"), dict) else {}
                return RuleSelection(
                    ruleset=ruleset,
                    profile=str(db_data.get("profile", profile)),
                    version=str(store_meta.get("version", db_data.get("version", ""))),
                    path=Path(f"db://{ruleset}/{profile}"),
                    data=db_data,
                )

        path = self._profile_path(ruleset, profile)
        data = self._load_file(path)
        if data.get("ruleset") != ruleset:
            raise RuleEngineError(
                RULES_003_RULESET_MISMATCH,
                f"file={path} ruleset={data.get('ruleset')} expected={ruleset}",
            )
        self.validate(ruleset, data)
        return RuleSelection(
            ruleset=ruleset,
            profile=str(data.get("profile", profile)),
            version=str(data.get("version", "")),
            path=path,
            data=data,
        )

    def select_profile(
        self,
        ruleset: str,
        requested_profile: str | None = None,
        *,
        fallback_profile: str = "default.v1",
        fallback_on_missing: bool = True,
    ) -> RuleSelection:
        if not requested_profile:
            return self.load(ruleset, fallback_profile)
        try:
            return self.load(ruleset, requested_profile)
        except RuleEngineError as e:
            if (
                fallback_on_missing
                and e.err.code == RULES_002_PROFILE_NOT_FOUND.code
                and requested_profile != fallback_profile
            ):
                return self.load(ruleset, fallback_profile)
            raise

    def load_pair(
        self,
        email_profile: str | None = None,
        content_profile: str | None = None,
        *,
        fallback_on_missing: bool = True,
    ) -> tuple[RuleSelection, RuleSelection]:
        email = self.select_profile(
            "email_rules",
            email_profile,
            fallback_on_missing=fallback_on_missing,
        )
        content = self.select_profile(
            "content_rules",
            content_profile,
            fallback_on_missing=fallback_on_missing,
        )
        return email, content

    def explain(
        self,
        email: RuleSelection,
        content: RuleSelection,
        qc: RuleSelection | None = None,
        output: RuleSelection | None = None,
        mode: str = "normal",
        run_id: str | None = None,
        notes: list[str] | None = None,
    ) -> ExplainRecord:
        qc = qc or RuleSelection("qc_rules", "legacy", "", Path(""), {})
        output = output or RuleSelection("output_rules", "legacy", "", Path(""), {})
        return ExplainRecord(
            run_id=run_id or uuid.uuid4().hex[:12],
            mode=mode,
            email_profile=email.profile,
            email_version=email.version,
            content_profile=content.profile,
            content_version=content.version,
            qc_profile=qc.profile,
            qc_version=qc.version,
            output_profile=output.profile,
            output_version=output.version,
            notes=notes or [],
        )

    def validate_profile_pair(self, profile: str) -> dict[str, Any]:
        email, content = self.load_pair(
            email_profile=profile,
            content_profile=profile,
            fallback_on_missing=False,
        )
        return {
            "profile": profile,
            "validated": [
                {
                    "ruleset": email.ruleset,
                    "profile": email.profile,
                    "version": email.version,
                    "path": str(email.path),
                },
                {
                    "ruleset": content.ruleset,
                    "profile": content.profile,
                    "version": content.version,
                    "path": str(content.path),
                },
            ],
        }

    def _flatten_sources(self, defaults: dict[str, Any]) -> list[dict[str, Any]]:
        sources = defaults.get("sources", {}) if isinstance(defaults.get("sources"), dict) else {}
        out: list[dict[str, Any]] = []
        for group, items in sources.items():
            for item in _as_list(items):
                if not isinstance(item, dict):
                    continue
                s = deepcopy(item)
                s["group"] = group
                out.append(s)
        return out

    def _base_content_decision(self, content: RuleSelection) -> dict[str, Any]:
        defaults = content.data.get("defaults", {})
        overrides = content.data.get("overrides", {})
        time_window = defaults.get("time_window", {})

        return {
            "allow_sources": self._flatten_sources(defaults),
            "deny_sources": [],
            "keyword_sets": {
                "packs": _as_list(overrides.get("keywords_pack")),
                "include_keywords": [],
                "exclude_keywords": _as_list(overrides.get("exclude_terms")),
            },
            "categories_map": {
                "tracks": _as_list(defaults.get("coverage_tracks")),
                "lane_mapping": {},
                "platform_mapping": {},
                "event_mapping": {},
            },
            "dedupe_window": {
                "primary_hours": time_window.get("primary_hours", 24),
                "fallback_days": time_window.get("fallback_days", 7),
            },
            "item_limit": deepcopy(defaults.get("item_limit", {})),
            "region_filter": deepcopy(defaults.get("region_filter", {})),
            "confidence": {
                "min_confidence": float(overrides.get("min_confidence", 0.0) or 0.0),
                "ranking": {},
            },
            "source_priority": deepcopy(defaults.get("source_priority", {})),
            "dedupe_cluster": deepcopy(
                defaults.get(
                    "dedupe_cluster",
                    {
                        "enabled": False,
                        "window_hours": 72,
                        "key_strategies": [
                            "canonical_url",
                            "normalized_url_host_path",
                            "title_fingerprint_v1",
                        ],
                        "primary_select": [
                            "source_priority",
                            "evidence_grade",
                            "published_at_earliest",
                        ],
                        "max_other_sources": 5,
                    },
                )
            ),
            "content_sources": deepcopy(defaults.get("content_sources", {})),
        }

    def _base_email_decision(self, email: RuleSelection) -> dict[str, Any]:
        defaults = email.data.get("defaults", {})
        overrides = email.data.get("overrides", {})
        output = email.data.get("output", {})
        send_window = defaults.get("send_window", {})

        subject_template = str(defaults.get("subject_template", "全球IVD晨报 - {{date}}"))
        subject_prefix = str(overrides.get("subject_prefix", ""))
        if overrides.get("enabled") and subject_prefix:
            subject_template = f"{subject_prefix}{subject_template}"

        return {
            "subject_template": subject_template,
            "sections": _as_list(output.get("sections")),
            "recipients": [defaults.get("recipient", "${TO_EMAIL}")],
            "schedule": {
                "timezone": defaults.get("timezone", "Asia/Shanghai"),
                "hour": int(send_window.get("hour", 8)),
                "minute": int(send_window.get("minute", 30)),
            },
            "thresholds": {},
            "retry": deepcopy(defaults.get("retry", {})),
            "dedupe_window_hours": int(overrides.get("dedupe_window_hours", 24) or 24),
            "charts": {
                "enabled": bool(output.get("charts_enabled", False)),
                "types": _as_list(output.get("chart_types")),
            },
        }

    def _base_qc_decision(self, qc: RuleSelection) -> dict[str, Any]:
        defaults = qc.data.get("defaults", {}) if isinstance(qc.data.get("defaults"), dict) else {}
        return deepcopy(defaults)

    def _base_output_decision(self, output: RuleSelection) -> dict[str, Any]:
        defaults = output.data.get("defaults", {}) if isinstance(output.data.get("defaults"), dict) else {}
        out = output.data.get("output", {}) if isinstance(output.data.get("output"), dict) else {}
        base = deepcopy(defaults)
        if "sections_order" in out:
            base["sections_order"] = deepcopy(out.get("sections_order"))
        return base

    def _effect_paths(
        self,
        ruleset: str,
        rule_type: str,
        params: dict[str, Any],
    ) -> list[tuple[str, Any]]:
        if ruleset == "content_rules":
            if rule_type == "source_priority":
                return [("source_priority", params)]
            if rule_type == "include_filter":
                return [("keyword_sets.include_keywords", params.get("include_keywords", params))]
            if rule_type == "exclude_filter":
                return [
                    ("keyword_sets.exclude_keywords", params.get("exclude_keywords", params)),
                    ("deny_sources", _as_list(params.get("deny_sources"))),
                ]
            if rule_type == "lane_mapping":
                return [("categories_map.lane_mapping", params)]
            if rule_type == "platform_mapping":
                return [("categories_map.platform_mapping", params)]
            if rule_type == "event_mapping":
                return [("categories_map.event_mapping", params)]
            if rule_type == "dedupe":
                out = [("dedupe_window", params)]
                if "dedupe_cluster" in params:
                    out.append(("dedupe_cluster", params.get("dedupe_cluster")))
                return out
            if rule_type == "confidence_ranking":
                return [("confidence.ranking", params)]
            if rule_type == "region_filter":
                return [("region_filter", params)]

        if ruleset == "email_rules":
            if rule_type == "subject_template":
                return [("subject_template", params.get("template", ""))]
            if rule_type == "recipient_policy":
                out = []
                recipients = params.get("recipients")
                if recipients is None:
                    default_to = params.get("default_to")
                    if default_to:
                        recipients = [default_to]
                if recipients is not None:
                    out.append(("recipients", _as_list(recipients)))
                return out
            if rule_type == "dedupe":
                return [("dedupe_window_hours", int(params.get("window_hours", 24) or 24))]
            if rule_type == "retry_policy":
                return [("retry", params)]
            if rule_type == "backup_policy":
                return [("backup", params)]
            if rule_type == "content_format":
                out = []
                if "section_priority" in params:
                    out.append(("sections", _as_list(params.get("section_priority"))))
                out.append(("thresholds", params))
                return out
            if rule_type == "charts_toggle":
                out = [("charts.enabled", bool(params.get("charts_enabled", False)))]
                if "chart_types" in params:
                    out.append(("charts.types", _as_list(params.get("chart_types"))))
                return out

        if ruleset == "qc_rules":
            if rule_type == "qc_thresholds":
                return [
                    ("min_24h_items", params.get("min_24h_items")),
                    ("fallback_days", params.get("fallback_days")),
                    ("7d_topup_limit", params.get("7d_topup_limit")),
                    ("apac_min_share", params.get("apac_min_share")),
                    ("china_min_share", params.get("china_min_share")),
                    ("daily_repeat_rate_max", params.get("daily_repeat_rate_max")),
                    ("recent_7d_repeat_rate_max", params.get("recent_7d_repeat_rate_max")),
                ]
            if rule_type == "required_sources":
                return [("required_sources_checklist", params.get("required_sources_checklist", []))]
            if rule_type == "rumor_policy":
                return [("rumor_policy", params)]
            if rule_type == "fail_policy":
                return [("fail_policy", params)]
            if rule_type == "mix_targets":
                return [("regulatory_vs_commercial_mix", params)]

        if ruleset == "output_rules":
            if rule_type == "sections":
                out = []
                if "order" in params:
                    out.append(("sections_order", _as_list(params.get("order"))))
                if "enabled" in params:
                    out.append(("sections_enabled_map", deepcopy(params.get("enabled"))))
                return out
            if rule_type == "section_a":
                return [("A", params)]
            if rule_type == "section_sizes":
                return [
                    ("E", {"trends_count": params.get("trends_count")}),
                    ("F", {"gaps_count": {"min": params.get("gaps_min"), "max": params.get("gaps_max")}}),
                ]
            if rule_type == "heatmap":
                return [("D", {"heatmap_regions": _as_list(params.get("regions"))})]
            if rule_type == "constraints":
                return [("constraints", params)]

        return []

    def _apply_one(
        self,
        decision: dict[str, Any],
        path: str,
        value: Any,
        merge_strategy: str,
    ) -> Any:
        exists, old = _get_path(decision, path)
        if not exists:
            _set_path(decision, path, deepcopy(value))
            return deepcopy(value)

        if merge_strategy == "append":
            old_list = _as_list(old)
            new_list = _as_list(value)
            merged = old_list[:]
            for i in new_list:
                if i not in merged:
                    merged.append(i)
            _set_path(decision, path, merged)
            return merged

        if merge_strategy == "merge" and isinstance(old, dict) and isinstance(value, dict):
            merged = _deep_merge_dict(old, value)
            _set_path(decision, path, merged)
            return merged

        _set_path(decision, path, deepcopy(value))
        return deepcopy(value)

    def _resolve_rules(
        self,
        selection: RuleSelection,
        decision: dict[str, Any],
        *,
        default_merge: str,
        explain: dict[str, Any],
        provenance: dict[str, dict[str, Any]],
    ) -> None:
        raw_rules = selection.data.get("rules", [])
        rules = [r for r in raw_rules if isinstance(r, dict)]
        indexed = list(enumerate(rules))
        indexed.sort(key=lambda x: (int(x[1].get("priority", 0)), x[0]))

        for idx, rule in indexed:
            rule_id = str(rule.get("id", f"rule_{idx}"))
            enabled = bool(rule.get("enabled", False))
            rule_type = str(rule.get("type", "unknown"))
            priority = int(rule.get("priority", 0))
            params = rule.get("params", {})
            if not isinstance(params, dict):
                params = {"value": params}

            if not enabled:
                explain["skipped_rules"].append(
                    {
                        "ruleset": selection.ruleset,
                        "rule_id": rule_id,
                        "type": rule_type,
                        "priority": priority,
                        "reason": "disabled",
                    }
                )
                continue

            merge_strategy = str(rule.get("merge_strategy") or rule.get("merge") or default_merge)
            if merge_strategy not in ("last_match", "append", "merge"):
                merge_strategy = default_merge

            effects = self._effect_paths(selection.ruleset, rule_type, params)
            if not effects:
                explain["skipped_rules"].append(
                    {
                        "ruleset": selection.ruleset,
                        "rule_id": rule_id,
                        "type": rule_type,
                        "priority": priority,
                        "reason": "no_effect_mapping",
                    }
                )
                continue

            affected_fields: list[str] = []
            for path, value in effects:
                exists, old = _get_path(decision, path)
                if exists and path in provenance:
                    explain["conflicts"].append(
                        {
                            "field": path,
                            "previous_rule": provenance[path]["rule_id"],
                            "current_rule": rule_id,
                            "strategy": merge_strategy,
                            "previous_value": old,
                        }
                    )

                new_value = self._apply_one(decision, path, value, merge_strategy)
                provenance[path] = {
                    "rule_id": rule_id,
                    "priority": priority,
                    "ruleset": selection.ruleset,
                    "value": deepcopy(new_value),
                }
                if explain["conflicts"] and explain["conflicts"][-1].get("field") == path:
                    explain["conflicts"][-1]["resolved_value"] = deepcopy(new_value)
                affected_fields.append(path)

            if rule_type == "exclude_filter":
                reason = "命中排除规则，更新排除关键词或拒绝源集合"
            elif rule_type == "include_filter":
                reason = "命中包含规则，扩展关键词集合"
            else:
                reason = "命中规则并更新决策字段"

            explain["applied_rules"].append(
                {
                    "ruleset": selection.ruleset,
                    "rule_id": rule_id,
                    "type": rule_type,
                    "priority": priority,
                    "merge_strategy": merge_strategy,
                    "affected_fields": affected_fields,
                    "reason": reason,
                }
            )

    def build_decision(
        self,
        profile: str,
        *,
        conflict_strategy: str = "priority_last_match",
        run_id: str | None = None,
    ) -> dict[str, Any]:
        email, content = self.load_pair(
            email_profile=profile,
            content_profile=profile,
            fallback_on_missing=False,
        )
        try:
            self._boundary_check(email)
        except RuleEngineError as e:
            # Email rules must never influence candidate selection; if violated, fallback email only.
            if e.err.code != RULES_BOUNDARY_VIOLATION.code or profile == "legacy":
                raise
            email = self.load("email_rules", "legacy")
            self._boundary_check(email)

        def _load_with_boundary_fallback(ruleset: str) -> tuple[RuleSelection, str]:
            try:
                sel = self.load(ruleset, profile)
            except RuleEngineError as e:
                # Backward-compatible: qc/output are optional until fully wired everywhere.
                if e.err.code == RULES_002_PROFILE_NOT_FOUND.code:
                    noop = {
                        "ruleset": ruleset,
                        "version": "",
                        "profile": "legacy",
                        "defaults": {},
                        "overrides": {"enabled": False},
                        "rules": [],
                        "output": {},
                    }
                    return RuleSelection(ruleset, "legacy", "", Path(f"noop://{ruleset}"), noop), str(e)
                raise
            try:
                self._boundary_check(sel)
                return sel, ""
            except RuleEngineError as e:
                if e.err.code != RULES_BOUNDARY_VIOLATION.code:
                    raise
                if profile == "legacy":
                    raise
                legacy_sel = self.load(ruleset, "legacy")
                self._boundary_check(legacy_sel)
                return legacy_sel, str(e)

        qc, qc_fallback_reason = _load_with_boundary_fallback("qc_rules")
        output, output_fallback_reason = _load_with_boundary_fallback("output_rules")

        default_merge = "last_match"
        if conflict_strategy == "priority_append":
            default_merge = "append"
        if conflict_strategy == "priority_merge":
            default_merge = "merge"

        content_decision = self._base_content_decision(content)
        qc_decision = self._base_qc_decision(qc)
        output_decision = self._base_output_decision(output)
        email_decision = self._base_email_decision(email)

        explain_obj: dict[str, Any] = {
            "run_id": run_id or uuid.uuid4().hex[:12],
            "profile": profile,
            "conflict_strategy": conflict_strategy,
            "applied_rules": [],
            "skipped_rules": [],
            "conflicts": [],
            "rulesets": [
                {"ruleset": "content_rules", "profile": content.profile, "version": content.version, "path": str(content.path)},
                {"ruleset": "qc_rules", "profile": qc.profile, "version": qc.version, "path": str(qc.path), "fallback_reason": qc_fallback_reason},
                {"ruleset": "output_rules", "profile": output.profile, "version": output.version, "path": str(output.path), "fallback_reason": output_fallback_reason},
                {"ruleset": "email_rules", "profile": email.profile, "version": email.version, "path": str(email.path)},
            ],
        }

        provenance: dict[str, dict[str, Any]] = {}
        self._resolve_rules(
            content,
            content_decision,
            default_merge=default_merge,
            explain=explain_obj,
            provenance=provenance,
        )
        provenance = {}
        self._resolve_rules(
            qc,
            qc_decision,
            default_merge=default_merge,
            explain=explain_obj,
            provenance=provenance,
        )
        provenance = {}
        self._resolve_rules(
            output,
            output_decision,
            default_merge=default_merge,
            explain=explain_obj,
            provenance=provenance,
        )
        provenance = {}
        self._resolve_rules(
            email,
            email_decision,
            default_merge=default_merge,
            explain=explain_obj,
            provenance=provenance,
        )

        explain_obj["summary"] = {
            "applied_count": len(explain_obj["applied_rules"]),
            "skipped_count": len(explain_obj["skipped_rules"]),
            "conflict_count": len(explain_obj["conflicts"]),
            "why_included": [
                r["rule_id"]
                for r in explain_obj["applied_rules"]
                if r.get("type") in ("include_filter", "source_priority")
            ],
            "why_excluded": [
                r["rule_id"] for r in explain_obj["applied_rules"] if r.get("type") == "exclude_filter"
            ],
        }

        return {
            "run_id": explain_obj["run_id"],
            "profile": profile,
            "rules_version": {
                "email": email.version,
                "content": content.version,
                "qc": qc.version,
                "output": output.version,
            },
            "conflict_strategy": conflict_strategy,
            "content_decision": content_decision,
            "qc_decision": qc_decision,
            "output_decision": output_decision,
            "email_decision": email_decision,
            "explain": explain_obj,
        }

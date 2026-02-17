from __future__ import annotations

import json
import uuid
from pathlib import Path
from typing import Any

import yaml

from .errors import (
    RULES_001_SCHEMA_INVALID,
    RULES_002_PROFILE_NOT_FOUND,
    RULES_003_RULESET_MISMATCH,
    RULES_004_PARSE_FAILED,
    RULES_005_SCHEMA_NOT_FOUND,
    RuleEngineError,
)
from .models import ExplainRecord, RuleSelection


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


class RuleEngine:
    def __init__(self, project_root: Path | None = None) -> None:
        self.project_root = project_root or Path(__file__).resolve().parents[2]
        self.rules_root = self.project_root / "rules"
        self.schemas_root = self.rules_root / "schemas"

    def _rules_dir(self, ruleset: str) -> Path:
        if ruleset not in ("email_rules", "content_rules"):
            raise RuleEngineError(RULES_003_RULESET_MISMATCH, f"unsupported ruleset={ruleset}")
        return self.rules_root / ruleset

    def _schema_path(self, ruleset: str) -> Path:
        name = "email_rules.schema.json" if ruleset == "email_rules" else "content_rules.schema.json"
        p = self.schemas_root / name
        if not p.exists():
            raise RuleEngineError(RULES_005_SCHEMA_NOT_FOUND, str(p))
        return p

    def _profile_path(self, ruleset: str, profile: str) -> Path:
        d = self._rules_dir(ruleset)
        candidates = [d / f"{profile}.yaml", d / f"{profile}.yml", d / f"{profile}.json"]
        for p in candidates:
            if p.exists():
                return p
        raise RuleEngineError(RULES_002_PROFILE_NOT_FOUND, f"{ruleset}:{profile}")

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
        mode: str = "normal",
        run_id: str | None = None,
        notes: list[str] | None = None,
    ) -> ExplainRecord:
        return ExplainRecord(
            run_id=run_id or uuid.uuid4().hex[:12],
            mode=mode,
            email_profile=email.profile,
            email_version=email.version,
            content_profile=content.profile,
            content_version=content.version,
            notes=notes or [],
        )

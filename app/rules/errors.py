from dataclasses import dataclass


@dataclass(frozen=True)
class RuleEngineErrorCode:
    code: str
    message: str


RULES_001_SCHEMA_INVALID = RuleEngineErrorCode(
    "RULES_001_SCHEMA_INVALID",
    "Rules schema validation failed.",
)
RULES_002_PROFILE_NOT_FOUND = RuleEngineErrorCode(
    "RULES_002_PROFILE_NOT_FOUND",
    "Requested rules profile was not found.",
)
RULES_003_RULESET_MISMATCH = RuleEngineErrorCode(
    "RULES_003_RULESET_MISMATCH",
    "Ruleset type does not match requested namespace.",
)
RULES_004_PARSE_FAILED = RuleEngineErrorCode(
    "RULES_004_PARSE_FAILED",
    "Rules file parse failed.",
)
RULES_005_SCHEMA_NOT_FOUND = RuleEngineErrorCode(
    "RULES_005_SCHEMA_NOT_FOUND",
    "Schema file was not found.",
)
RULES_BOUNDARY_VIOLATION = RuleEngineErrorCode(
    "RULES_BOUNDARY_VIOLATION",
    "Rules decision boundary was violated.",
)


class RuleEngineError(RuntimeError):
    def __init__(self, err: RuleEngineErrorCode, detail: str = "") -> None:
        suffix = f" detail={detail}" if detail else ""
        super().__init__(f"{err.code}: {err.message}{suffix}")
        self.err = err
        self.detail = detail

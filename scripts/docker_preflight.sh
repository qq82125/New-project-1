#!/usr/bin/env bash
set -u

# Docker preflight for this project.
# Goal: quickly detect common issues before `docker compose up -d --build`.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

CHECKS_TOTAL=0
CHECKS_FAILED=0

PASS_LIST=()
FAIL_LIST=()
WARN_LIST=()

if [ "${NO_COLOR:-}" = "1" ]; then
  C_RESET=""
  C_RED=""
  C_GREEN=""
  C_YELLOW=""
  C_CYAN=""
else
  C_RESET="$(printf '\033[0m')"
  C_RED="$(printf '\033[31m')"
  C_GREEN="$(printf '\033[32m')"
  C_YELLOW="$(printf '\033[33m')"
  C_CYAN="$(printf '\033[36m')"
fi

log_ok() {
  printf "%s[OK]%s %s\n" "$C_GREEN" "$C_RESET" "$1"
  PASS_LIST+=("$1")
}

log_fail() {
  printf "%s[FAIL]%s %s\n" "$C_RED" "$C_RESET" "$1"
  FAIL_LIST+=("$1")
  CHECKS_FAILED=$((CHECKS_FAILED + 1))
}

log_warn() {
  printf "%s[WARN]%s %s\n" "$C_YELLOW" "$C_RESET" "$1"
  WARN_LIST+=("$1")
}

run_required_check() {
  CHECKS_TOTAL=$((CHECKS_TOTAL + 1))
  local name="$1"
  shift
  if "$@" >/tmp/docker_preflight_last.out 2>/tmp/docker_preflight_last.err; then
    log_ok "$name"
  else
    log_fail "$name"
    sed -n '1,8p' /tmp/docker_preflight_last.err
    sed -n '1,8p' /tmp/docker_preflight_last.out
  fi
}

run_optional_check() {
  local name="$1"
  shift
  if "$@" >/tmp/docker_preflight_last.out 2>/tmp/docker_preflight_last.err; then
    log_ok "$name"
  else
    log_warn "$name"
    sed -n '1,4p' /tmp/docker_preflight_last.err
    sed -n '1,4p' /tmp/docker_preflight_last.out
  fi
}

printf "%sDocker preflight for New-project-1%s\n" "$C_CYAN" "$C_RESET"
printf "Workspace: %s\n\n" "$ROOT_DIR"

run_required_check "docker CLI available" command -v docker
run_required_check "docker daemon reachable" docker info
run_required_check "docker compose available" docker compose version

if [ -f "docker-compose.yml" ]; then
  run_required_check "compose file is valid" docker compose config -q
else
  log_fail "docker-compose.yml missing"
fi

if command -v curl >/dev/null 2>&1; then
  # Use GET (not HEAD), because this endpoint may return 405 for HEAD.
  run_optional_check "Docker Hub auth endpoint reachable" curl -fsS "https://auth.docker.io/token?service=registry.docker.io"
else
  log_warn "curl not found, skipped Docker Hub endpoint probe"
fi

if [ "${SKIP_PULL:-0}" = "1" ]; then
  log_warn "SKIP_PULL=1, skipped image pull test"
else
  run_optional_check "pull public image (hello-world)" docker pull hello-world:latest
fi

run_optional_check "compose services status" docker compose ps

echo
printf "%sSummary%s: total_required=%d failed_required=%d optional_warnings=%d\n" \
  "$C_CYAN" "$C_RESET" "$CHECKS_TOTAL" "$CHECKS_FAILED" "${#WARN_LIST[@]}"

if [ "${#PASS_LIST[@]}" -gt 0 ]; then
  printf "%sPassed:%s %s\n" "$C_GREEN" "$C_RESET" "$(IFS='; '; echo "${PASS_LIST[*]}")"
fi
if [ "${#WARN_LIST[@]}" -gt 0 ]; then
  printf "%sWarnings:%s %s\n" "$C_YELLOW" "$C_RESET" "$(IFS='; '; echo "${WARN_LIST[*]}")"
fi
if [ "${#FAIL_LIST[@]}" -gt 0 ]; then
  printf "%sFailed:%s %s\n" "$C_RED" "$C_RESET" "$(IFS='; '; echo "${FAIL_LIST[*]}")"
fi

if [ "$CHECKS_FAILED" -gt 0 ]; then
  echo "Result: BLOCKED (fix required checks before build/run)"
  exit 2
fi

echo "Result: READY (safe to run: docker compose up -d --build)"
exit 0

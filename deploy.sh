#!/bin/bash
# deploy.sh — Stock Signal Engine deployment script
# Handles first-time setup and subsequent deploys on Amazon Linux 2023 (EC2).
# Safe to run multiple times (idempotent).

set -e
set -o pipefail

# ─── Colors ────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
RESET='\033[0m'

# ─── Trap ──────────────────────────────────────────────────────────────────────
trap 'echo -e "\n${RED}❌ Deployment failed!${RESET}" >&2' ERR

NEEDS_ENV_EDIT=false
PYTHON_VERSION=""
TEST_COUNT=""

# ─── Step 1: Detect project root ───────────────────────────────────────────────
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"
echo -e "${BOLD}📁 Project directory: ${PROJECT_DIR}${RESET}"

# ─── Step 2: Pull latest from main ─────────────────────────────────────────────
echo ""
echo -e "${BOLD}🔄 Pulling latest from main...${RESET}"
if ! git pull origin main; then
    echo -e "${RED}❌ git pull failed. Check your network connection, remote access, or for uncommitted local changes on tracked files.${RESET}" >&2
    exit 1
fi
echo -e "${GREEN}✅ Pulled latest from main${RESET}"

# ─── Step 3: Check Python version ──────────────────────────────────────────────
echo ""
echo -e "${BOLD}🐍 Checking Python version...${RESET}"

# Load pyenv if available so the project's .python-version file is respected
if [[ -x "$HOME/.pyenv/bin/pyenv" ]] || command -v pyenv &>/dev/null 2>&1; then
    export PYENV_ROOT="${PYENV_ROOT:-$HOME/.pyenv}"
    export PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
fi

# Helper: returns major.minor as integers and checks >= 3.10
_python_is_310_plus() {
    local bin="$1"
    local ver
    ver="$("$bin" --version 2>&1 | awk '{print $2}')" || return 1
    local major minor
    major="$(echo "$ver" | cut -d. -f1)"
    minor="$(echo "$ver" | cut -d. -f2)"
    [[ "$major" -gt 3 ]] || [[ "$major" -eq 3 && "$minor" -ge 10 ]]
}

PYTHON_BIN=""
for candidate in python python3.11 python3.10 python3; do
    if command -v "$candidate" &>/dev/null 2>&1; then
        if _python_is_310_plus "$candidate"; then
            PYTHON_BIN="$(command -v "$candidate")"
            break
        fi
    fi
done

if [[ -z "$PYTHON_BIN" ]]; then
    FALLBACK_VER="$(python3 --version 2>&1 | awk '{print $2}' || echo "not found")"
    echo -e "${RED}❌ Python 3.10+ required. Found: ${FALLBACK_VER}${RESET}" >&2
    echo -e "${RED}   Install via pyenv: pyenv install 3.11.9 && pyenv local 3.11.9${RESET}" >&2
    exit 1
fi

PYTHON_VERSION="$("$PYTHON_BIN" --version 2>&1 | awk '{print $2}')"
echo -e "${GREEN}✅ Python ${PYTHON_VERSION} detected (via ${PYTHON_BIN})${RESET}"

# ─── Step 4: Create or activate venv ───────────────────────────────────────────
echo ""
echo -e "${BOLD}🔧 Setting up virtual environment...${RESET}"
if [[ -d "${PROJECT_DIR}/.venv" ]]; then
    VENV_PYTHON_VER="$("${PROJECT_DIR}/.venv/bin/python" --version 2>&1 | awk '{print $2}')"
    VENV_MAJOR="$(echo "$VENV_PYTHON_VER" | cut -d. -f1)"
    VENV_MINOR="$(echo "$VENV_PYTHON_VER" | cut -d. -f2)"
    if [[ "$VENV_MAJOR" -lt 3 ]] || [[ "$VENV_MAJOR" -eq 3 && "$VENV_MINOR" -lt 10 ]]; then
        echo -e "${YELLOW}⚠️  Existing venv uses Python ${VENV_PYTHON_VER}. Recreating with ${PYTHON_BIN}...${RESET}"
        rm -rf "${PROJECT_DIR}/.venv"
        "$PYTHON_BIN" -m venv "${PROJECT_DIR}/.venv"
        echo -e "${GREEN}✅ Created virtual environment at .venv/${RESET}"
    else
        echo -e "${GREEN}✅ Virtual environment exists at .venv/${RESET}"
    fi
else
    "$PYTHON_BIN" -m venv "${PROJECT_DIR}/.venv"
    echo -e "${GREEN}✅ Created virtual environment at .venv/${RESET}"
fi

# shellcheck disable=SC1091
source "${PROJECT_DIR}/.venv/bin/activate"

ACTIVE_PYTHON="$(which python)"
if [[ "$ACTIVE_PYTHON" != *".venv"* ]]; then
    echo -e "${RED}❌ Virtual environment activation failed. Expected python inside .venv, got: ${ACTIVE_PYTHON}${RESET}" >&2
    exit 1
fi
echo -e "${GREEN}✅ Virtual environment active: ${ACTIVE_PYTHON}${RESET}"

# ─── Step 5: Install/upgrade dependencies ──────────────────────────────────────
echo ""
echo -e "${BOLD}📦 Installing dependencies...${RESET}"
pip install --upgrade pip -q
if ! pip install -r "${PROJECT_DIR}/requirements.txt"; then
    echo -e "${RED}❌ pip install failed. Check requirements.txt and your network connection.${RESET}" >&2
    exit 1
fi
echo -e "${GREEN}✅ Dependencies installed${RESET}"

# ─── Step 6: Check .env file ───────────────────────────────────────────────────
echo ""
echo -e "${BOLD}🔑 Checking .env configuration...${RESET}"
REQUIRED_KEYS=(
    "POLYGON_API_KEY"
    "FINNHUB_API_KEY"
    "ANTHROPIC_API_KEY"
    "TELEGRAM_BOT_TOKEN"
    "TELEGRAM_CHAT_ID"
)

if [[ ! -f "${PROJECT_DIR}/.env" ]]; then
    cp "${PROJECT_DIR}/.env.example" "${PROJECT_DIR}/.env"
    echo -e "${YELLOW}⚠️  Created .env from template — EDIT IT with your API keys before running!${RESET}"
    NEEDS_ENV_EDIT=true
else
    MISSING_KEYS=()
    for key in "${REQUIRED_KEYS[@]}"; do
        # Check if key exists in .env
        if ! grep -q "^${key}=" "${PROJECT_DIR}/.env"; then
            MISSING_KEYS+=("$key (missing)")
            continue
        fi
        # Check if value is still the placeholder
        VALUE="$(grep "^${key}=" "${PROJECT_DIR}/.env" | cut -d= -f2-)"
        if [[ "$VALUE" == "your_"* ]] || [[ -z "$VALUE" ]]; then
            MISSING_KEYS+=("$key (placeholder not replaced)")
        fi
    done

    if [[ ${#MISSING_KEYS[@]} -gt 0 ]]; then
        echo -e "${YELLOW}⚠️  The following .env keys need to be set:${RESET}"
        for k in "${MISSING_KEYS[@]}"; do
            echo -e "${YELLOW}   - ${k}${RESET}"
        done
        NEEDS_ENV_EDIT=true
    else
        echo -e "${GREEN}✅ .env file configured${RESET}"
    fi
fi

# ─── Step 7: Create data directories ───────────────────────────────────────────
echo ""
echo -e "${BOLD}📂 Creating data directories...${RESET}"
mkdir -p "${PROJECT_DIR}/data"
mkdir -p "${PROJECT_DIR}/data/backups"
mkdir -p "${PROJECT_DIR}/logs"
echo -e "${GREEN}✅ Data directories ready${RESET}"

# ─── Step 8: Set up database ───────────────────────────────────────────────────
echo ""
echo -e "${BOLD}🗄️  Initializing database...${RESET}"
if ! python "${PROJECT_DIR}/scripts/setup_db.py"; then
    echo -e "${RED}❌ Database setup failed. Check scripts/setup_db.py for errors.${RESET}" >&2
    exit 1
fi
echo -e "${GREEN}✅ Database initialized${RESET}"

# ─── Step 9: Run tests ─────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}🧪 Running tests...${RESET}"
set +e
TEST_OUTPUT="$(python -m pytest tests/ -v --tb=short 2>&1)"
TEST_EXIT_CODE=$?
set -e

echo "$TEST_OUTPUT"

if [[ $TEST_EXIT_CODE -ne 0 ]]; then
    echo -e "${RED}❌ Tests failed! Deployment aborted.${RESET}" >&2
    echo -e "${RED}   Fix the failing tests before deploying.${RESET}" >&2
    exit 1
fi

# Extract passed count from pytest summary line (e.g. "42 passed")
TEST_COUNT="$(echo "$TEST_OUTPUT" | grep -oE '[0-9]+ passed' | tail -1 | awk '{print $1}' || echo "?")"
echo -e "${GREEN}✅ All tests passed${RESET}"

# ─── Step 10: Print summary ────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════╗${RESET}"
echo -e "${BOLD}║         🚀 Deployment Complete!              ║${RESET}"
echo -e "${BOLD}╠══════════════════════════════════════════════╣${RESET}"
printf "${BOLD}║  Project:  %-34s║${RESET}\n" "${PROJECT_DIR}"
printf "${BOLD}║  Python:   %-34s║${RESET}\n" "${PYTHON_VERSION}"
printf "${BOLD}║  Venv:     %-34s║${RESET}\n" ".venv/"
printf "${BOLD}║  Database: %-34s║${RESET}\n" "see config/database.json"
printf "${BOLD}║  Tests:    %-34s║${RESET}\n" "${TEST_COUNT} passed"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${RESET}"

if [[ "$NEEDS_ENV_EDIT" == "true" ]]; then
    echo ""
    echo -e "${YELLOW}⚠️  ACTION REQUIRED:${RESET}"
    echo -e "${YELLOW}   Edit .env and set your API keys:${RESET}"
    echo -e "${YELLOW}   - POLYGON_API_KEY${RESET}"
    echo -e "${YELLOW}   - FINNHUB_API_KEY${RESET}"
    echo -e "${YELLOW}   - ANTHROPIC_API_KEY${RESET}"
    echo -e "${YELLOW}   - TELEGRAM_BOT_TOKEN${RESET}"
    echo -e "${YELLOW}   - TELEGRAM_CHAT_ID${RESET}"
    echo ""
    echo -e "${YELLOW}   Then run: python scripts/test_api_access.py${RESET}"
fi

echo ""
echo -e "${BOLD}Quick start:${RESET}"
echo "  source .venv/bin/activate"
echo "  python scripts/test_api_access.py     # verify API access"
echo "  python scripts/run_backfill.py        # run initial backfill"

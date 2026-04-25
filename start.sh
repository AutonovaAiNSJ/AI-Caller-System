#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "🚀 Starting OutboundAI..."

# ── Load .env only for local dev — VPS env vars always take priority ──────────
# On a VPS/Docker, this file will not exist and the block is skipped entirely.
if [ -f ".env" ]; then
    echo "📄 Loading .env (local dev mode)..."
    # Use -a to export without subshell; skip blank lines and comments
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
fi

# ── Validate required env vars ────────────────────────────────────────────────
MISSING=""
for VAR in LIVEKIT_URL LIVEKIT_API_KEY LIVEKIT_API_SECRET GOOGLE_API_KEY SUPABASE_URL SUPABASE_SERVICE_KEY; do
    if [ -z "${!VAR}" ]; then
        MISSING="$MISSING $VAR"
    fi
done
if [ -n "$MISSING" ]; then
    echo "❌ Missing required environment variables:$MISSING"
    echo "   Set them in Coolify → Environment Variables (or .env for local dev)"
    exit 1
fi

echo "📋 Configuration:"
echo "   LiveKit:  ${LIVEKIT_URL}"
echo "   Gemini:   ${GEMINI_MODEL:-gemini-3.1-flash-live-preview} / ${GEMINI_TTS_VOICE:-Aoede}"
echo "   Supabase: ${SUPABASE_URL}"
echo "   SIP Trunk: ${OUTBOUND_TRUNK_ID:-not set}"

# ── Resolve port — Coolify/Railway inject PORT; default to 8000 ──────────────
APP_PORT="${PORT:-8000}"
echo "🌐 Starting FastAPI server on port ${APP_PORT}..."
uvicorn server:app --host 0.0.0.0 --port "${APP_PORT}" &
SERVER_PID=$!

# Ensure uvicorn is killed when this script exits for any reason
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

# ── Wait for FastAPI to be ready (up to 30s) ─────────────────────────────────
echo "⏳ Waiting for FastAPI to be ready on port ${APP_PORT}..."
for i in $(seq 1 30); do
    if curl -sf "http://localhost:${APP_PORT}/" > /dev/null 2>&1; then
        echo "✅ FastAPI server ready (${i}s)"
        break
    fi
    sleep 1
    if [ "$i" -eq 30 ]; then
        echo "⚠️  FastAPI did not respond in 30s — starting agent anyway"
    fi
done

# ── Start LiveKit agent worker (foreground — container exits when this does) ──
echo "🤖 Starting LiveKit agent worker..."
python agent.py start

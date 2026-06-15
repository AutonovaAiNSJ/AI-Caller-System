-- ═══════════════════════════════════════════════════════
-- OutboundAI — Complete Database Schema
-- Run once in Supabase Dashboard → SQL Editor
-- ═══════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS appointments (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    phone TEXT NOT NULL,
    email TEXT,
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    service TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'booked',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS call_logs (
    id TEXT PRIMARY KEY,
    phone_number TEXT NOT NULL,
    lead_name TEXT,
    outcome TEXT,
    reason TEXT,
    duration_seconds INTEGER,
    timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS call_sessions (
    id TEXT PRIMARY KEY,
    room_name TEXT NOT NULL,
    direction TEXT NOT NULL DEFAULT 'outbound',
    phone_number TEXT NOT NULL,
    lead_name TEXT,
    status TEXT NOT NULL DEFAULT 'dispatching',
    started_at TEXT NOT NULL,
    connected_at TEXT,
    ended_at TEXT,
    outcome TEXT,    
    reason TEXT,
    duration_seconds INTEGER,
    recording_url TEXT,
    metadata TEXT,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS error_logs (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'error',
    message TEXT NOT NULL,
    detail TEXT,
    timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS call_transcripts (
    id BIGSERIAL PRIMARY KEY,
    room_name TEXT,
    speaker TEXT,
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

ALTER TABLE appointments  DISABLE ROW LEVEL SECURITY;
ALTER TABLE call_logs     DISABLE ROW LEVEL SECURITY;
ALTER TABLE call_sessions DISABLE ROW LEVEL SECURITY;
ALTER TABLE settings      DISABLE ROW LEVEL SECURITY;
ALTER TABLE error_logs    DISABLE ROW LEVEL SECURITY;
ALTER TABLE call_transcripts DISABLE ROW LEVEL SECURITY;

ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS recording_url TEXT;
ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS notes TEXT;
ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS direction TEXT DEFAULT 'outbound';
ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS call_session_id TEXT;

CREATE TABLE IF NOT EXISTS campaigns (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    contacts_json TEXT NOT NULL DEFAULT '[]',
    schedule_type TEXT NOT NULL DEFAULT 'once',
    schedule_time TEXT DEFAULT '09:00',
    call_delay_seconds INTEGER DEFAULT 3,
    system_prompt TEXT,
    created_at TEXT NOT NULL,
    last_run_at TEXT,
    total_dispatched INTEGER DEFAULT 0,
    total_failed INTEGER DEFAULT 0
);
ALTER TABLE campaigns DISABLE ROW LEVEL SECURITY;

ALTER TABLE appointments ADD COLUMN IF NOT EXISTS calcom_booking_uid TEXT;
ALTER TABLE appointments ADD COLUMN IF NOT EXISTS gcal_event_id TEXT;
ALTER TABLE appointments ADD COLUMN IF NOT EXISTS gcal_event_link TEXT;

CREATE TABLE IF NOT EXISTS contact_memory (
    id TEXT PRIMARY KEY,
    phone_number TEXT NOT NULL,
    insight TEXT NOT NULL,
    created_at TEXT NOT NULL
);
ALTER TABLE contact_memory DISABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_contact_memory_phone ON contact_memory (phone_number);

ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS agent_profile_id TEXT;

CREATE TABLE IF NOT EXISTS agent_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    voice TEXT NOT NULL DEFAULT 'Aoede',
    model TEXT NOT NULL DEFAULT 'gemini-3.1-flash-live-preview',
    system_prompt TEXT,
    enabled_tools TEXT DEFAULT '[]',
    is_default INTEGER DEFAULT 0,
    created_at TEXT NOT NULL
);
ALTER TABLE agent_profiles DISABLE ROW LEVEL SECURITY;

ALTER TABLE appointments ADD COLUMN IF NOT EXISTS email TEXT;

-- ════════════════════════════════════════════════════════════════════════════
-- White Label Phase 1: tenants, branding, API keys, wallet, audit logs
-- Run after backing up production. Existing data is assigned to tenant 'default'.
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    company_name TEXT NOT NULL,
    slug TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL DEFAULT 'ACTIVE',
    billing_mode TEXT NOT NULL DEFAULT 'MANAGED',
    wallet_balance NUMERIC NOT NULL DEFAULT 0,
    wallet_low_balance_threshold NUMERIC NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    company_logo TEXT,
    favicon TEXT,
    primary_color TEXT,
    secondary_color TEXT,
    support_email TEXT,
    website_url TEXT,
    company_logo_url TEXT,
    favicon_url TEXT,
    onboarded BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TEXT NOT NULL DEFAULT NOW()::TEXT,
    updated_at TEXT
);
ALTER TABLE tenants DISABLE ROW LEVEL SECURITY;

INSERT INTO tenants (
    id, company_name, slug, status, billing_mode,
    wallet_balance, wallet_low_balance_threshold, is_active, created_at
)
VALUES ('default', 'OutboundAI', 'default', 'ACTIVE', 'MANAGED', 0, 0, TRUE, NOW()::TEXT)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS tenant_audit_logs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    user_email TEXT,
    action TEXT NOT NULL,
    detail TEXT,
    timestamp TEXT NOT NULL DEFAULT NOW()::TEXT
);
ALTER TABLE tenant_audit_logs DISABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS tenant_api_keys (
    tenant_id TEXT NOT NULL DEFAULT 'default',
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'BYOK',
    updated_at TEXT NOT NULL DEFAULT NOW()::TEXT,
    PRIMARY KEY (tenant_id, key)
);
ALTER TABLE tenant_api_keys DISABLE ROW LEVEL SECURITY;

ALTER TABLE appointments     ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE call_logs        ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE call_sessions    ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE settings         ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE error_logs       ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE call_transcripts ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE campaigns        ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE contact_memory   ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';
ALTER TABLE agent_profiles   ADD COLUMN IF NOT EXISTS tenant_id TEXT DEFAULT 'default';

ALTER TABLE call_logs ADD COLUMN IF NOT EXISTS room_name TEXT;
ALTER TABLE call_transcripts ADD COLUMN IF NOT EXISTS is_final BOOLEAN DEFAULT TRUE;

UPDATE appointments     SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE call_logs        SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE call_sessions    SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE settings         SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE error_logs       SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE call_transcripts SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE campaigns        SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE contact_memory   SET tenant_id = 'default' WHERE tenant_id IS NULL;
UPDATE agent_profiles   SET tenant_id = 'default' WHERE tenant_id IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'settings_pkey'
          AND conrelid = 'settings'::regclass
    ) THEN
        ALTER TABLE settings DROP CONSTRAINT settings_pkey;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_settings_tenant_key ON settings (tenant_id, key);
CREATE INDEX IF NOT EXISTS idx_appointments_tenant ON appointments (tenant_id, date, time);
CREATE INDEX IF NOT EXISTS idx_call_logs_tenant ON call_logs (tenant_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_call_sessions_tenant ON call_sessions (tenant_id, room_name);
CREATE INDEX IF NOT EXISTS idx_call_transcripts_tenant ON call_transcripts (tenant_id, room_name, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_campaigns_tenant ON campaigns (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_contact_memory_tenant_phone ON contact_memory (tenant_id, phone_number);
CREATE INDEX IF NOT EXISTS idx_agent_profiles_tenant ON agent_profiles (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tenant_audit_logs_tenant ON tenant_audit_logs (tenant_id, timestamp DESC);

-- ════════════════════════════════════════════════════════════════════════════
-- Supabase Auth Integration: users and tenant_users tables
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    full_name TEXT,
    role TEXT NOT NULL,
    tenant_id TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);

CREATE TABLE IF NOT EXISTS tenant_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id TEXT NOT NULL,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
ALTER TABLE tenant_users ENABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_tenant_users_tenant_user ON tenant_users (tenant_id, user_id);

-- ════════════════════════════════════════════════════════════════════════════
-- Phase 2: Invite flow + Platform Pricing
-- ════════════════════════════════════════════════════════════════════════════

-- pending_invites: bridges invite creation → first login role assignment
CREATE TABLE IF NOT EXISTS pending_invites (
    id TEXT PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    tenant_id TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'TENANT_ADMIN',
    invited_by TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '7 days')
);
ALTER TABLE pending_invites DISABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_pending_invites_email ON pending_invites (email);

-- Platform pricing is stored as regular settings rows under tenant_id='default'.
-- Keys: PRICE_PER_CALL_ATTEMPT, PRICE_PER_APPOINTMENT
-- These are inserted/updated via the /api/super-admin/pricing endpoint.

-- ════════════════════════════════════════════════════════════════════════════
-- Phase 3: Tenant Deletion, Suspension and Email Delivery Logs Upgrade
-- ════════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS deleted_tenants (
    tenant_id TEXT PRIMARY KEY,
    tenant_name TEXT NOT NULL,
    slug TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    created_at TEXT,
    deleted_at TEXT NOT NULL DEFAULT NOW()::TEXT,
    deleted_by TEXT,
    deletion_reason TEXT,
    wallet_balance NUMERIC NOT NULL DEFAULT 0,
    billing_mode TEXT NOT NULL,
    full_snapshot_json TEXT NOT NULL
);
ALTER TABLE deleted_tenants DISABLE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS email_delivery_logs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    recipient TEXT NOT NULL,
    subject TEXT NOT NULL,
    provider_response TEXT,
    error_message TEXT,
    status TEXT NOT NULL DEFAULT 'sent', -- 'sent', 'delivered', 'failed', 'opened'
    timestamp TEXT NOT NULL DEFAULT NOW()::TEXT
);
ALTER TABLE email_delivery_logs DISABLE ROW LEVEL SECURITY;
CREATE INDEX IF NOT EXISTS idx_email_delivery_logs_tenant ON email_delivery_logs (tenant_id, timestamp DESC);

ALTER TABLE tenants ADD COLUMN IF NOT EXISTS suspension_reason TEXT;
ALTER TABLE tenants ADD COLUMN IF NOT EXISTS suspension_notes TEXT;


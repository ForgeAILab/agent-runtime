-- Source: open-forge-design adopt-session-journal-2026-10-19/design.md D4.
-- One row per (session, domain): the head record, its fences and tombstone.
CREATE TABLE agent_session_journal_head (
    session_id            TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain                TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    revision              INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0), -- CAS; 0 = writer reservation, no head
    writer_epoch          INTEGER NOT NULL DEFAULT 0 CHECK (writer_epoch >= 0),
    batch_sequence        INTEGER NOT NULL DEFAULT 0 CHECK (batch_sequence >= 0),
    head_ciphertext       BLOB,            -- sealed JournalHead; NULL before first publish and after retirement
    head_nonce            BLOB,
    has_checkpoint        INTEGER NOT NULL DEFAULT 0 CHECK (has_checkpoint IN (0, 1)),
    retired_revision      INTEGER,         -- tombstone: survives collection
    retired_root_epoch    INTEGER,
    retired_at            TEXT,
    retirement_grant_revision INTEGER,     -- host authorization, bound to one revision
    retirement_granted_at TEXT,
    gc_dirty              INTEGER NOT NULL DEFAULT 0 CHECK (gc_dirty IN (0, 1)),
    key_revision          INTEGER NOT NULL,
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,
    PRIMARY KEY (session_id, domain),
    CHECK ((head_ciphertext IS NULL) = (head_nonce IS NULL)),
    CHECK (retired_revision IS NULL OR head_ciphertext IS NULL)
);
CREATE INDEX idx_session_journal_head_dirty ON agent_session_journal_head(domain, gc_dirty)
    WHERE gc_dirty = 1;

-- Physical immutable bodies, shared by both domains and by every session
-- that references identical content. No session FK: lifetime = memberships.
CREATE TABLE agent_session_journal_body (
    object_key     BLOB PRIMARY KEY CHECK (length(object_key) = 32),
    ciphertext     BLOB NOT NULL,
    nonce          BLOB NOT NULL CHECK (length(nonce) = 24),
    encoded_len    INTEGER NOT NULL CHECK (encoded_len > 0), -- logical journal bytes
    key_revision   INTEGER NOT NULL,
    created_at     TEXT NOT NULL
);

-- Logical object store per domain and session (published or staged).
CREATE TABLE agent_session_journal_object (
    session_id   TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain       TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    object_key   BLOB NOT NULL REFERENCES agent_session_journal_body(object_key),
    staged_pin   BLOB,                     -- NULL = published membership
    PRIMARY KEY (session_id, domain, object_key)
) WITHOUT ROWID;
CREATE INDEX idx_session_journal_object_key ON agent_session_journal_object(object_key);

-- Idempotency / replay fences, retained for the session's lifetime.
CREATE TABLE agent_session_journal_operation (
    session_id        TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain            TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    operation_key     BLOB NOT NULL CHECK (length(operation_key) = 32),
    input_mac         BLOB NOT NULL CHECK (length(input_mac) = 32), -- HMAC of the private commit digest
    revision          INTEGER NOT NULL,
    batch_sequence    INTEGER NOT NULL,
    writer_epoch      INTEGER NOT NULL,
    result_ciphertext BLOB NOT NULL,       -- sealed original JournalHead
    result_nonce      BLOB NOT NULL,
    committed_at      TEXT NOT NULL,
    PRIMARY KEY (session_id, domain, operation_key)
) WITHOUT ROWID;

-- Durable pins (transfer, retained root, import, staging) and engine indexes
-- (dependency roots, retained-head catalog), as sealed engine records.
CREATE TABLE agent_session_journal_record (
    session_id   TEXT NOT NULL REFERENCES agent_session(id) ON DELETE CASCADE,
    domain       TEXT NOT NULL CHECK (domain IN ('ordinary', 'protected')),
    record_kind  TEXT NOT NULL CHECK (record_kind IN
                     ('transfer_pin', 'retained_pin', 'import_pin', 'staging_pin',
                      'dependency', 'retained_head')),
    record_key   BLOB NOT NULL,
    ciphertext   BLOB NOT NULL,
    nonce        BLOB NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (session_id, domain, record_kind, record_key)
) WITHOUT ROWID;

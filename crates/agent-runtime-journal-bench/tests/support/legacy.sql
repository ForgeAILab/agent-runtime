-- Forge V062 + V142 + V202609302227; parent table is the harness identity stub.
CREATE TABLE protected_agent_session_state (
 session_id TEXT PRIMARY KEY REFERENCES agent_session(id) ON DELETE CASCADE,
 snapshot_ciphertext BLOB, snapshot_nonce BLOB,
 checkpoint_ciphertext BLOB, checkpoint_nonce BLOB,
 checkpoint_turn_id TEXT, checkpoint_revision INTEGER, checkpoint_fingerprint TEXT,
 key_revision INTEGER NOT NULL, state_revision INTEGER NOT NULL DEFAULT 1,
 updated_at TEXT NOT NULL, lcm_policy_revision TEXT, snapshot_digest TEXT,
 checkpoint_lcm_policy_revision TEXT
);

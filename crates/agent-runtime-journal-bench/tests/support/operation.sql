-- Same D4 operation rows retained by S1-S3; S4 replaces these with packed records.
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

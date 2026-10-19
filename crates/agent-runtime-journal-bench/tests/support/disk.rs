use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{Connection, params};
use serde::Serialize;
use sha2::{Digest, Sha256};

static NEXT: AtomicU64 = AtomicU64::new(0);
pub(crate) const DATE: &str = "2026-10-19T00:00:00.123456789+00:00";
pub(crate) const PARENT: &str = "CREATE TABLE agent_session(id TEXT PRIMARY KEY);";
pub(crate) fn key(value: &[u8]) -> Vec<u8> {
    Sha256::digest(value).to_vec()
}
pub(crate) fn compress(bytes: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(bytes, 3).unwrap()
}
// AEAD ciphertext is incompressible. SQLite never compresses these bytes itself;
// zero padding therefore has exactly the same physical length as actual sealing.
pub(crate) fn seal(bytes: &[u8], magic: &[u8], always_compress: bool) -> Vec<u8> {
    let encoded = compress(bytes);
    let len = if always_compress || (bytes.len() >= 1024 && encoded.len() * 8 <= bytes.len() * 7) {
        encoded.len()
    } else {
        bytes.len()
    };
    vec![0; magic.len() + 1 + len + 16]
}
#[derive(Debug, Serialize)]
pub(crate) struct Size {
    pub(crate) page_count: u64,
    pub(crate) page_size: u64,
    pub(crate) empty_schema_bytes: u64,
    pub(crate) file_bytes: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) row_value_bytes: u64,
    pub(crate) dbstat_bytes: u64,
    pub(crate) row_index_page_overhead: i64,
    pub(crate) table_bytes: BTreeMap<String, u64>,
}
// Every variable-width value written goes through these counters. SQL SUM
// independently verifies them before the physical file is measured.
#[derive(Debug)]
pub(crate) struct Db {
    pub(crate) conn: Connection,
    path: PathBuf,
    empty: u64,
    sums: BTreeMap<String, String>,
    expected: u64,
}
impl Db {
    pub(crate) fn new(root: &Path, ddl: &str) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let path = root.join(format!(
            "measure-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "PRAGMA page_size=4096; PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;",
        )
        .unwrap();
        conn.execute_batch(PARENT).unwrap();
        conn.execute_batch(ddl).unwrap();
        Self::vacuum(&conn);
        let empty = Self::physical(&conn);
        conn.execute(
            "INSERT INTO agent_session VALUES (?)",
            [super::layouts::SESSION],
        )
        .unwrap();
        Self {
            conn,
            path,
            empty,
            sums: BTreeMap::new(),
            expected: super::layouts::SESSION.len() as u64,
        }
    }
    fn vacuum(conn: &Connection) {
        conn.execute_batch(
            "PRAGMA wal_checkpoint(TRUNCATE); VACUUM; PRAGMA wal_checkpoint(TRUNCATE);",
        )
        .unwrap();
    }
    fn physical(conn: &Connection) -> u64 {
        let pages: u64 = conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        let size: u64 = conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        pages * size
    }
    pub(crate) fn account(&mut self, table: &str, columns: &str, bytes: usize) {
        self.sums.insert(table.into(), columns.into());
        self.expected += bytes as u64;
    }
    pub(crate) fn finish(mut self) -> Size {
        self.sums
            .insert("agent_session".into(), "length(id)".into());
        let actual: u64 = self
            .sums
            .iter()
            .map(|(table, columns)| {
                self.conn
                    .query_row(
                        &format!("SELECT coalesce(sum({columns}),0) FROM {table}"),
                        [],
                        |r| r.get::<_, u64>(0),
                    )
                    .unwrap()
            })
            .sum();
        assert_eq!(
            actual, self.expected,
            "independent SQL row sum disagrees with simulator"
        );
        Self::vacuum(&self.conn);
        let page_count = self
            .conn
            .query_row("PRAGMA page_count", [], |r| r.get(0))
            .unwrap();
        let page_size = self
            .conn
            .query_row("PRAGMA page_size", [], |r| r.get(0))
            .unwrap();
        let file_bytes = std::fs::metadata(&self.path).unwrap().len();
        assert_eq!(file_bytes, page_count * page_size);
        let mut tables = BTreeMap::new();
        let mut statement = self
            .conn
            .prepare("SELECT name,sum(pgsize) FROM dbstat GROUP BY name")
            .unwrap();
        for row in statement
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))
            .unwrap()
        {
            let (name, bytes) = row.unwrap();
            tables.insert(name, bytes);
        }
        let dbstat_bytes = tables.values().sum();
        assert_eq!(
            file_bytes, dbstat_bytes,
            "SQLite dbstat must cover every file page"
        );
        let physical_bytes = file_bytes - self.empty;
        Size {
            page_count,
            page_size,
            empty_schema_bytes: self.empty,
            file_bytes,
            physical_bytes,
            row_value_bytes: actual,
            dbstat_bytes,
            row_index_page_overhead: physical_bytes as i64 - actual as i64,
            table_bytes: tables,
        }
    }
}
impl Drop for Db {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", self.path.display()));
        }
    }
}
pub(crate) fn legacy(root: &Path, row: &super::fixture::LegacyRow, blobs: usize) -> Size {
    let mut db = Db::new(root, include_str!("legacy.sql"));
    let checkpoint = row.checkpoint.as_ref().unwrap();
    let cp = seal(
        &serde_json::to_vec(checkpoint).unwrap(),
        b"FORGE-SESSION\0",
        true,
    );
    let snapshot = (blobs == 2).then(|| {
        seal(
            &serde_json::to_vec(row.latest.as_ref().unwrap()).unwrap(),
            b"FORGE-SESSION\0",
            true,
        )
    });
    let nonce = vec![0u8; 24];
    let sn = snapshot.as_ref().map(|_| nonce.clone());
    let turn = checkpoint.turn.as_str();
    let fp = checkpoint.operation_fingerprint.to_string();
    let digest = "0".repeat(64);
    let policy = "forge-lcm-policy-v1";
    db.conn.execute("INSERT INTO protected_agent_session_state (session_id,checkpoint_ciphertext,checkpoint_nonce,checkpoint_turn_id,checkpoint_revision,checkpoint_fingerprint,snapshot_ciphertext,snapshot_nonce,snapshot_digest,key_revision,lcm_policy_revision,checkpoint_lcm_policy_revision,state_revision,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,1,?10,?10,?11,?12)",params![super::layouts::SESSION,cp,nonce,turn,checkpoint.state_revision,fp,snapshot,sn,digest,policy,row.writes,DATE]).unwrap();
    db.account("protected_agent_session_state","length(session_id)+length(checkpoint_ciphertext)+length(checkpoint_nonce)+length(checkpoint_turn_id)+length(checkpoint_fingerprint)+coalesce(length(snapshot_ciphertext),0)+coalesce(length(snapshot_nonce),0)+length(snapshot_digest)+length(lcm_policy_revision)+length(checkpoint_lcm_policy_revision)+length(updated_at)",super::layouts::SESSION.len()+cp.len()+24+turn.len()+fp.len()+snapshot.as_ref().map_or(0,|s|s.len()+24)+64+policy.len()*2+DATE.len());
    db.finish()
}

pub(crate) fn empty_file_bytes(path: &Path) -> u64 {
    let conn = Connection::open(path).unwrap();
    Db::vacuum(&conn);
    Db::physical(&conn)
}
// Measure the actual adapter file in place; do not synthesize/rewrite its rows.
pub(crate) fn existing(path: &Path, empty: u64) -> Size {
    let conn = Connection::open(path).unwrap();
    Db::vacuum(&conn);
    let page_count: u64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let page_size: u64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    let file_bytes = std::fs::metadata(path).unwrap().len();
    assert_eq!(file_bytes, page_count * page_size);
    let mut table_bytes = BTreeMap::new();
    let mut st = conn
        .prepare("SELECT name,sum(pgsize) FROM dbstat GROUP BY name")
        .unwrap();
    for row in st
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))
        .unwrap()
    {
        let (table, size) = row.unwrap();
        table_bytes.insert(table, size);
    }
    let dbstat_bytes = table_bytes.values().sum();
    assert_eq!(file_bytes, dbstat_bytes);
    let mut row_value_bytes = 0;
    for (table, columns) in [
        (
            "session",
            "length(storage)+length(session)+length(payload)+length(nonce)",
        ),
        ("pack", "length(owner)+length(payload)+length(nonce)"),
        ("membership", "length(storage)+length(session)+length(bits)"),
        (
            "pin",
            "length(storage)+length(session)+length(pin)+length(payload)+length(nonce)",
        ),
        (
            "catalog",
            "length(storage)+length(session)+length(head)+length(payload)+length(nonce)",
        ),
        (
            "operation_pack",
            "length(storage)+length(session)+length(payload)+length(nonce)",
        ),
    ] {
        row_value_bytes += conn
            .query_row(
                &format!("SELECT coalesce(sum({columns}),0) FROM {table}"),
                [],
                |r| r.get::<_, u64>(0),
            )
            .unwrap();
    }
    let physical_bytes = file_bytes - empty;
    Size {
        page_count,
        page_size,
        empty_schema_bytes: empty,
        file_bytes,
        physical_bytes,
        row_value_bytes,
        dbstat_bytes,
        row_index_page_overhead: physical_bytes as i64 - row_value_bytes as i64,
        table_bytes,
    }
}

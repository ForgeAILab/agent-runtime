use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use agent_runtime::core::journal::*;
use rusqlite::params;
use serde::Serialize;
use serde_json::Value;

use super::{disk, fixture};
use disk::{DATE, Db, Size};
use fixture::JournalMeasurementExport;

pub(crate) const SESSION: &str = "00000000-0000-4000-8000-000000000001";
const MAGIC: &[u8] = b"FORGE-JOURNAL\0";
const PACK_DDL: &str = "CREATE TABLE pack(session_id TEXT NOT NULL REFERENCES agent_session(id), pack_id INTEGER NOT NULL, ciphertext BLOB NOT NULL, nonce BLOB NOT NULL, ordinary_members BLOB NOT NULL, protected_members BLOB NOT NULL, PRIMARY KEY(session_id,pack_id)); CREATE TABLE meta(session_id TEXT NOT NULL, domain TEXT NOT NULL, record_key BLOB NOT NULL, ciphertext BLOB NOT NULL, nonce BLOB NOT NULL, PRIMARY KEY(session_id,domain,record_key)) WITHOUT ROWID;";
const INDEX_DDL: &str = "CREATE TABLE object_index(session_id TEXT NOT NULL, domain TEXT NOT NULL, object_key BLOB NOT NULL, pack_id INTEGER NOT NULL, object_offset INTEGER NOT NULL, encoded_len INTEGER NOT NULL, PRIMARY KEY(session_id,domain,object_key)) WITHOUT ROWID;";
type Objects = BTreeMap<JournalObjectId, (JournalObjectKind, Vec<u8>, u8)>;
#[derive(Debug, Serialize, Default)]
pub(crate) struct Kind {
    pub(crate) objects: usize,
    pub(crate) memberships: usize,
    pub(crate) logical_bytes: usize,
    pub(crate) standalone_zstd_bytes: usize,
    pub(crate) kind_group_zstd_bytes: usize,
    pub(crate) d4_body_ciphertext_nonce_bytes: usize,
    pub(crate) d4_body_membership_key_value_bytes: usize,
    pub(crate) allocated_row_index_page_overhead: i64,
}
#[derive(Debug, Serialize)]
pub(crate) struct Layout {
    pub(crate) name: String,
    pub(crate) before: Size,
    pub(crate) after: Size,
    pub(crate) before_without_operations: u64,
    pub(crate) after_without_operations: u64,
    pub(crate) operation_physical_bytes_before: i64,
    pub(crate) operation_physical_bytes_after: i64,
}
#[derive(Debug, Serialize)]
pub(crate) struct Operations {
    pub(crate) commits: usize,
    pub(crate) turns: usize,
    pub(crate) commits_per_turn: f64,
    pub(crate) logical_bytes: usize,
    pub(crate) d4_ciphertext_nonce_bytes: usize,
    pub(crate) packed_ciphertext_nonce_bytes: usize,
    pub(crate) window_records: usize,
    pub(crate) window_ciphertext_nonce_bytes: usize,
    pub(crate) current_epoch_records: usize,
    pub(crate) packed_bytes_per_commit: f64,
}
#[derive(Debug, Serialize)]
pub(crate) struct ManifestClosure {
    pub(crate) manifest: usize,
    pub(crate) kinds: BTreeMap<String, usize>,
    pub(crate) new_vs_cache_and_earlier_manifests: BTreeMap<String, usize>,
    pub(crate) shared_with_cache: usize,
}
#[derive(Debug, Serialize)]
pub(crate) struct Report {
    pub(crate) fixture: String,
    pub(crate) n: usize,
    pub(crate) tools: bool,
    pub(crate) provider_cache_enabled: bool,
    pub(crate) final_messages: usize,
    pub(crate) manifests_retained: u64,
    pub(crate) idle_legacy_blobs: usize,
    pub(crate) legacy_digest_skips: usize,
    pub(crate) legacy_canonical_difference_fields: Vec<String>,
    pub(crate) legacy_l1: Size,
    pub(crate) legacy_l2: Size,
    pub(crate) kinds_before: BTreeMap<String, Kind>,
    pub(crate) kinds_after: BTreeMap<String, Kind>,
    pub(crate) layouts: Vec<Layout>,
    pub(crate) operations: Operations,
    pub(crate) manifest_closures: Vec<ManifestClosure>,
    pub(crate) s123_s4_s5_before: Size,
    pub(crate) s123_s4_s5_after: Size,
    pub(crate) gate_ratio: Option<f64>,
    pub(crate) packed_before: Size,
    pub(crate) packed_after: Size,
    pub(crate) packed_before_l2_ratio: f64,
    pub(crate) packed_after_l2_ratio: f64,
    pub(crate) retained_operations_before: usize,
    pub(crate) retained_operations_after: usize,
    pub(crate) s5_simulator_payload_delta: u64,
    pub(crate) projected_after_s5_bytes: u64,
    pub(crate) projected_after_s5_l2_ratio: f64,
}
fn head_rows(db: &mut Db, exports: &[(&str, JournalMeasurementExport)], d4: bool) {
    for (domain, export) in exports {
        for head in export.heads.values() {
            let bytes = serde_json::to_vec(head).unwrap();
            let sealed = disk::seal(&bytes, MAGIC, false);
            let nonce = vec![0u8; 24];
            if d4 {
                db.conn.execute("INSERT INTO agent_session_journal_head(session_id,domain,revision,writer_epoch,batch_sequence,head_ciphertext,head_nonce,has_checkpoint,key_revision,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,1,?9,?9)",params![SESSION,domain,head.revision.0,head.writer_epoch,head.batch_sequence,sealed,nonce,head.checkpoint.is_some(),DATE]).unwrap();
                db.account("agent_session_journal_head","length(session_id)+length(domain)+length(head_ciphertext)+length(head_nonce)+length(created_at)+length(updated_at)",SESSION.len()+domain.len()+sealed.len()+24+DATE.len()*2);
            } else {
                meta(db, domain, b"head", &bytes);
            }
        }
        // Selectable-head catalog entries correspond to retained Head objects.
        // These fixtures have no transfers, staging or external dependencies.
        for (kind, object) in &export.objects {
            if *kind != JournalObjectKind::Head {
                continue;
            }
            let key = object.id.to_bytes().to_vec();
            let record = br#"{"version":1,"dependencies":[]}"#;
            if d4 {
                let ciphertext = disk::seal(record, MAGIC, false);
                db.conn.execute("INSERT INTO agent_session_journal_record VALUES (?1,?2,'retained_head',?3,?4,?5,?6)",params![SESSION,domain,key,ciphertext,vec![0u8;24],DATE]).unwrap();
                db.account("agent_session_journal_record","length(session_id)+length(domain)+length(record_kind)+length(record_key)+length(ciphertext)+length(nonce)+length(created_at)",SESSION.len()+domain.len()+13+32+ciphertext.len()+24+DATE.len());
            } else {
                meta(db, domain, &key, record);
            }
        }
    }
}
fn meta(db: &mut Db, domain: &str, key: &[u8], bytes: &[u8]) {
    let ciphertext = disk::seal(bytes, MAGIC, false);
    db.conn
        .execute(
            "INSERT INTO meta VALUES (?1,?2,?3,?4,?5)",
            params![SESSION, domain, key, ciphertext, vec![0u8; 24]],
        )
        .unwrap();
    db.account(
        "meta",
        "length(session_id)+length(domain)+length(record_key)+length(ciphertext)+length(nonce)",
        SESSION.len() + domain.len() + key.len() + ciphertext.len() + 24,
    );
}
fn operations(db: &mut Db, exports: &[(&str, JournalMeasurementExport)], mode: u8) {
    for (domain, export) in exports {
        if mode == 0 {
            continue;
        }
        let current = export.heads.values().next().unwrap();
        let last = export
            .operations
            .iter()
            .filter(|op| op.result.writer_epoch == current.writer_epoch)
            .map(|op| op.result.revision)
            .max();
        let records: Vec<_> = export
            .operations
            .iter()
            .filter(|op| {
                mode != 3
                    || op.result.revision.0 >= current.revision.0.saturating_sub(64)
                    || (op.result.writer_epoch == current.writer_epoch
                        && Some(op.result.revision) == last)
            })
            .collect();
        if mode >= 2 {
            meta(
                db,
                domain,
                b"operations",
                &serde_json::to_vec(&records).unwrap(),
            );
            continue;
        }
        for op in records {
            let bytes = serde_json::to_vec(&op.result).unwrap();
            let sealed = disk::seal(&bytes, MAGIC, false);
            let key = disk::key(op.operation.as_str().as_bytes());
            let mac = op.digest.to_bytes();
            db.conn.execute("INSERT INTO agent_session_journal_operation VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",params![SESSION,domain,key,mac.as_slice(),op.result.revision.0,op.result.batch_sequence,op.result.writer_epoch,sealed,vec![0u8;24],DATE]).unwrap();
            db.account("agent_session_journal_operation","length(session_id)+length(domain)+length(operation_key)+length(input_mac)+length(result_ciphertext)+length(result_nonce)+length(committed_at)",SESSION.len()+domain.len()+64+sealed.len()+24+DATE.len());
        }
    }
}
fn d4(root: &Path, exports: &[(&str, JournalMeasurementExport)], ops: bool) -> Size {
    let mut db = Db::new(root, include_str!("forge_d4.sql"));
    db.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    for (id, (_, bytes, bits)) in fixture::objects(exports) {
        let key = id.to_bytes();
        let sealed = disk::seal(&bytes, MAGIC, false);
        db.conn
            .execute(
                "INSERT INTO agent_session_journal_body VALUES (?1,?2,?3,?4,1,?5)",
                params![key.as_slice(), sealed, vec![0u8; 24], bytes.len(), DATE],
            )
            .unwrap();
        db.account(
            "agent_session_journal_body",
            "length(object_key)+length(ciphertext)+length(nonce)+length(created_at)",
            32 + sealed.len() + 24 + DATE.len(),
        );
        for (bit, domain) in [(1, "ordinary"), (2, "protected")] {
            if bits & bit == 0 {
                continue;
            }
            db.conn
                .execute(
                    "INSERT INTO agent_session_journal_object VALUES (?1,?2,?3,NULL)",
                    params![SESSION, domain, key.as_slice()],
                )
                .unwrap();
            db.account(
                "agent_session_journal_object",
                "length(session_id)+length(domain)+length(object_key)",
                SESSION.len() + domain.len() + 32,
            );
        }
    }
    head_rows(&mut db, exports, true);
    operations(&mut db, exports, u8::from(ops));
    db.conn.execute_batch("COMMIT").unwrap();
    db.finish()
}
fn varint(mut n: usize, out: &mut Vec<u8>) {
    while n >= 128 {
        out.push((n as u8) | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn read_varint(bytes: &[u8], pos: &mut usize) -> usize {
    let mut n = 0;
    let mut shift = 0;
    loop {
        let byte = bytes[*pos];
        *pos += 1;
        n |= ((byte & 127) as usize) << shift;
        if byte < 128 {
            return n;
        }
        shift += 7;
    }
}
// Physical pack framing: magic, count, sorted prefix directory, then canonical
// bodies in kind order. Full-id candidates are always verified from their bytes.
fn pack(
    objects: &[(JournalObjectId, JournalObjectKind, Vec<u8>, u8)],
    compact: bool,
) -> (Vec<u8>, Vec<usize>) {
    let mut body = Vec::new();
    let mut offsets = Vec::new();
    for (_, _, bytes, _) in objects {
        offsets.push(body.len());
        body.extend_from_slice(bytes);
    }
    let mut output = b"JRPACK\0\x01".to_vec();
    output.extend_from_slice(&(objects.len() as u32).to_le_bytes());
    if compact {
        let mut sorted: Vec<_> = objects.iter().enumerate().collect();
        sorted.sort_by_key(|(_, o)| o.0.clone());
        for (index, (id, _, bytes, _)) in sorted {
            output.extend_from_slice(&id.to_bytes()[..4]);
            varint(offsets[index], &mut output);
            varint(bytes.len(), &mut output);
        }
        let directory_end = output.len();
        output.extend_from_slice(&body);
        // Decode the actual directory and re-hash every referenced body, including
        // prefix collisions. This is independent of the offsets used to write it.
        let known: BTreeSet<_> = objects.iter().map(|o| o.0.clone()).collect();
        let mut pos = 12;
        for _ in objects {
            let prefix = &output[pos..pos + 4];
            pos += 4;
            let offset = read_varint(&output, &mut pos);
            let len = read_varint(&output, &mut pos);
            let bytes = output[directory_end + offset..directory_end + offset + len].to_vec();
            let envelope: Value = serde_json::from_slice(&bytes).unwrap();
            let kind: JournalObjectKind = serde_json::from_value(envelope["kind"].clone()).unwrap();
            let id = journal_object_id(kind, JOURNAL_SCHEMA_VERSION, &bytes);
            assert_eq!(&id.to_bytes()[..4], prefix);
            assert!(known.contains(&id));
        }
    } else {
        output.extend_from_slice(&body);
    }
    (output, offsets)
}
fn write_pack(
    db: &mut Db,
    id: usize,
    objects: &[(JournalObjectId, JournalObjectKind, Vec<u8>, u8)],
    compact: bool,
    shared: bool,
) {
    if objects.is_empty() {
        return;
    }
    let (bytes, offsets) = pack(objects, compact);
    let ciphertext = disk::seal(&bytes, MAGIC, true);
    let mut ordinary = vec![0u8; objects.len().div_ceil(8)];
    let mut protected = ordinary.clone();
    for (i, (_, _, _, bits)) in objects.iter().enumerate() {
        if bits & 1 != 0 {
            ordinary[i / 8] |= 1 << (i % 8);
        }
        if bits & 2 != 0 {
            protected[i / 8] |= 1 << (i % 8);
        }
    }
    db.conn
        .execute(
            "INSERT INTO pack VALUES (?1,?2,?3,?4,?5,?6)",
            params![SESSION, id, ciphertext, vec![0u8; 24], ordinary, protected],
        )
        .unwrap();
    db.account("pack","length(session_id)+length(ciphertext)+length(nonce)+length(ordinary_members)+length(protected_members)",SESSION.len()+ciphertext.len()+24+ordinary.len()+protected.len());
    if !compact {
        for (index, (object, _, body, bits)) in objects.iter().enumerate() {
            for (bit, domain) in if shared {
                vec![(3, "shared")]
            } else {
                vec![(1, "ordinary"), (2, "protected")]
            } {
                if bits & bit == 0 {
                    continue;
                }
                db.conn
                    .execute(
                        "INSERT INTO object_index VALUES (?1,?2,?3,?4,?5,?6)",
                        params![
                            SESSION,
                            domain,
                            object.to_bytes().as_slice(),
                            id,
                            offsets[index],
                            body.len()
                        ],
                    )
                    .unwrap();
                db.account(
                    "object_index",
                    "length(session_id)+length(domain)+length(object_key)",
                    SESSION.len() + domain.len() + 32,
                );
            }
        }
    }
}
fn packed(root: &Path, fixture: &fixture::Fixture, after: bool, lever: u8, op_mode: u8) -> Size {
    let exports = if after {
        &fixture.after
    } else {
        &fixture.before
    };
    let all = fixture::objects(exports);
    let compact = lever == 3;
    let shared = lever >= 2 && after;
    let ddl = format!(
        "{PACK_DDL} {} {}",
        include_str!("operation.sql"),
        if compact { "" } else { INDEX_DDL }
    );
    let mut db = Db::new(root, &ddl);
    db.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
    if shared {
        let mut objects: Vec<_> = all
            .into_iter()
            .map(|(id, (kind, bytes, bits))| (id, kind, bytes, bits))
            .collect();
        objects.sort_by(|a, b| a.1.as_str().cmp(b.1.as_str()).then(a.0.cmp(&b.0)));
        write_pack(&mut db, 0, &objects, compact, true);
    } else {
        for (index, batch) in fixture.batches.iter().enumerate() {
            let bit = if batch.domain == "ordinary" { 1 } else { 2 };
            let objects: Vec<_> = batch
                .ids
                .iter()
                .filter_map(|id| {
                    all.get(id)
                        .filter(|v| v.2 & bit != 0)
                        .map(|(kind, bytes, _)| (id.clone(), *kind, bytes.clone(), bit))
                })
                .collect();
            write_pack(&mut db, index, &objects, compact, false);
        }
    }
    head_rows(&mut db, exports, false);
    operations(&mut db, exports, op_mode);
    db.conn.execute_batch("COMMIT").unwrap();
    db.finish()
}
fn kinds(all: &Objects, physical: u64) -> BTreeMap<String, Kind> {
    let mut out = BTreeMap::<String, Kind>::new();
    let mut grouped = BTreeMap::<String, Vec<u8>>::new();
    for (kind, bytes, bits) in all.values() {
        grouped
            .entry(kind.as_str().into())
            .or_default()
            .extend_from_slice(bytes);
        let entry = out.entry(kind.as_str().into()).or_default();
        entry.objects += 1;
        entry.memberships += bits.count_ones() as usize;
        entry.logical_bytes += bytes.len();
        entry.standalone_zstd_bytes += disk::compress(bytes).len();
        let sealed = disk::seal(bytes, MAGIC, false).len() + 24;
        entry.d4_body_ciphertext_nonce_bytes += sealed;
        entry.d4_body_membership_key_value_bytes += 32
            + DATE.len()
            + (SESSION.len() + 32) * (bits.count_ones() as usize)
            + if bits & 1 != 0 { 8 } else { 0 }
            + if bits & 2 != 0 { 9 } else { 0 };
    }
    for (kind, bytes) in grouped {
        out.get_mut(&kind).unwrap().kind_group_zstd_bytes = disk::compress(&bytes).len();
    }
    let values: usize = out
        .values()
        .map(|k| k.d4_body_ciphertext_nonce_bytes + k.d4_body_membership_key_value_bytes)
        .sum();
    let overhead = physical as i64 - values as i64;
    for kind in out.values_mut() {
        kind.allocated_row_index_page_overhead = overhead
            * (kind.d4_body_ciphertext_nonce_bytes + kind.d4_body_membership_key_value_bytes)
                as i64
            / values as i64;
    }
    out
}
fn references(value: &Value, all: &Objects, out: &mut Vec<JournalObjectId>) {
    match value {
        Value::String(s) => {
            let id = if s.len() == 64 {
                JournalObjectId::parse(s.clone()).ok()
            } else if s.len() == 40 {
                serde_json::from_value::<JournalCompactObjectId>(value.clone())
                    .ok()
                    .and_then(|s| s.to_object().ok())
            } else {
                None
            };
            if let Some(id) = id.filter(|id| all.contains_key(id)) {
                out.push(id);
            }
        }
        Value::Array(values) => {
            for value in values {
                references(value, all, out);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                references(value, all, out);
            }
        }
        _ => {}
    }
}
fn closure(id: &JournalObjectId, all: &Objects) -> BTreeSet<JournalObjectId> {
    let mut found = BTreeSet::new();
    let mut pending = vec![id.clone()];
    while let Some(id) = pending.pop() {
        if !found.insert(id.clone()) {
            continue;
        }
        let value: Value = serde_json::from_slice(&all[&id].1).unwrap();
        references(&value["value"], all, &mut pending);
    }
    found
}
fn count_kinds(
    ids: impl Iterator<Item = JournalObjectId>,
    all: &Objects,
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for id in ids {
        *counts.entry(all[&id].0.as_str().into()).or_default() += 1;
    }
    counts
}
fn manifests(all: &Objects) -> Vec<ManifestClosure> {
    let mut shared = BTreeSet::new();
    for (id, (kind, _, _)) in all {
        if *kind == JournalObjectKind::ExtensionReference {
            shared.extend(closure(id, all));
        }
    }
    let cache = shared.clone();
    let mut result = Vec::new();
    for (id, (kind, _, _)) in all {
        if *kind != JournalObjectKind::ManifestReference {
            continue;
        }
        let ids = closure(id, all);
        let new = count_kinds(ids.difference(&shared).cloned(), all);
        let shared_with_cache = ids.intersection(&cache).count();
        shared.extend(ids.iter().cloned());
        result.push(ManifestClosure {
            manifest: result.len(),
            kinds: count_kinds(ids.into_iter(), all),
            new_vs_cache_and_earlier_manifests: new,
            shared_with_cache,
        });
    }
    result
}
pub(crate) fn measure(root: &Path, fixture: fixture::Fixture) -> Report {
    let mut layouts = Vec::new();
    for lever in 0..=3 {
        let (before, after, bare_before, bare_after) = if lever == 0 {
            (
                d4(root, &fixture.before, true),
                d4(root, &fixture.after, true),
                d4(root, &fixture.before, false).physical_bytes,
                d4(root, &fixture.after, false).physical_bytes,
            )
        } else {
            (
                packed(root, &fixture, false, lever, 1),
                packed(root, &fixture, true, lever, 1),
                packed(root, &fixture, false, lever, 0).physical_bytes,
                packed(root, &fixture, true, lever, 0).physical_bytes,
            )
        };
        layouts.push(Layout {
            name: ["D4", "S1", "S1+S2", "S1+S2+S3"][lever as usize].into(),
            operation_physical_bytes_before: before.physical_bytes as i64 - bare_before as i64,
            operation_physical_bytes_after: after.physical_bytes as i64 - bare_after as i64,
            before,
            after,
            before_without_operations: bare_before,
            after_without_operations: bare_after,
        });
    }
    let commits = fixture
        .before
        .iter()
        .map(|(_, e)| e.operation_count)
        .sum::<usize>();
    let mut logical = 0;
    let mut d4bytes = 0;
    let mut packedbytes = 0;
    let mut windowbytes = 0;
    let mut window_count = 0;
    let mut epochs = 0;
    for (_, export) in &fixture.before {
        let current = export.heads.values().next().unwrap();
        let last = export
            .operations
            .iter()
            .filter(|op| op.result.writer_epoch == current.writer_epoch)
            .map(|op| op.result.revision)
            .max();
        let window: Vec<_> = export
            .operations
            .iter()
            .filter(|op| {
                op.result.revision.0 >= current.revision.0.saturating_sub(64)
                    || (op.result.writer_epoch == current.writer_epoch
                        && Some(op.result.revision) == last)
            })
            .collect();
        epochs += export
            .operations
            .iter()
            .filter(|op| op.result.writer_epoch == current.writer_epoch)
            .count();
        window_count += window.len();
        logical += serde_json::to_vec(&export.operations).unwrap().len();
        d4bytes += export
            .operations
            .iter()
            .map(|op| disk::seal(&serde_json::to_vec(&op.result).unwrap(), MAGIC, false).len() + 24)
            .sum::<usize>();
        packedbytes += disk::seal(
            &serde_json::to_vec(&export.operations).unwrap(),
            MAGIC,
            true,
        )
        .len()
            + 24;
        windowbytes += disk::seal(&serde_json::to_vec(&window).unwrap(), MAGIC, true).len() + 24;
    }
    let all_before = fixture::objects(&fixture.before);
    let all_after = fixture::objects(&fixture.after);
    let l1 = disk::legacy(root, &fixture.legacy, 1);
    let l2 = disk::legacy(root, &fixture.legacy, 2);
    let s123_s4_s5_before = packed(root, &fixture, false, 3, 2);
    let s123_s4_s5_after = packed(root, &fixture, true, 3, 3);
    let gate_ratio = (l1.physical_bytes != 0)
        .then(|| s123_s4_s5_after.physical_bytes as f64 / l1.physical_bytes as f64);
    let head = fixture.after[0].1.heads.values().next().unwrap();
    let provider_cache_enabled = fixture.name != "A";
    // Task-1.4 archived simulator payload delta. All A/B records are in the
    // replay window. C: 819747 lifetime - 23769 window = 795978 bytes.
    // Historical simulator delta only. The former projected-size fields are
    // now aliases of the actual S5 measurement; never subtract S5 twice.
    let s5_delta = if fixture.name == "C" { 795_978 } else { 0 };
    let projected = fixture.packed_after.physical_bytes;
    Report {
        retained_operations_before: fixture
            .before
            .iter()
            .map(|(_, export)| export.operation_count)
            .sum(),
        retained_operations_after: fixture
            .after
            .iter()
            .map(|(_, export)| export.operation_count)
            .sum(),
        packed_before_l2_ratio: fixture.packed_before.physical_bytes as f64
            / l2.physical_bytes as f64,
        packed_after_l2_ratio: fixture.packed_after.physical_bytes as f64
            / l2.physical_bytes as f64,
        projected_after_s5_l2_ratio: projected as f64 / l2.physical_bytes as f64,
        projected_after_s5_bytes: projected,
        s5_simulator_payload_delta: s5_delta,
        packed_before: fixture.packed_before,
        packed_after: fixture.packed_after,
        provider_cache_enabled,
        fixture: fixture.name,
        n: fixture.n,
        tools: fixture.tools,
        final_messages: head.snapshot.history.len as usize,
        manifests_retained: head.snapshot.manifests.len,
        idle_legacy_blobs: 1 + usize::from(fixture.legacy.snapshot.is_some()),
        legacy_digest_skips: fixture.legacy.skipped,
        legacy_canonical_difference_fields: fixture.legacy.canonical_difference_fields.clone(),
        legacy_l1: l1,
        legacy_l2: l2,
        kinds_before: kinds(&all_before, layouts[0].before_without_operations),
        kinds_after: kinds(&all_after, layouts[0].after_without_operations),
        layouts,
        operations: Operations {
            commits,
            turns: fixture.turns,
            commits_per_turn: commits as f64 / fixture.turns as f64,
            logical_bytes: logical,
            d4_ciphertext_nonce_bytes: d4bytes,
            packed_ciphertext_nonce_bytes: packedbytes,
            window_records: window_count,
            window_ciphertext_nonce_bytes: windowbytes,
            current_epoch_records: epochs,
            packed_bytes_per_commit: packedbytes as f64 / commits as f64,
        },
        manifest_closures: manifests(&all_after),
        s123_s4_s5_before,
        s123_s4_s5_after,
        gate_ratio,
    }
}

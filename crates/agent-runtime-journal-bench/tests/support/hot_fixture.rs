use super::*;
use agent_runtime::runtime::Runtime;
use agent_runtime_core::journal::profile::{self, Observer, Sample};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::io::{BufWriter, Write};
use std::time::Instant;

pub(crate) const LABELS: &[&str] = &[
    "rt.encoding.canonical_journal_json_len",
    "rt.assembler.structured_sequence",
    "rt.assembler.manifest",
    "rt.assembler.snapshot_cached",
    "rt.transition.commit",
    "rt.transition.project_snapshot",
    "rt.draft.manifest",
    "rt.links.memoized_closure",
    "rt.links.draft_reachable_closure",
    "rt.links.reachable_closure",
    "rt.staged.materialize_staged_checkpoint",
    "rt.staged.materialize_staged_snapshot",
    "rt.memo.get",
    "rt.memo.hit",
    "rt.memo.miss",
    "rt.memo.eviction",
    "rt.memo.insert",
    "rt.memo.release",
    "rt.memo.memo_entry",
    "rt.memo.writer_memo",
    "rt.memo.seed_memo",
    "rt.memo.validated_root",
    "rt.memo.commit_memo_entry",
    "rt.mod.open",
    "rt.mod.commit",
    "rt.mod.renew",
    "rt.mod.close",
    "rt.mod.collect",
    "rt.gc.restart.fence",
    "rt.gc.restart.proof",
    "rt.gc.restart.epoch",
    "rt.gc.restart.page",
    "rt.persistence.hydrate_state",
    "rt.persistence.closure_objects",
    "rt.persistence.pin_roots",
    "rt.persistence.pin_closure",
    "rt.persistence.collection_live",
    "rt.persistence.write",
    "rt.persistence.publish",
    "rt.persistence.commit_inner",
    "rt.controls.collect_inner",
    "rt.controls.collect_sessions",
];

#[derive(Debug, Default, Clone, Serialize)]
pub(crate) struct Metric {
    pub calls: u64,
    pub nanos: u64,
    pub max_nanos: u64,
    pub units: u64,
    pub bytes: u64,
}
impl Metric {
    fn add(&mut self, s: &Sample) {
        self.calls += 1;
        self.nanos += s.nanos;
        self.max_nanos = self.max_nanos.max(s.nanos);
        self.units += s.units;
        self.bytes += s.bytes;
    }
}
#[derive(Debug, Serialize)]
pub(crate) struct Operation {
    pub boundary: String,
    pub turn: usize,
    pub wall_nanos: u64,
    pub stages: BTreeMap<String, Metric>,
    pub pack_write_bytes: Option<u64>,
    pub attribution_nanos: BTreeMap<String, u64>,
}
#[derive(Debug, Serialize)]
pub(crate) struct Case {
    pub fixture: String,
    pub profile_revision: &'static str,
    pub n: usize,
    pub tools: bool,
    pub sample: usize,
    pub context: String,
    pub policy: String,
    pub observer: bool,
    pub collection: bool,
    pub memo_budget: usize,
    pub fixture_fingerprint: String,
    pub canonical_snapshot: String,
    pub ordinary_roots: String,
    pub protected_roots: String,
    pub operations: Vec<Operation>,
    pub final_messages: usize,
    pub pack_bytes: u64,
    pub missing_metrics: Value,
    pub memory_snapshots: Vec<Value>,
}
#[derive(Debug)]
pub(crate) struct Reporter {
    pub directory: PathBuf,
    spans: BufWriter<std::fs::File>,
    calls: BufWriter<std::fs::File>,
    collect: BufWriter<std::fs::File>,
    meter: Option<Arc<PackedSqliteJournalStorage>>,
}
impl Reporter {
    pub(crate) fn new(directory: &std::path::Path) -> Self {
        std::fs::create_dir_all(directory).unwrap();
        Self {
            directory: directory.into(),
            meter: None,
            spans: BufWriter::new(std::fs::File::create(directory.join("spans.jsonl")).unwrap()),
            collect: BufWriter::new(
                std::fs::File::create(directory.join("collect.jsonl")).unwrap(),
            ),
            calls: BufWriter::new(
                std::fs::File::create(directory.join("operations.jsonl")).unwrap(),
            ),
        }
    }
    pub(crate) fn measure<'a, F: Future + 'a>(
        &'a mut self,
        case: &str,
        turn: usize,
        boundary: &str,
        enabled: bool,
        f: F,
    ) -> std::pin::Pin<Box<dyn Future<Output = (F::Output, Operation)> + 'a>> {
        let case = case.to_owned();
        let boundary = boundary.to_owned();
        Box::pin(async move {
            let observer = Observer::default();
            let before = self.meter.as_ref().map(|s| s.pack_write_bytes().unwrap());
            let start = Instant::now();
            let result = if enabled {
                observer
                    .run(profile::instrument("bench.operation", f))
                    .await
            } else {
                f.await
            };
            let wall_nanos = start.elapsed().as_nanos() as u64;
            let pack_write_bytes = self
                .meter
                .as_ref()
                .zip(before)
                .map(|(s, b)| s.pack_write_bytes().unwrap() - b);
            let samples = observer.drain().unwrap();
            let attribution_nanos = attribute(&samples, wall_nanos);
            let mut stages = BTreeMap::<String, Metric>::new();
            for sample in &samples {
                stages.entry(sample.label.into()).or_default().add(sample);
                serde_json::to_writer(
                    &mut self.spans,
                    &json!({"case":case,"turn":turn,"boundary":boundary,"span":sample}),
                )
                .unwrap();
                self.spans.write_all(b"\n").unwrap();
            }
            let by_id: BTreeMap<_, _> = samples.iter().map(|s| (s.id, s)).collect();
            for call in samples.iter().filter(|s| s.label == "rt.mod.collect") {
                let mut holds = Vec::new();
                let mut waits = Vec::new();
                // Per-call collection diagnostics: label -> [units, bytes].
                let mut diag = BTreeMap::<&str, [u64; 2]>::new();
                for event in samples.iter().filter(|s| s.label.starts_with("rt.gc.")) {
                    let mut parent = event.parent;
                    while let Some(id) = parent {
                        if id == call.id {
                            let slot = diag.entry(event.label).or_default();
                            slot[0] += event.units;
                            slot[1] += event.bytes;
                            break;
                        }
                        parent = by_id.get(&id).and_then(|s| s.parent);
                    }
                }
                for interval in &samples {
                    if !["rt.gate.hold", "rt.gate.wait"].contains(&interval.label) {
                        continue;
                    }
                    let mut parent = interval.parent;
                    while let Some(id) = parent {
                        if id == call.id {
                            if interval.label == "rt.gate.hold" {
                                holds.push(interval);
                            } else {
                                waits.push(interval);
                            }
                            break;
                        }
                        parent = by_id.get(&id).and_then(|s| s.parent);
                    }
                }
                let physical = samples
                    .iter()
                    .find(|s| {
                        s.label == "bench.collect.pack_write_bytes" && s.parent == call.parent
                    })
                    .map(|s| s.bytes);
                let examined = samples
                    .iter()
                    .find(|s| {
                        s.label == "bench.collect.examined_members" && s.parent == call.parent
                    })
                    .map(|s| s.units);
                serde_json::to_writer(&mut self.collect,&json!({"case":case,"turn":turn,"boundary":boundary,"call":call,"held_intervals":holds,"wait_intervals":waits,"pack_write_bytes":physical,"examined_members":examined,"diag":diag})).unwrap();
                self.collect.write_all(b"\n").unwrap();
            }
            self.collect.flush().unwrap();
            for label in LABELS {
                stages.entry((*label).into()).or_default();
            }
            let operation = Operation {
                boundary,
                turn,
                wall_nanos,
                stages,
                pack_write_bytes,
                attribution_nanos,
            };
            serde_json::to_writer(&mut self.calls, &json!({"case":case,"operation":operation}))
                .unwrap();
            self.calls.write_all(b"\n").unwrap();
            self.spans.flush().unwrap();
            self.calls.flush().unwrap();
            (result, operation)
        })
    }
    pub(crate) fn save(&self, cases: &[Case]) {
        std::fs::write(
            self.directory.join("summary.json"),
            serde_json::to_vec_pretty(cases).unwrap(),
        )
        .unwrap();
        let mut md = String::from(
            "Runtime reference adapter, release; inclusive stages, last turn includes S2 and both collections. Not Forge sqlx/AEAD or real L2.\n\n| Fixture | N | Shape | Sample | Context | Observer | Last charged turn ms | Writer prep ms | Engine commits ms | Candidate ms | Memo ms | Checkpoint ms | Collect ms | Max call ms | Max gate ms | Warm resume ms |\n| --- | ---: | --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
        );
        use std::fmt::Write as _;
        for case in cases {
            let last = case
                .operations
                .iter()
                .rev()
                .find(|o| o.boundary == "turn")
                .unwrap();
            let ms = |label: &str| last.stages.get(label).map_or(0.0, |m| m.nanos as f64 / 1e6);
            let max = |label: &str| {
                case.operations
                    .iter()
                    .filter_map(|o| o.stages.get(label))
                    .map(|m| m.max_nanos)
                    .max()
                    .unwrap_or(0) as f64
                    / 1e6
            };
            let warm = case
                .operations
                .iter()
                .find(|o| o.boundary == "warm_resume")
                .map_or(0.0, |o| o.wall_nanos as f64 / 1e6);
            writeln!(&mut md,"| {} | {} | {} | {} | {} | {} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} |",case.fixture,case.n,if case.tools {"tool"} else {"plain"},case.sample,case.context,case.observer,last.wall_nanos as f64/1e6,ms("rt.writer.preparation"),ms("rt.mod.commit"),ms("rt.transition.project_snapshot"),ms("rt.memo.commit_memo_entry"),ms("rt.staged.materialize_staged_checkpoint"),ms("rt.mod.collect"),max("rt.mod.collect"),max("rt.gate.hold"),warm).unwrap();
        }
        std::fs::write(self.directory.join("summary.md"), md).unwrap();
    }
}
pub(crate) fn fingerprint(value: &impl Serialize) -> String {
    format!("{:x}", Sha256::digest(serde_json::to_vec(value).unwrap()))
}
/// Hashes generated bytes and exact provider scripts without exposing their content.
pub(crate) fn fixture_fingerprint(name: &str, n: usize, tools: bool) -> String {
    let turns = turns(name, n, tools);
    let generator = if name.starts_with('C') { "C" } else { name };
    let mut text = Text(0x9876_1234_5678_abcd);
    let inputs: Vec<_> = (0..turns)
        .map(|turn| input(name, tools, turn, &mut text))
        .collect();
    // ScriptedStream is Debug rather than Serialize; this preserves event order and bytes.
    let scripts = format!("{:?}", scripts(generator, tools, turns));
    fingerprint(&(
        seed(if name.starts_with('C') { 0 } else { n }, name != "A"),
        scripts,
        inputs,
        100u64,
        32u64,
        "bench-cache-1",
        "bench-local-endpoint",
        "endpoint-1",
    ))
}
fn turns(name: &str, n: usize, tools: bool) -> usize {
    if name.starts_with('C') {
        if tools { n / 4 } else { n / 2 }
    } else {
        2
    }
}
fn input(name: &str, tools: bool, turn: usize, text: &mut Text) -> String {
    if name == "A" {
        if turn == 0 {
            "bootstrap turn".into()
        } else if tools {
            "use the tool".into()
        } else {
            "one more message".into()
        }
    } else {
        text.prose(100, 800)
    }
}
fn runtime<S: SessionStore + CheckpointStore + 'static>(
    store: Arc<S>,
    name: &str,
    tools: bool,
    scripts: Vec<ScriptedStream>,
) -> Runtime {
    let mut capabilities = Capabilities::basic_streaming();
    if name != "A" {
        capabilities.cache = true;
        capabilities.override_prompt_cache(PromptCacheControl::Implicit);
    }
    let provider = Arc::new(FakeProvider::new("fake", capabilities, scripts));
    let mut builder = RuntimeBuilder::new(ModelId::new("fake"))
        .provider(provider)
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(4_000_000, 4_000_000, 4096),
        ))
        .clock(Arc::new(FixedClock))
        .session_store(store.clone())
        .checkpoint_store(store);
    if name != "A" {
        builder = builder
            .cache_capability(ProviderCacheCapability::from_control(
                RegistryRevision::new("bench-cache-1"),
                "fake",
                PromptCacheControl::Implicit,
            ))
            .cache_endpoint_identity(CacheEndpointIdentity::from_opaque(
                "bench-local-endpoint",
                RegistryRevision::new("endpoint-1"),
            ));
    }
    if tools {
        builder = builder
            .legacy_approval_authority()
            .approval(Arc::new(AllowAll))
            .tool(Arc::new(Echo));
    }
    builder.build().unwrap()
}
#[derive(Debug)]
struct Conservative;
impl JournalProjectionPolicy for Conservative {
    fn revision(&self) -> Option<u64> {
        Some(1)
    }
    fn project_delta(
        &self,
        _base: &agent_runtime_core::journal::JournalSnapshotView,
        delta: &agent_runtime_core::journal::JournalTransitionDelta,
        _revision: u64,
    ) -> Result<agent_runtime_core::journal::JournalDeltaProjection, RuntimeError> {
        let mut delta = delta.clone();
        delta
            .extension_updates
            .insert("bench.optional_sensitive_diagnostic".to_owned(), None);
        if delta
            .extension_references
            .contains_key("bench.optional_sensitive_diagnostic")
        {
            return Ok(agent_runtime_core::journal::JournalDeltaProjection::FullProjectionRequired);
        }
        Ok(agent_runtime_core::journal::JournalDeltaProjection::Projected(Box::new(delta)))
    }
    fn project(&self, mut s: SessionSnapshot) -> Result<SessionSnapshot, RuntimeError> {
        s.extension_state
            .remove("bench.optional_sensitive_diagnostic");
        Ok(s)
    }
}
#[derive(Debug)]
struct Store {
    ordinary: JournalEngine<PackedSqliteJournalStorage>,
    protected: JournalEngine<PackedSqliteJournalStorage>,
    storage: Arc<PackedSqliteJournalStorage>,
    legacy: Legacy,
}
impl Store {
    fn new(storage: Arc<PackedSqliteJournalStorage>, conservative: bool, memo: usize) -> Self {
        let config = JournalEngineConfig {
            validated_root_memo_bytes: memo,
            collection_enabled: true,
        };
        let ordinary = JournalEngine::ordinary(
            storage.clone(),
            JournalStorageId::new("forge.session-journal.v1.ordinary"),
            if conservative {
                Arc::new(Conservative)
            } else {
                Arc::new(IdentityJournalProjection)
            },
            config.clone(),
        );
        let protected = JournalEngine::protected(
            storage.clone(),
            JournalStorageId::new("forge.session-journal.v1.protected"),
            config,
        );
        Self {
            ordinary,
            protected,
            storage,
            legacy: Legacy::default(),
        }
    }
    async fn maintenance(&self, id: &SessionId, collection: bool) {
        self.storage.post_terminal(id).unwrap();
        if collection {
            self.collect().await;
        }
    }
    async fn collect(&self) {
        profile::instrument("bench.full_drain", async {
            for journal in [&self.ordinary, &self.protected] {
                for step in 0..100_000 {
                    // Bounded calls (P4): continue the engine's pass while it
                    // reports more work; each call is one measured collect.
                    loop {
                        let more = profile::instrument("bench.collect.call", async {
                            let before = self.storage.pack_write_bytes().unwrap();
                            let report = journal
                                .collect(JournalGcRequest {
                                    storage_id: journal.storage_id(),
                                    expected_root_epoch: journal.root_epoch().unwrap(),
                                    max_objects: 20_000,
                                    work_budget: Default::default(),
                                })
                                .await
                                .unwrap();
                            profile::event(
                                "bench.collect.pack_write_bytes",
                                1,
                                self.storage.pack_write_bytes().unwrap() - before,
                            );
                            profile::event(
                                "bench.collect.examined_members",
                                report.examined,
                                report.reclaimed_bytes,
                            );
                            profile::event("bench.collect.deleted", report.deleted, 0);
                            report.has_more
                        })
                        .await;
                        if !more {
                            break;
                        }
                    }
                    if self
                        .storage
                        .dirty_sessions(
                            &JournalDomain {
                                storage_id: journal.storage_id(),
                                role: if std::ptr::eq(journal, &self.ordinary) {
                                    JournalRole::Ordinary
                                } else {
                                    JournalRole::Protected
                                },
                            },
                            None,
                            1,
                        )
                        .await
                        .unwrap()
                        .is_empty()
                    {
                        break;
                    }
                    assert!(step < 99_999, "v0.9.0 drain made no terminating progress");
                }
            }
        })
        .await;
    }
}
#[async_trait]
impl SessionStore for Store {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(Arc::new(self.ordinary.clone()))
    }
    async fn load(&self, id: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        SessionStore::load(&self.legacy, id).await
    }
    async fn save(&self, _: &SessionSnapshot) -> Result<(), RuntimeError> {
        panic!("native save fell back to legacy")
    }
}
#[async_trait]
impl CheckpointStore for Store {
    fn journal(&self) -> Option<Arc<dyn SessionJournal>> {
        Some(Arc::new(self.protected.clone()))
    }
    async fn load_latest(&self, id: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.legacy.load_latest(id).await
    }
    async fn save(&self, _: &TurnCheckpoint) -> Result<(), RuntimeError> {
        panic!("native checkpoint fell back to legacy")
    }
}
pub(crate) struct Parameters<'a> {
    pub name: &'a str,
    pub n: usize,
    pub tools: bool,
    pub sample: usize,
    pub context: &'a str,
    pub enabled: bool,
    pub conservative: bool,
    pub memo: usize,
    pub collection: bool,
}
pub(crate) async fn run(reporter: &mut Reporter, p: Parameters<'_>) -> (Case, LegacyRow) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let fixture_directory = reporter.directory.join("fixtures");
    std::fs::create_dir_all(&fixture_directory).unwrap();
    let path = fixture_directory.join(format!(
        "hot-path-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let file = SqliteFile {
        path: path.clone(),
        empty: 0,
    };
    let storage = Arc::new(PackedSqliteJournalStorage::open(&path).unwrap());
    let store = Arc::new(Store::new(storage.clone(), p.conservative, p.memo));
    let c = p.name.starts_with('C');
    let generator = if c { "C" } else { p.name };
    let turns = turns(p.name, p.n, p.tools);
    let id = SessionId::new(format!(
        "task-27-{}-{}",
        if p.tools { "tool" } else { "plain" },
        p.n
    ));
    let mut scripted = scripts(generator, p.tools, turns).into_iter();
    let mut make = || {
        let count = if c {
            if p.tools { 2 } else { 1 }
        } else {
            scripted.len()
        };
        runtime(
            store.clone(),
            p.name,
            p.tools,
            scripted.by_ref().take(count).collect(),
        )
    };
    let key = format!(
        "{}-{}-{}-{}-{}-{}-{}-{}",
        p.context, p.name, p.n, p.tools, p.sample, p.enabled, p.conservative, p.memo
    );
    reporter.meter = Some(storage.clone());
    let mut operations = Vec::new();
    let mut memory_snapshots = Vec::new();
    let rt = make();
    let (history, op) = reporter
        .measure(&key, 0, "seed_bootstrap", p.enabled, async {
            if c { vec![] } else { seed(p.n, p.name != "A") }
        })
        .await;
    operations.push(op);
    let (session, op) = reporter
        .measure(
            &key,
            0,
            "initial_open",
            p.enabled,
            rt.start_session(StartSession::create(id.clone(), history)),
        )
        .await;
    operations.push(op);
    let mut session = session.unwrap();
    let mut text = Text(0x9876_1234_5678_abcd);
    for turn in 0..turns {
        if c && turn != 0 {
            let rt = make();
            let (opened, op) = reporter
                .measure(
                    &key,
                    turn,
                    "writer_reopen",
                    p.enabled,
                    rt.start_session(StartSession::resume(id.clone())),
                )
                .await;
            session = opened.unwrap();
            operations.push(op);
        }
        let value = input(p.name, p.tools, turn, &mut text);
        let (_, op) = reporter
            .measure(&key, turn, "turn", p.enabled, async {
                session.run(UserInput::text(value)).await.unwrap();
                store.maintenance(&id, p.collection).await;
            })
            .await;
        operations.push(op);
        memory_snapshots.push(json!({"turn":turn,"ordinary":store.ordinary.profile_state().unwrap(),"protected":store.protected.profile_state().unwrap(),"storage_cache":store.storage.profile_cache().unwrap()}));
        if c && turn + 1 != turns {
            let (_, op) = reporter
                .measure(&key, turn, "writer_shutdown", p.enabled, session.shutdown())
                .await;
            operations.push(op);
        }
        if c && (turn + 1) % 50 == 0 {
            eprintln!("{key}: {}/{} turns", turn + 1, turns);
        }
    }
    let (_, op) = reporter
        .measure(&key, turns, "persist", p.enabled, async {
            session.persist().await.unwrap();
            session.persist().await.unwrap();
        })
        .await;
    operations.push(op);
    let snapshot = session.snapshot();
    if c {
        assert_eq!(snapshot.history.len(), p.n);
        assert_eq!(snapshot.manifests.len(), 32.min(turns));
    }
    if p.name != "A" {
        assert!(
            snapshot.extension_state[RUNTIME_PREVIOUS_CACHE_NAMESPACE].value["cache_identity"]
                .is_object()
        );
    }
    let (_, op) = reporter
        .measure(&key, turns, "shutdown_drain", p.enabled, async {
            session.shutdown().await.unwrap();
            store.collect().await;
        })
        .await;
    operations.push(op);
    // Root selection and full Runtime restores are distinct operations.
    for warm in [false, true] {
        let probe = if warm {
            store.clone()
        } else {
            Arc::new(Store::new(
                Arc::new(PackedSqliteJournalStorage::open(&path).unwrap()),
                p.conservative,
                p.memo,
            ))
        };
        reporter.meter = Some(probe.storage.clone());
        if warm {
            let (_, op) = reporter
                .measure(
                    &key,
                    turns,
                    "warm_root_prepare",
                    p.enabled,
                    root_probe(&probe, &id),
                )
                .await;
            operations.push(op);
        }
        let (_, op) = reporter
            .measure(
                &key,
                turns,
                if warm { "warm_root" } else { "cold_root" },
                p.enabled,
                root_probe(&probe, &id),
            )
            .await;
        operations.push(op);
        let resume_probe = if warm {
            probe
        } else {
            Arc::new(Store::new(
                Arc::new(PackedSqliteJournalStorage::open(&path).unwrap()),
                p.conservative,
                p.memo,
            ))
        };
        reporter.meter = Some(resume_probe.storage.clone());
        let rt = runtime(resume_probe, p.name, p.tools, vec![]);
        let (opened, op) = reporter
            .measure(
                &key,
                turns,
                if warm { "warm_resume" } else { "cold_resume" },
                p.enabled,
                rt.start_session(StartSession::resume(id.clone())),
            )
            .await;
        let opened = opened.unwrap();
        assert_eq!(opened.snapshot().history, snapshot.history);
        operations.push(op);
        let (closed, op) = reporter
            .measure(
                &key,
                turns,
                if warm {
                    "warm_resume_cleanup"
                } else {
                    "cold_resume_cleanup"
                },
                p.enabled,
                opened.shutdown(),
            )
            .await;
        closed.unwrap();
        operations.push(op);
    }
    reporter.meter = Some(storage.clone());
    let (_, op) = reporter
        .measure(&key, turns, "extension_probe", p.enabled, async {
            let lease = store
                .ordinary
                .open(
                    &id,
                    JournalOpen {
                        root: JournalRootSelection::Latest,
                        expected_revision: None,
                        operation: JournalOperationId::new("bench-extension"),
                        pin: JournalPinId::new("bench-extension"),
                        intent: JournalOpenIntent::ReadOnly,
                    },
                )
                .await
                .unwrap();
            JournalReader::new(
                &store.ordinary,
                &lease,
                JournalReadLimits {
                    objects: 4_000_000,
                    encoded_bytes: 4 * 1024 * 1024 * 1024,
                    entries: 16_000_000,
                },
            )
            .unwrap()
            .extension(RUNTIME_PREVIOUS_CACHE_NAMESPACE)
            .await
            .unwrap();
            store.ordinary.close(lease).await.unwrap();
        })
        .await;
    operations.push(op);
    let ordinary = storage
        .load(
            &JournalDomain {
                storage_id: store.ordinary.storage_id(),
                role: JournalRole::Ordinary,
            },
            &id,
        )
        .await
        .unwrap()
        .unwrap()
        .head
        .unwrap()
        .decode::<JournalHead>(JournalObjectKind::Head)
        .unwrap();
    let protected = storage
        .load(
            &JournalDomain {
                storage_id: store.protected.storage_id(),
                role: JournalRole::Protected,
            },
            &id,
        )
        .await
        .unwrap()
        .unwrap()
        .head
        .unwrap()
        .decode::<JournalHead>(JournalObjectKind::Head)
        .unwrap();
    let (legacy, legacy_snapshot) = legacy_run(reporter, &key, &p, &id, &mut operations).await;
    assert_eq!(legacy_snapshot.history, snapshot.history);
    let import_path = fixture_directory.join(format!(
        "hot-import-{}-{}.sqlite",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let import_file = SqliteFile {
        path: import_path.clone(),
        empty: 0,
    };
    let import_store = Arc::new(Store::new(
        Arc::new(PackedSqliteJournalStorage::open(&import_path).unwrap()),
        p.conservative,
        p.memo,
    ));
    *import_store.legacy.0.lock().unwrap() = legacy.clone();
    reporter.meter = Some(import_store.storage.clone());
    let import_runtime = runtime(import_store.clone(), p.name, p.tools, vec![]);
    let (imported, op) = reporter
        .measure(
            &key,
            turns,
            "first_import",
            p.enabled,
            import_runtime.start_session(StartSession::resume(id.clone())),
        )
        .await;
    let imported = imported.unwrap();
    assert_eq!(imported.snapshot().history, legacy_snapshot.history);
    operations.push(op);
    let (closed, op) = reporter
        .measure(
            &key,
            turns,
            "import_cleanup",
            p.enabled,
            imported.shutdown(),
        )
        .await;
    closed.unwrap();
    operations.push(op);
    reporter.meter = None;
    drop(import_runtime);
    drop(import_store);
    drop(import_file);
    let case = Case {
        profile_revision: "p2-authenticated-closure-v1",
        fixture: p.name.into(),
        n: p.n,
        tools: p.tools,
        sample: p.sample,
        context: p.context.into(),
        policy: if p.conservative {
            "conservative"
        } else {
            "identity"
        }
        .into(),
        observer: p.enabled,
        collection: p.collection,
        memo_budget: p.memo,
        fixture_fingerprint: fixture_fingerprint(p.name, p.n, p.tools),
        canonical_snapshot: fingerprint(&snapshot),
        ordinary_roots: fingerprint(&ordinary.snapshot),
        protected_roots: fingerprint(&protected.snapshot),
        operations,
        final_messages: snapshot.history.len(),
        pack_bytes: storage.pack_write_bytes().unwrap(),
        memory_snapshots,
        missing_metrics: json!({"allocator_heap":{"value":null,"reason":"no allocator hook; canonical memo charge is not heap"},"cursor_frontier":{"value":0,"reason":"v0.9.0 has no resumable cursor"},"persistent_version_depth":{"value":null,"reason":"persistent views have no linked version chain; exact per-writer depth telemetry is not instrumented"},"writer_working_state_bytes":{"value":null,"reason":"no non-invasive allocator instrumentation"},"shared_proof_retention_heap":{"value":null,"reason":"external proof reference counts and estimated memo charge are exposed; allocator heap is not"},"storage_cache_allocator_heap":{"value":null,"reason":"canonical body bytes and estimated location index bytes are exposed separately"},"semantic_nodes_copied":{"value":null,"reason":"targeted index/mutation work is counted; generic clone/API traversal and allocator-byte instrumentation remain incomplete"},"preflight_pages_yields_restarts":{"value":null,"reason":"P4 is not implemented"}}),
    };
    drop(store);
    drop(storage);
    drop(file);
    (case, legacy)
}

async fn legacy_run(
    reporter: &mut Reporter,
    key: &str,
    p: &Parameters<'_>,
    id: &SessionId,
    operations: &mut Vec<Operation>,
) -> (LegacyRow, SessionSnapshot) {
    reporter.meter = None;
    let legacy = Arc::new(Legacy::default());
    let c = p.name.starts_with('C');
    let count = turns(p.name, p.n, p.tools);
    let mut scripted = scripts(if c { "C" } else { p.name }, p.tools, count).into_iter();
    let mut make = || {
        let count = if c {
            if p.tools { 2 } else { 1 }
        } else {
            scripted.len()
        };
        runtime(
            legacy.clone(),
            p.name,
            p.tools,
            scripted.by_ref().take(count).collect(),
        )
    };
    let rt = make();
    let (opened, op) = reporter
        .measure(
            key,
            0,
            "legacy_initial_open",
            p.enabled,
            rt.start_session(StartSession::create(
                id.clone(),
                if c { vec![] } else { seed(p.n, p.name != "A") },
            )),
        )
        .await;
    operations.push(op);
    let mut session = opened.unwrap();
    let mut text = Text(0x9876_1234_5678_abcd);
    for turn in 0..count {
        if c && turn != 0 {
            session = make()
                .start_session(StartSession::resume(id.clone()))
                .await
                .unwrap();
        }
        let (result, op) = reporter
            .measure(
                key,
                turn,
                "legacy_turn",
                p.enabled,
                session.run(UserInput::text(input(p.name, p.tools, turn, &mut text))),
            )
            .await;
        result.unwrap();
        operations.push(op);
        if c && turn + 1 != count {
            session.shutdown().await.unwrap();
        }
    }
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    let snapshot = session.snapshot();
    session.shutdown().await.unwrap();
    let rt = runtime(legacy.clone(), p.name, p.tools, vec![]);
    let (resumed, op) = reporter
        .measure(
            key,
            count,
            "legacy_resume",
            p.enabled,
            rt.start_session(StartSession::resume(id.clone())),
        )
        .await;
    operations.push(op);
    resumed.unwrap().shutdown().await.unwrap();
    let row = legacy.0.lock().unwrap().clone();
    (row, snapshot)
}

pub(crate) async fn diagnostics(reporter: &mut Reporter) {
    let path = std::env::temp_dir().join(format!("hot-diagnostic-{}.sqlite", std::process::id()));
    let file = SqliteFile {
        path: path.clone(),
        empty: 0,
    };
    let storage = Arc::new(PackedSqliteJournalStorage::open(&path).unwrap());
    reporter.meter = Some(storage.clone());
    let store = Arc::new(Store::new(storage.clone(), true, 128 * 1024 * 1024));
    let id = SessionId::new("bench-large-live");
    let rt = runtime(store.clone(), "B", false, scripts("B", false, 2));
    let session = rt
        .start_session(StartSession::create(id.clone(), seed(4000, true)))
        .await
        .unwrap();
    session
        .run(UserInput::text("bootstrap turn"))
        .await
        .unwrap();
    session
        .run(UserInput::text("one more message"))
        .await
        .unwrap();
    session.shutdown().await.unwrap();
    store.collect().await;
    for index in 0..100 {
        let id = SessionId::new(format!("bench-unrelated-{index:03}"));
        let rt = runtime(store.clone(), "A", false, scripts("A", false, 1));
        let session = rt
            .start_session(StartSession::create(id, vec![]))
            .await
            .unwrap();
        session
            .run(UserInput::text("bootstrap turn"))
            .await
            .unwrap();
        session.shutdown().await.unwrap();
    }
    let mut operations = Vec::new();
    let (_, op) = reporter
        .measure(
            "diagnostics",
            0,
            "100_dirty_sessions",
            true,
            store.collect(),
        )
        .await;
    operations.push(op);
    for engine in [&store.ordinary, &store.protected] {
        let lease = engine
            .open(
                &id,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("diag"),
                    pin: JournalPinId::new("diag"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap();
        engine.close(lease).await.unwrap();
    }
    let (_, op) = reporter
        .measure(
            "diagnostics",
            0,
            "large_live_no_garbage_100_unrelated",
            true,
            async {
                for engine in [&store.ordinary, &store.protected] {
                    loop {
                        let report = engine
                            .collect(JournalGcRequest {
                                storage_id: engine.storage_id(),
                                expected_root_epoch: engine.root_epoch().unwrap(),
                                max_objects: 0,
                                work_budget: Default::default(),
                            })
                            .await
                            .unwrap();
                        assert_eq!(report.deleted, 0);
                        if !report.has_more {
                            break;
                        }
                    }
                }
            },
        )
        .await;
    operations.push(op);
    std::fs::write(
        reporter.directory.join("diagnostics.json"),
        serde_json::to_vec_pretty(&operations).unwrap(),
    )
    .unwrap();
    drop(store);
    drop(storage);
    drop(file);
}

async fn root_probe(store: &Store, id: &SessionId) {
    for journal in [&store.ordinary, &store.protected] {
        let lease = journal
            .open(
                id,
                JournalOpen {
                    root: JournalRootSelection::Latest,
                    expected_revision: None,
                    operation: JournalOperationId::new("bench-root"),
                    pin: JournalPinId::new("bench-root"),
                    intent: JournalOpenIntent::ReadOnly,
                },
            )
            .await
            .unwrap();
        journal.close(lease).await.unwrap();
    }
}

pub(crate) fn attribute(samples: &[Sample], wall: u64) -> BTreeMap<String, u64> {
    const CATEGORIES: [&str; 17] = [
        "collection",
        "engine",
        "S2",
        "writer_preparation",
        "writer_publication",
        "writer_submission_copy",
        "checkpoint_validation",
        "checkpoint_fingerprint",
        "writer_checkpoint_clone",
        "writer_open",
        "writer_other",
        "writer_submission_setup",
        "snapshot_capture",
        "request_build",
        "restore_context",
        "import_validation",
        "unattributed",
    ];
    let mut events = Vec::new();
    for sample in samples {
        let category = match sample.label {
            "rt.mod.collect" => Some(0),
            "rt.mod.commit" | "rt.mod.open" | "rt.mod.renew" | "rt.mod.close" => Some(1),
            "rt.storage.post_terminal" => Some(2),
            "rt.writer.preparation" => Some(3),
            "rt.writer.candidate_publication" => Some(4),
            "rt.writer.commit_submission" => Some(5),
            "rt.checkpoint.validate" => Some(6),
            "rt.checkpoint.fingerprint" | "rt.checkpoint.state_fingerprint" => Some(7),
            "rt.writer.boundary_checkpoint_clone" => Some(8),
            "rt.writer.open_domain" => Some(9),
            "rt.writer.commit_domain" => Some(10),
            "rt.writer.snapshot_submission" | "rt.writer.checkpoint_submission" => Some(11),
            "rt.runtime.capture_snapshot" | "rt.runtime.public_snapshot" => Some(12),
            "rt.runtime.build_request" => Some(13),
            "rt.runtime.restore_context" => Some(14),
            "rt.runtime.import_validation" => Some(15),
            _ => None,
        };
        if let Some(category) = category {
            let start = sample.started_ns.min(wall);
            let end = sample.started_ns.saturating_add(sample.nanos).min(wall);
            if end > start {
                events.push((start, true, category));
                events.push((end, false, category));
            }
        }
    }
    events.sort_unstable();
    let mut active = [0usize; 16];
    let mut elapsed = [0u64; 17];
    let mut position = 0;
    for (time, begin, category) in events {
        let chosen = active.iter().position(|n| *n > 0).unwrap_or(16);
        elapsed[chosen] += time - position;
        position = time;
        if begin {
            active[category] += 1;
        } else {
            active[category] -= 1;
        }
    }
    elapsed[16] += wall - position;
    CATEGORIES
        .into_iter()
        .zip(elapsed)
        .map(|(key, value)| (key.to_owned(), value))
        .collect()
}

/// Isolates preparation on the unchanged A/B seed corpus. DTO capture is
/// setup, so this is a lower bound on the Runtime's fixed-delta work.
pub(crate) async fn fixed_delta_work(directory: &std::path::Path) {
    use agent_runtime_core::journal::{
        JournalSnapshotView, JournalWriterAssembler, VolatileJournalStorage,
    };
    let mut rows = Vec::new();
    for name in ["A", "B"] {
        for n in [1000usize, 4000] {
            let snapshot = SessionSnapshot {
                id: SessionId::new(format!("fixed-delta-{name}-{n}")),
                history: seed(n, name != "A"),
                usage: Default::default(),
                identity: Default::default(),
                manifests: Vec::new(),
                extension_state: Default::default(),
                updated: Timestamp(100),
            };
            let fingerprint = format!(
                "{:x}",
                Sha256::digest(serde_json::to_vec(&snapshot.history).unwrap())
            );
            let journal = JournalEngine::protected(
                Arc::new(VolatileJournalStorage::default()),
                JournalStorageId::new("fixed-delta"),
                JournalEngineConfig {
                    validated_root_memo_bytes: 128 * 1024 * 1024,
                    ..Default::default()
                },
            );
            let lease = journal
                .open(
                    &snapshot.id,
                    JournalOpen {
                        root: JournalRootSelection::Latest,
                        expected_revision: None,
                        operation: JournalOperationId::new("open"),
                        pin: JournalPinId::new("writer"),
                        intent: JournalOpenIntent::ExclusiveWriter,
                    },
                )
                .await
                .unwrap();
            let checkpoint = TurnCheckpoint::session_boundary(snapshot.clone(), 1).unwrap();
            let mut assembler = JournalWriterAssembler::default();
            let (roots, mut cache) = assembler.snapshot_cached(&snapshot, 0, None, None).unwrap();
            let refs = assembler.checkpoint(&checkpoint, roots.clone()).unwrap();
            cache.finish_preparation(&assembler);
            let boundary = JournalBoundary {
                version: 1,
                session: snapshot.id.clone(),
                turn: Some(checkpoint.turn.clone()),
                checkpoint_sequence: 1,
                planned_step_count: 0,
            };
            let mut delta = JournalTransitionDelta {
                boundary: boundary.clone(),
                bootstrap: Some(Box::new(snapshot.clone())),
                history_append: vec![],
                usage_append: vec![],
                extension_updates: Default::default(),
                extension_references: Default::default(),
                manifests_append: vec![],
                manifest_references: vec![],
            };
            let mut commit = JournalCommit {
                batch: JournalBatch {
                    session: snapshot.id.clone(),
                    sequence: 1,
                    predecessor: None,
                    expected_revision: None,
                    writer_epoch: 1,
                    operation: JournalOperationId::new("base"),
                    boundary,
                    snapshot: roots,
                    checkpoint: Some(
                        JournalObject::encode(JournalObjectKind::Checkpoint, &refs)
                            .unwrap()
                            .id,
                    ),
                },
                digest: journal_history_chain(std::iter::empty()),
                objects: assembler.objects.values().cloned().collect(),
                delta: delta.clone(),
                checkpoint: Some(refs),
            };
            commit.digest = commit.input_digest().unwrap();
            let head = journal
                .commit_prepared(&lease, commit, assembler.preparation_hints())
                .await
                .unwrap();
            let known = assembler.objects.keys().cloned().collect::<BTreeSet<_>>();
            let view = JournalSnapshotView::from_snapshot(snapshot.clone()).unwrap();
            let appended = Message::user("fixed delta payload");
            let mut next = snapshot.clone();
            next.history.push(appended.clone());
            next.identity.event += 1;
            let mut checkpoint = checkpoint;
            checkpoint.snapshot = next.clone();
            checkpoint.state_revision += 1;
            checkpoint.watermark.checkpoint_sequence += 1;
            delta.bootstrap = None;
            delta.history_append.push(appended);
            delta.boundary.checkpoint_sequence = 2;
            let observer = profile::Observer::default();
            let result = observer
                .run(async {
                    let mut assembler = JournalWriterAssembler::default();
                    let (roots, _) = assembler
                        .snapshot_view_cached(&next, 0, Some(&view), Some(&cache))
                        .unwrap();
                    let refs = assembler.checkpoint(&checkpoint, roots.clone()).unwrap();
                    let mut commit = JournalCommit {
                        batch: JournalBatch {
                            session: next.id.clone(),
                            sequence: 2,
                            predecessor: Some(JournalDigest::parse(head.batch.as_str()).unwrap()),
                            expected_revision: Some(head.revision),
                            writer_epoch: 1,
                            operation: JournalOperationId::new("fixed-delta"),
                            boundary: delta.boundary.clone(),
                            snapshot: roots,
                            checkpoint: Some(
                                JournalObject::encode(JournalObjectKind::Checkpoint, &refs)
                                    .unwrap()
                                    .id,
                            ),
                        },
                        digest: journal_history_chain(std::iter::empty()),
                        objects: assembler
                            .objects
                            .values()
                            .filter(|o| !known.contains(&o.id))
                            .cloned()
                            .collect(),
                        delta,
                        checkpoint: Some(refs),
                    };
                    commit.digest = commit.input_digest().unwrap();
                    journal
                        .commit_prepared(&lease, commit, assembler.preparation_hints())
                        .await
                        .unwrap()
                })
                .await;
            let mut counts = BTreeMap::<String, u64>::new();
            for sample in observer.drain().unwrap() {
                if sample.label.starts_with("rt.work.") {
                    *counts.entry(sample.label.to_owned()).or_default() += sample.units;
                }
            }
            assert_eq!(
                result.snapshot,
                JournalWriterAssembler::default()
                    .snapshot(&next, 0)
                    .unwrap()
            );
            let read = journal
                .open(
                    &next.id,
                    JournalOpen {
                        root: JournalRootSelection::Latest,
                        expected_revision: None,
                        operation: JournalOperationId::new("read"),
                        pin: JournalPinId::new("read"),
                        intent: JournalOpenIntent::ReadOnly,
                    },
                )
                .await
                .unwrap();
            let actual = JournalReader::new(
                &journal,
                &read,
                JournalReadLimits {
                    objects: 4_000_000,
                    encoded_bytes: 4 * 1024 * 1024 * 1024,
                    entries: 16_000_000,
                },
            )
            .unwrap()
            .checkpoint()
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                canonical_journal_json(&actual).unwrap(),
                canonical_journal_json(&checkpoint).unwrap()
            );
            journal.close(read).await.unwrap();
            journal.close(lease).await.unwrap();
            rows.push(json!({"fixture":name,"n":n,"base":"protected seed only; DTO capture excluded","seed_bytes_sha256":fingerprint,"counts":counts}));
        }
    }
    std::fs::create_dir_all(directory).unwrap();
    std::fs::write(
        directory.join("fixed-delta-work.json"),
        serde_json::to_vec_pretty(&rows).unwrap(),
    )
    .unwrap();
}

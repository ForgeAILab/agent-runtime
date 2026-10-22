#[allow(dead_code)]
#[path = "support/disk.rs"]
mod disk;
#[allow(dead_code)]
#[path = "support/fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/layouts.rs"]
mod layouts;
use fixture::hot::{self, Parameters, Reporter};
use std::path::PathBuf;

fn root() -> PathBuf {
    std::env::var_os("JOURNAL_MEASUREMENT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("rt-journal-hot-path"))
}
fn integer(name: &str, default: usize) -> usize {
    std::env::var(name).map_or(default, |v| v.parse().expect("integer bench configuration"))
}
#[tokio::test]
async fn journal_hot_path_n100_smoke() {
    let mut reporter = Reporter::new(&root().join("smoke"));
    let mut cases = Vec::new();
    for name in ["A", "B"] {
        for tools in [false, true] {
            let mut paired = Vec::new();
            for enabled in [false, true] {
                let (case, _) = Box::pin(hot::run(
                    &mut reporter,
                    Parameters {
                        name,
                        n: 100,
                        tools,
                        sample: 0,
                        context: "smoke",
                        enabled,
                        conservative: true,
                        memo: 128 * 1024 * 1024,
                        collection: true,
                    },
                ))
                .await;
                if enabled {
                    let turn = case
                        .operations
                        .iter()
                        .find(|op| op.boundary == "turn")
                        .unwrap();
                    assert!(
                        turn.stages["rt.mod.commit"].calls > 0,
                        "spawned turn commits must be observed"
                    );
                }
                paired.push((
                    case.fixture_fingerprint.clone(),
                    case.canonical_snapshot.clone(),
                    case.ordinary_roots.clone(),
                    case.protected_roots.clone(),
                ));
                cases.push(case);
            }
            assert_eq!(paired[0], paired[1], "observer off/on canonical parity");
        }
    }
    reporter.save(&cases);
}
#[tokio::test]
#[ignore = "frozen hot-path baseline; requires release and journal-profile"]
async fn journal_hot_path_release_report() {
    if cfg!(debug_assertions) {
        panic!("release benchmark requires --release");
    }
    let context = std::env::var("JOURNAL_PROFILE_CONTEXT").unwrap_or_else(|_| "matrix".into());
    assert!(["matrix", "isolated"].contains(&context.as_str()));
    let names = std::env::var("JOURNAL_PROFILE_FIXTURES").unwrap_or_else(|_| {
        if context == "isolated" {
            "C,C-plain-supplement".into()
        } else {
            "A,B,C,C-plain-supplement".into()
        }
    });
    let sizes = std::env::var("JOURNAL_PROFILE_SIZES").unwrap_or_else(|_| "100,1000,4000".into());
    let mut reporter = Reporter::new(&root());
    let mut cases = Vec::new();
    let mut legacy_holders = Vec::new();
    std::fs::write(root().join("configuration.json"),serde_json::to_vec_pretty(&serde_json::json!({"profile_revision":"p1-incremental-v2","package":env!("CARGO_PKG_VERSION"),"debug_assertions":cfg!(debug_assertions),"opt_level":3,"lto":"thin","features":["journal-profile","test-support"],"adapter":"PackedSqliteJournalStorage","sealing":"modeled nonce/tag lengths; no real AEAD","sharing":"PerSession across domains","repack_on_collection":true,"memo_bytes_per_engine":128*1024*1024,"clock":100,"history_seed":"0xbadc0ffee1234567","script_seed":"0x12349876abcd5678","input_seed":"0x987612345678abcd","manifest_window":32,"collection_release_limit":20000,"cadence":"S2 and complete both-domain drains per turn plus shutdown","context":context,"historical_legacy_holders":context=="matrix","allocator_heap":null})).unwrap()).unwrap();
    for name in names.split(',') {
        for n in sizes.split(',').map(|n| n.parse::<usize>().unwrap()) {
            let selected_name = if name == "C-plain-supplement" {
                "JOURNAL_PROFILE_C_PLAIN_SIZES"
            } else {
                "JOURNAL_PROFILE_C_SIZES"
            };
            if name.starts_with('C')
                && std::env::var(selected_name).is_ok_and(|sizes| {
                    !sizes
                        .split(',')
                        .any(|value| value.parse::<usize>().unwrap() == n)
                })
            {
                continue;
            }

            if name.starts_with('C') && n == 100 {
                continue;
            }
            let variants: &[bool] = if name == "C" {
                &[true]
            } else if name == "C-plain-supplement" {
                &[false]
            } else {
                &[false, true]
            };
            let repeats = integer(
                if name.starts_with('C') {
                    "JOURNAL_PROFILE_C_SAMPLES"
                } else {
                    "JOURNAL_PROFILE_AB_SAMPLES"
                },
                if name.starts_with('C') { 3 } else { 5 },
            );
            for &tools in variants {
                for sample in 0..repeats {
                    eprintln!("measuring {context}/{name}/{n}/tools={tools}/sample={sample}");
                    let modes: &[bool] = if name.starts_with('C') {
                        &[true]
                    } else if sample % 2 == 0 {
                        &[false, true]
                    } else {
                        &[true, false]
                    };
                    let mut canonical = None;
                    for &enabled in modes {
                        let (case, holder) = Box::pin(hot::run(
                            &mut reporter,
                            Parameters {
                                name,
                                n,
                                tools,
                                sample,
                                context: &context,
                                enabled,
                                conservative: true,
                                memo: 128 * 1024 * 1024,
                                collection: true,
                            },
                        ))
                        .await;
                        let value = (
                            case.fixture_fingerprint.clone(),
                            case.canonical_snapshot.clone(),
                            case.ordinary_roots.clone(),
                            case.protected_roots.clone(),
                        );
                        if let Some(previous) = &canonical {
                            assert_eq!(previous, &value, "observer off/on canonical parity");
                        }
                        canonical = Some(value);
                        cases.push(case);
                        if context == "matrix" && enabled && sample == 0 {
                            legacy_holders.push(holder);
                        }
                        reporter.save(&cases);
                    }
                }
            }
        }
    }
    std::hint::black_box(legacy_holders);
    if std::env::var_os("JOURNAL_PROFILE_DIAGNOSTICS").is_some() {
        Box::pin(hot::diagnostics(&mut reporter)).await;
        for memo in [0, 1, 1024] {
            let (case, _) = Box::pin(hot::run(
                &mut reporter,
                Parameters {
                    name: "B",
                    n: 100,
                    tools: false,
                    sample: 0,
                    context: "memo-diagnostic",
                    enabled: true,
                    conservative: false,
                    memo,
                    collection: true,
                },
            ))
            .await;
            cases.push(case);
            reporter.save(&cases);
        }
        let (case, _) = Box::pin(hot::run(
            &mut reporter,
            Parameters {
                name: "B",
                n: 100,
                tools: false,
                sample: 0,
                context: "collection-off-diagnostic",
                enabled: true,
                conservative: false,
                memo: 128 * 1024 * 1024,
                collection: false,
            },
        ))
        .await;
        cases.push(case);
        reporter.save(&cases);
    }
}
#[tokio::test]
async fn journal_profile_async_scope_isolation() {
    use agent_runtime_core::journal::profile::{Observer, instrument, synchronous};
    let a = Observer::default();
    let b = Observer::default();
    let one = a.run(instrument("one", async {
        tokio::task::yield_now().await;
        synchronous("one-child", || ());
    }));
    let two = b.run(instrument("two", async {
        tokio::task::yield_now().await;
        synchronous("two-child", || ());
    }));
    tokio::join!(one, two);
    for (observer, parent, child) in [(a, "one", "one-child"), (b, "two", "two-child")] {
        let rows = observer.drain().unwrap();
        assert_eq!(rows.len(), 2);
        let p = rows.iter().find(|s| s.label == parent).unwrap();
        let c = rows.iter().find(|s| s.label == child).unwrap();
        assert_eq!(c.parent, Some(p.id));
        assert!(observer.drain().unwrap().is_empty());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_profile_spawned_context() {
    use agent_runtime_core::journal::profile::{Observer, instrument};
    let observer = Observer::default();
    observer
        .run(instrument("parent", async {
            tokio::spawn(agent_runtime_core::journal::profile::inherit(Box::pin(
                async {
                    tokio::task::yield_now().await;
                    instrument("spawned", async {}).await;
                },
            )))
            .await
            .unwrap();
        }))
        .await;
    let samples = observer.drain().unwrap();
    let parent = samples.iter().find(|s| s.label == "parent").unwrap();
    let child = samples.iter().find(|s| s.label == "spawned").unwrap();
    assert_eq!(child.parent, Some(parent.id));
}
#[tokio::test]
async fn journal_profile_calibration() {
    use agent_runtime_core::journal::profile::{Observer, synchronous};
    let mut rows = Vec::new();
    let count = 100_000;
    for enabled in [false, true] {
        let observer = Observer::default();
        let start = std::time::Instant::now();
        let run = async {
            for _ in 0..count {
                synchronous("calibration", || std::hint::black_box(()));
            }
        };
        if enabled {
            observer.run(run).await;
        } else {
            run.await;
        }
        let elapsed = start.elapsed().as_nanos();
        let samples = observer.drain().unwrap();
        assert_eq!(samples.len(), if enabled { count } else { 0 });
        let callback = std::time::Instant::now();
        for sample in &samples {
            std::hint::black_box((sample.label, sample.nanos, sample.units, sample.bytes));
        }
        let callback_ns = callback.elapsed().as_nanos();
        let sink = std::time::Instant::now();
        let mut bytes = Vec::new();
        for row in &samples {
            serde_json::to_writer(&mut bytes, row).unwrap();
        }
        rows.push(serde_json::json!({"enabled":enabled,"calls":count,"elapsed_ns":elapsed,"ns_per_call":elapsed as f64/count as f64,"callback_sink_ns":callback_ns,"buffered_sink_ns":sink.elapsed().as_nanos(),"sink_bytes":bytes.len(),"corrected_gate_times":false}));
    }
    std::fs::create_dir_all(root()).unwrap();
    std::fs::write(
        root().join("calibration.json"),
        serde_json::to_vec_pretty(&rows).unwrap(),
    )
    .unwrap();
}

#[test]
fn journal_profile_disjoint_attribution_handles_overlaps() {
    use agent_runtime_core::journal::profile::Sample;
    let make = |id, label, start, nanos| Sample {
        id,
        parent: None,
        label,
        nanos,
        started_ns: start,
        units: 1,
        bytes: 0,
    };
    let rows = [
        make(1, "rt.writer.commit_domain", 0, 100),
        make(2, "rt.writer.preparation", 0, 40),
        make(3, "rt.writer.snapshot_cached", 5, 30),
        make(4, "rt.mod.commit", 40, 40),
        make(5, "rt.writer.candidate_publication", 80, 20),
        make(6, "rt.checkpoint.validate", 100, 20),
        make(7, "rt.checkpoint.fingerprint", 110, 5),
    ];
    let result = hot::attribute(&rows, 130);
    assert_eq!(result["writer_preparation"], 40);
    assert_eq!(result["engine"], 40);
    assert_eq!(result["writer_publication"], 20);
    assert_eq!(result["checkpoint_validation"], 20);
    assert_eq!(result["checkpoint_fingerprint"], 0);
    assert_eq!(result["writer_other"], 0);
    assert_eq!(result["unattributed"], 10);
    assert_eq!(result.values().sum::<u64>(), 130);
}

#[tokio::test]
#[ignore = "release fixed-delta work diagnostic; reports unmet prefix gates separately"]
async fn journal_hot_path_fixed_delta_work_report() {
    if cfg!(debug_assertions) {
        panic!("release work report requires --release");
    }
    hot::fixed_delta_work(&root()).await;
}

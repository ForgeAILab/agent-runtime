#[path = "support/disk.rs"]
mod disk;
#[path = "support/fixture.rs"]
mod fixture;
#[path = "support/layouts.rs"]
mod layouts;

use std::path::PathBuf;
fn root() -> PathBuf {
    std::env::var_os("JOURNAL_MEASUREMENT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("agent-runtime-journal-measurement"))
}

#[tokio::test]
async fn n100_physical_disk_smoke() {
    for name in ["A", "B"] {
        for tools in [false, true] {
            let report = layouts::measure(&root(), fixture::build(name, 100, tools).await);
            assert!((1..=2).contains(&report.idle_legacy_blobs));
            assert_eq!(
                report.idle_legacy_blobs == 2,
                !report.legacy_canonical_difference_fields.is_empty()
            );
            assert!(report.legacy_l2.physical_bytes >= report.legacy_l1.physical_bytes);
            assert!(report.legacy_digest_skips > 0);
            for layout in &report.layouts {
                assert!(layout.before.physical_bytes >= layout.after.physical_bytes);
                assert!(layout.after.physical_bytes > 0);
                // Only S4 changes operation storage. S1-S3 must have the same
                // actual SQLite operation pages as D4 before and after GC.
                for (size, baseline) in [
                    (&layout.before, &report.layouts[0].before),
                    (&layout.after, &report.layouts[0].after),
                ] {
                    assert_eq!(
                        size.table_bytes["agent_session_journal_operation"],
                        baseline.table_bytes["agent_session_journal_operation"]
                    );
                }
            }
            assert!(report.operations.commits > 2);
            assert!(report.manifests_retained >= 2);
            eprintln!(
                "smoke {name} tools={tools}: blobs={} different={:?} L1={} S12345={} ratio={:?}",
                report.idle_legacy_blobs,
                report.legacy_canonical_difference_fields,
                report.legacy_l1.physical_bytes,
                report.s123_s4_s5_after.physical_bytes,
                report.gate_ratio
            );
        }
    }
}
#[tokio::test]
#[ignore = "full physical-disk report; run in release mode"]
async fn physical_disk_release_report() {
    let root = root();
    std::fs::create_dir_all(&root).unwrap();
    let mut reports = Vec::new();
    for name in ["A", "B", "C"] {
        let sizes: &[usize] = if name == "C" {
            &[1000]
        } else {
            &[100, 1000, 4000]
        };
        for &n in sizes {
            let variants: &[bool] = if name == "C" { &[true] } else { &[false, true] };
            for &tools in variants {
                eprintln!("measuring {name} N={n} tools={tools}");
                let report = layouts::measure(&root, fixture::build(name, n, tools).await);
                eprintln!(
                    "measured {name} N={n} tools={tools}: native/L2={:.5}, retained operations {} -> {}",
                    report.packed_after_l2_ratio,
                    report.retained_operations_before,
                    report.retained_operations_after
                );
                reports.push(report);
                std::fs::write(
                    root.join("measurements.json"),
                    serde_json::to_vec_pretty(&reports).unwrap(),
                )
                .unwrap();
                std::fs::write(root.join("measurements.md"), markdown(&reports)).unwrap();
            }
        }
    }
}

fn markdown(reports: &[layouts::Report]) -> String {
    use std::fmt::Write;
    let mut out = String::from(
        "Real packed SQLite physical bytes; schema baseline subtracted. S1–S5 implemented; packed after includes real collection pruning.\n\n| Fixture | N | Turn | L1 | L2 | Packed before | Packed after | Before/L2 | After/L2 |\n| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for r in reports {
        writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {:.5} | {:.5} |",
            r.fixture,
            r.n,
            if r.tools { "tool" } else { "plain" },
            r.legacy_l1.physical_bytes,
            r.legacy_l2.physical_bytes,
            r.packed_before.physical_bytes,
            r.packed_after.physical_bytes,
            r.packed_before_l2_ratio,
            r.packed_after_l2_ratio
        )
        .unwrap();
    }
    out
}

#[tokio::test]
async fn n100_actual_packed_physical_smoke() {
    for name in ["A", "B"] {
        let report = layouts::measure(&root(), fixture::build(name, 100, false).await);
        assert!(report.packed_after.physical_bytes > 0);
        assert!(report.packed_before.physical_bytes >= report.packed_after.physical_bytes);
        assert_eq!(
            report.packed_before.table_bytes["operation_pack"],
            report.packed_after.table_bytes["operation_pack"]
        );
        assert_eq!(report.s5_simulator_payload_delta, 0);
        assert_eq!(
            report.packed_after.physical_bytes,
            report.projected_after_s5_bytes
        );
        eprintln!(
            "actual {name}/100: {} / {} bytes, {:.5}x L2 after",
            report.packed_before.physical_bytes,
            report.packed_after.physical_bytes,
            report.packed_after_l2_ratio
        );
    }
}

#[tokio::test]
#[ignore = "supplemental growing N=4000 physical report; run in release mode"]
async fn physical_disk_grown_4000_report() {
    let root = root();
    std::fs::create_dir_all(&root).unwrap();
    eprintln!("measuring supplemental C N=4000 tools=true");
    let report = layouts::measure(&root, fixture::build("C", 4000, true).await);
    eprintln!(
        "measured C N=4000: native/L2={:.5}, retained operations {} -> {}",
        report.packed_after_l2_ratio,
        report.retained_operations_before,
        report.retained_operations_after
    );
    let row = serde_json::to_value(&report).unwrap();
    std::fs::write(
        root.join("measurements-c4000.json"),
        serde_json::to_vec_pretty(&row).unwrap(),
    )
    .unwrap();
    std::fs::write(
        root.join("measurements-c4000.md"),
        markdown(std::slice::from_ref(&report)),
    )
    .unwrap();
    let path = root.join("measurements.json");
    let mut rows: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    rows.retain(|old| !(old["fixture"] == "C" && old["n"] == 4000));
    rows.push(row);
    std::fs::write(path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
}

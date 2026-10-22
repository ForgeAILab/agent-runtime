#[allow(dead_code)]
#[path = "support/disk.rs"]
mod disk;
#[allow(dead_code)]
#[path = "support/fixture.rs"]
mod fixture;
#[allow(dead_code)]
#[path = "support/layouts.rs"]
mod layouts;

#[tokio::test]
async fn journal_profile_default_feature_parity() {
    use sha2::{Digest, Sha256};
    let mut rows = Vec::new();
    for name in ["A", "B"] {
        for tools in [false, true] {
            let fixture = Box::pin(fixture::build_with_collection(name, 100, tools, true)).await;
            let roots: Vec<_> = fixture
                .after
                .iter()
                .map(|(domain, export)| {
                    let head = export.heads.values().next().unwrap();
                    (
                        *domain,
                        format!(
                            "{:x}",
                            Sha256::digest(serde_json::to_vec(&head.snapshot).unwrap())
                        ),
                    )
                })
                .collect();
            rows.push(serde_json::json!({"fixture":name,"tools":tools,"roots":roots,"legacy_digest":fixture.legacy.digest,"legacy_writes":fixture.legacy.writes}));
        }
    }
    let directory = std::env::var_os("JOURNAL_MEASUREMENT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("default-feature-parity.json"),
        serde_json::to_vec_pretty(&rows).unwrap(),
    )
    .unwrap();
}

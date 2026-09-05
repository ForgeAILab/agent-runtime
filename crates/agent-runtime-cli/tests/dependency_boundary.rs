use std::fs;
use std::path::Path;

#[test]
fn cli_package_remains_a_workspace_leaf() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let crates_dir = manifest_dir.parent().expect("workspace crates directory");

    for entry in fs::read_dir(crates_dir).expect("read workspace crates") {
        let entry = entry.expect("read crate entry");
        if entry.path() == manifest_dir {
            continue;
        }
        let manifest = entry.path().join("Cargo.toml");
        if !manifest.is_file() {
            continue;
        }
        let contents = fs::read_to_string(&manifest).expect("read sibling manifest");
        assert!(
            !contents.contains("agent-runtime-cli"),
            "{} must not depend on the CLI leaf package",
            manifest.display()
        );
    }
}

#[test]
fn cli_mcp_dependency_is_stdio_only_and_declares_its_higher_msrv() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let contents = fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read CLI manifest");

    assert!(contents.contains("rust-version = \"1.88\""));
    assert!(contents.contains("agent-runtime-mcp.workspace = true"));
    assert!(!contents.contains("agent-runtime-mcp = {"));
    assert!(!contents.contains("features = [\"http\"]"));
}

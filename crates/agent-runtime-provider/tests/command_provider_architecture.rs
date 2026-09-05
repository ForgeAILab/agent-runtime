use std::fs;
use std::path::Path;

#[test]
fn process_hosting_is_optional_and_facade_gated() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let provider_manifest =
        fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read provider manifest");
    let runtime_manifest = fs::read_to_string(manifest_dir.join("../agent-runtime/Cargo.toml"))
        .expect("read runtime manifest");

    assert!(provider_manifest.contains("default = []"));
    assert!(provider_manifest.contains("command-provider = ["));
    assert!(provider_manifest.contains("command-group = {"));
    assert!(provider_manifest.contains("optional = true"));
    assert!(
        runtime_manifest
            .contains("command-provider = [\"agent-runtime-provider/command-provider\"]")
    );
}

#[test]
fn production_command_provider_has_no_shell_or_consumer_dependency() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read manifest");
    let source = fs::read_to_string(manifest_dir.join("src/command.rs")).expect("read source");
    let production = format!("{manifest}\n{source}").to_ascii_lowercase();

    for forbidden in [
        "command::new(\"sh\")",
        "command::new(\"/bin/sh\")",
        "command::new(\"cmd\")",
        "smith-runtime",
        "smith-config",
        "nyx-",
        "forge-daemon",
        "forge-client",
    ] {
        assert!(
            !production.contains(forbidden),
            "production command-provider boundary contains `{forbidden}`"
        );
    }
}

//! Neutral file/capsule fixtures. No Smith checkout was available: these are
//! contract models, not copies of Smith's three production implementations.

use super::*;
use serde::{Serialize, de::DeserializeOwned};
use std::path::{Path, PathBuf};

static NEXT: AtomicUsize = AtomicUsize::new(0);
#[derive(Debug)]
struct Files(PathBuf);
impl Files {
    fn new() -> Arc<Self> {
        let root = std::env::temp_dir().join(format!(
            "consumer-journal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Arc::new(Self(root))
    }
    fn write(&self, name: &str, value: &impl Serialize) {
        self.publish(name, &serde_json::to_vec(value).unwrap(), false);
    }
    fn publish(&self, name: &str, bytes: &[u8], before_head: bool) {
        use std::io::Write;
        let temporary = self.0.join(format!("{name}.staged"));
        let mut file = std::fs::File::create(&temporary).unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
        if before_head {
            return;
        }
        std::fs::rename(temporary, self.0.join(name)).unwrap();
        std::fs::File::open(&self.0).unwrap().sync_all().unwrap();
    }
    fn read<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>, RuntimeError> {
        let path = self.0.join(name);
        if !path.exists() {
            return Ok(None);
        }
        serde_json::from_slice(&std::fs::read(path).unwrap())
            .map(Some)
            .map_err(Into::into)
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// These impls deliberately define only the released required methods: journal
// discovery stays the default None. All compile inside the consumer target.
#[derive(Debug)]
struct FileSessions(Arc<Files>);
#[async_trait]
impl SessionStore for FileSessions {
    async fn load(&self, _: &SessionId) -> Result<Option<SessionSnapshot>, RuntimeError> {
        self.0.read("snapshot.json")
    }
    async fn save(&self, snapshot: &SessionSnapshot) -> Result<(), RuntimeError> {
        self.0.write("snapshot.json", snapshot);
        Ok(())
    }
}
#[derive(Debug)]
struct FileCheckpoints(Arc<Files>);
#[async_trait]
impl CheckpointStore for FileCheckpoints {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        self.0.read("checkpoint.json")
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        validate_save(self.0.read("checkpoint.json")?, checkpoint).await?;
        self.0.write("checkpoint.json", checkpoint);
        Ok(())
    }
}
#[derive(Debug)]
struct CapsuleCheckpoints(Arc<Files>);
#[async_trait]
impl CheckpointStore for CapsuleCheckpoints {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        // The capsule slot contains the existing typed fixture bytes. No
        // Smith-specific container schema is known or invented here.
        self.0.read("capsule-checkpoint.json")
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        let current = self.load_latest(&checkpoint.session).await?;
        validate_save(current, checkpoint).await?;
        self.0.write("capsule-checkpoint.json", checkpoint);
        Ok(())
    }
}
async fn validate_save(
    current: Option<TurnCheckpoint>,
    checkpoint: &TurnCheckpoint,
) -> Result<(), RuntimeError> {
    let store = crate::InMemoryCheckpointStore::new();
    if let Some(current) = current {
        store.seed(current)?;
    }
    store.save(checkpoint).await
}

fn legacy(unbounded: bool) -> (SessionSnapshot, TurnCheckpoint) {
    let (snapshot, checkpoint) = if unbounded {
        (
            include_str!("../fixtures/legacy-snapshot-unbounded.json"),
            include_str!("../fixtures/legacy-checkpoint-unbounded.json"),
        )
    } else {
        (
            include_str!("../fixtures/legacy-snapshot-absent.json"),
            include_str!("../fixtures/legacy-checkpoint-absent.json"),
        )
    };
    (
        serde_json::from_str(snapshot).unwrap(),
        serde_json::from_str(checkpoint).unwrap(),
    )
}

fn files_unchanged(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect()
}

/// Exercises the source-compatible file, capsule and in-memory fixture impls,
/// existing v3 diagnostic files and signed pending approval/request migration.
/// Native publication/cut guarantees come from the reference journal suite;
/// the staged-file check below models atomic rename, not a power-loss test.
pub async fn assert_file_journal_migration() {
    for unbounded in [false, true] {
        let (snapshot, checkpoint) = legacy(unbounded);
        for layout in 0..3 {
            let files = Files::new();
            let sessions = Arc::new(FileSessions(files.clone()));
            sessions.save(&snapshot).await.unwrap();
            let checkpoints: Arc<dyn CheckpointStore> = match layout {
                0 => {
                    files.write("checkpoint.json", &checkpoint);
                    Arc::new(FileCheckpoints(files.clone()))
                }
                1 => {
                    files.write("capsule-checkpoint.json", &checkpoint);
                    Arc::new(CapsuleCheckpoints(files.clone()))
                }
                _ => {
                    let memory = Arc::new(crate::InMemoryCheckpointStore::new());
                    memory.seed(checkpoint.clone()).unwrap();
                    memory
                }
            };
            assert!(sessions.journal().is_none());
            assert!(checkpoints.journal().is_none());
            assert!(sessions.require_journal().is_err());
            assert!(checkpoints.require_journal().is_err());
            checkpoints.save(&checkpoint).await.unwrap();
            checkpoints.save(&checkpoint).await.unwrap();
            assert_eq!(
                checkpoints.load_latest(&snapshot.id).await.unwrap(),
                Some(checkpoint.clone())
            );
            let original_files = files_unchanged(&files.0);
            let source = sessions.load(&snapshot.id).await.unwrap().unwrap();
            // Direct Vec read remains a supported public consumer surface.
            assert_eq!(source.manifests, snapshot.manifests);
            let pair = Pair::new(false);
            pair.snapshots
                .lock()
                .unwrap()
                .insert(source.id.as_str().into(), source);
            let loaded_checkpoint = checkpoints
                .load_latest(&snapshot.id)
                .await
                .unwrap()
                .unwrap();
            pair.checkpoints
                .lock()
                .unwrap()
                .insert(snapshot.id.as_str().into(), loaded_checkpoint);
            for _ in 0..2 {
                recovery::import(&pair, &checkpoint).await;
            }
            assert_eq!(
                files_unchanged(&files.0),
                original_files,
                "import must not rewrite legacy files/capsules"
            );
        }
    }
    // Pending approval/prepared arguments and signed reasoning are real runtime
    // boundaries. Each file/capsule source is reread before native import.
    for boundary in [
        recovery::Boundary::CallingModel,
        recovery::Boundary::Approval,
    ] {
        let checkpoint = recovery::legacy_boundary(boundary).await;
        for capsule in [false, true] {
            let files = Files::new();
            let checkpoints: Arc<dyn CheckpointStore> = if capsule {
                files.write("capsule-checkpoint.json", &checkpoint);
                Arc::new(CapsuleCheckpoints(files.clone()))
            } else {
                files.write("checkpoint.json", &checkpoint);
                Arc::new(FileCheckpoints(files.clone()))
            };
            let checkpoint = checkpoints
                .load_latest(&checkpoint.session)
                .await
                .unwrap()
                .unwrap();
            assert!(
                serde_json::to_string(&checkpoint)
                    .unwrap()
                    .contains("exact-signature")
            );
            let pair = Pair::new(false);
            recovery::seed(&pair, &checkpoint, false);
            recovery::import(&pair, &checkpoint).await;
        }
    }
    // A staged unpublished tail does not replace the published file. Corrupt
    // committed JSON is an error, never an absent or empty session.
    let files = Files::new();
    let (snapshot, _) = legacy(false);
    files.write("snapshot.json", &snapshot);
    let before = std::fs::read(files.0.join("snapshot.json")).unwrap();
    files.publish("snapshot.json", b"{torn", true);
    assert_eq!(
        std::fs::read(files.0.join("snapshot.json")).unwrap(),
        before
    );
    assert_eq!(
        FileSessions(files.clone())
            .load(&snapshot.id)
            .await
            .unwrap(),
        Some(snapshot.clone())
    );
    files.publish("snapshot.json", b"{corrupt", false);
    assert!(FileSessions(files).load(&snapshot.id).await.is_err());

    // Native repeated unchanged saves submit no Message/Usage/request body
    // again; newly revisioned batch/head metadata is expected.
    let pair = Pair::new(false);
    let provider = Arc::new(scenarios::fake_text("answer"));
    let runtime = pair.runtime(provider).build().unwrap();
    let session = runtime.start_session(StartSession::new()).await.unwrap();
    session.run(UserInput::text("hello")).await.unwrap();
    let id = session.id().clone();
    let body_ids = || {
        pair.ordinary
            .chunks
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(id, chunks)| {
                let object = JournalObject {
                    id: id.clone(),
                    bytes: chunks.concat(),
                };
                matches!(
                    object.kind().unwrap(),
                    JournalObjectKind::Message
                        | JournalObjectKind::UsageRecord
                        | JournalObjectKind::Request
                )
                .then_some(id.clone())
            })
            .collect::<Vec<_>>()
    };
    let before = body_ids();
    session.persist().await.unwrap();
    session.persist().await.unwrap();
    assert_eq!(body_ids(), before);
    assert_eq!(
        pair.ordinary.materialized(&id).await.0.unwrap().manifests,
        session.snapshot().manifests
    );
    session.shutdown().await.unwrap();
}

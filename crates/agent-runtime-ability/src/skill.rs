//! A neutral skill: a named, described set of instructions loaded on demand.
//!
//! This is the reusable core of a "skills system" — a name, a routing
//! description, the instruction body (inline or a file resolved at load time),
//! optional supporting files, and free-form metadata. It deliberately omits
//! product policy (frontmatter schemas, trust levels, requirement checks,
//! package discovery, routing lint); a host layers those on top.
//!
//! A skill is the paradigm case of the descriptor-first ability lifecycle: its
//! [`Ability::descriptor`] (the routing description, reduced to a bounded
//! card) can be indexed and searched with zero I/O, while its instruction
//! body — potentially a large file plus supporting assets — is read only when
//! [`ActivationHandle::activate`] is actually called.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use agent_runtime_registry::{EntryProvenance, RegistryRevision, RegistrySource};

use crate::Named;
use crate::ability::{Ability, AbilityKind};
use crate::activation::{Activated, ActivationError, ActivationHandle};
use crate::descriptor::AbilityDescriptor;

/// Where a skill's instruction body comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum SkillSource {
    /// The instruction body is held inline.
    Inline(String),
    /// The instruction body is read from this file at load time.
    File(PathBuf),
}

/// A supporting file a skill references (script, template, reference doc).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SkillFile {
    /// A stable logical name for the file.
    pub name: String,
    /// The file's path.
    pub path: PathBuf,
}

impl SkillFile {
    /// A supporting file with a logical `name` at `path`.
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
        }
    }
}

/// A named set of instructions the agent can load into context on demand.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Skill {
    /// The stable, unique skill name.
    pub name: String,
    /// The routing description used to decide when the skill applies.
    pub description: String,
    /// Where the instruction body comes from.
    pub source: SkillSource,
    /// Supporting files, if any.
    #[cfg_attr(feature = "serde", serde(default))]
    pub files: Vec<SkillFile>,
    /// Free-form metadata (host-defined keys).
    #[cfg_attr(feature = "serde", serde(default))]
    pub metadata: BTreeMap<String, String>,
    /// An explicit content revision, overriding the default derived one. Set
    /// this for a file-backed skill when a real content hash is available
    /// from packaging time — the descriptor itself must never read the file
    /// to compute one.
    #[cfg_attr(feature = "serde", serde(default))]
    revision: Option<RegistryRevision>,
}

impl Skill {
    /// A skill whose instruction body is held inline.
    pub fn inline(
        name: impl Into<String>,
        description: impl Into<String>,
        instructions: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            source: SkillSource::Inline(instructions.into()),
            files: Vec::new(),
            metadata: BTreeMap::new(),
            revision: None,
        }
    }

    /// A skill whose instruction body is read from `path` at load time.
    pub fn from_file(
        name: impl Into<String>,
        description: impl Into<String>,
        path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            source: SkillSource::File(path.into()),
            files: Vec::new(),
            metadata: BTreeMap::new(),
            revision: None,
        }
    }

    /// Adds a supporting file.
    pub fn with_file(mut self, file: SkillFile) -> Self {
        self.files.push(file);
        self
    }

    /// Sets a metadata key.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Overrides the descriptor's content revision — for example, a hash
    /// computed out of band when the skill was packaged. Without this, a
    /// file-backed skill's default revision is derived from its path, not its
    /// content, since deriving it from content would require reading the file
    /// eagerly (which the descriptor must never do).
    pub fn with_content_revision(mut self, revision: RegistryRevision) -> Self {
        self.revision = Some(revision);
        self
    }

    /// Resolves the skill's instruction body, reading the file if needed.
    pub fn load_instructions(&self) -> std::io::Result<String> {
        match &self.source {
            SkillSource::Inline(text) => Ok(text.clone()),
            SkillSource::File(path) => std::fs::read_to_string(path),
        }
    }

    /// The descriptor's content revision: the explicit override if one was
    /// set, otherwise derived from the inline instruction text or (for a
    /// file-backed skill) from the instruction file's path — never its
    /// content, since computing that would mean reading the file at
    /// descriptor-construction time.
    fn content_revision(&self) -> RegistryRevision {
        self.revision.clone().unwrap_or_else(|| match &self.source {
            SkillSource::Inline(text) => RegistryRevision::from_content(text),
            SkillSource::File(path) => {
                RegistryRevision::from_content(path.to_string_lossy().as_bytes())
            }
        })
    }

    /// The instruction file path, if this skill is file-backed.
    pub fn instructions_path(&self) -> Option<&Path> {
        match &self.source {
            SkillSource::File(path) => Some(path),
            SkillSource::Inline(_) => None,
        }
    }
}

impl Named for Skill {
    fn name(&self) -> &str {
        &self.name
    }
}

impl Ability for Skill {
    fn description(&self) -> &str {
        &self.description
    }

    fn kind(&self) -> AbilityKind {
        AbilityKind::Skill
    }

    fn descriptor(&self) -> AbilityDescriptor {
        let revision = self.content_revision();
        AbilityDescriptor::new(
            AbilityKind::Skill,
            self.name.clone(),
            EntryProvenance::new(RegistrySource::BuiltIn, revision.clone()),
            self.name.clone(),
            self.description.clone(),
            revision,
        )
    }
}

impl ActivationHandle for Skill {
    /// Reads the instruction body — the file, if this skill is file-backed.
    /// This is the *only* place a skill's file is ever read; building or
    /// searching its descriptor never touches it.
    fn activate(&self) -> Result<Activated, ActivationError> {
        self.load_instructions()
            .map(Activated::SkillInstructions)
            .map_err(|error| ActivationError::Unavailable {
                reason: error.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_skill_loads_its_body() {
        let skill = Skill::inline("brand-kit", "Make brand boards", "Step 1: ...")
            .with_metadata("version", "1");
        assert_eq!(skill.load_instructions().unwrap(), "Step 1: ...");
        assert_eq!(skill.kind(), AbilityKind::Skill);
        assert_eq!(skill.description(), "Make brand boards");
        assert!(skill.instructions_path().is_none());
        assert_eq!(skill.metadata.get("version").map(String::as_str), Some("1"));
    }

    #[test]
    fn file_skill_reads_body_at_load_time() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("SKILL.md");
        std::fs::write(&path, "do the thing").unwrap();
        let skill = Skill::from_file("deploy", "Deploy the app", &path)
            .with_file(SkillFile::new("script", dir.path().join("run.sh")));
        assert_eq!(skill.load_instructions().unwrap(), "do the thing");
        assert_eq!(skill.instructions_path(), Some(path.as_path()));
        assert_eq!(skill.files.len(), 1);
    }

    /// Spec scenario: "Search a skill without loading its body". The skill
    /// points at an instruction file that does not exist yet; building and
    /// searching its descriptor must still succeed, because both operate
    /// only on bounded card metadata. The file is read for the first time
    /// only once `activate` is called, after it exists.
    #[test]
    fn searching_a_skill_never_reads_its_instruction_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("SKILL.md");
        let skill = Skill::from_file("deploy", "Deploy the app to production", &path);
        assert!(!path.exists());

        // Indexing and searching use only the descriptor's bounded card.
        let descriptor = skill.descriptor();
        assert!(descriptor.card().matches_any(&["deploy".to_string()]));
        assert_eq!(descriptor.kind(), &AbilityKind::Skill);
        assert!(
            !path.exists(),
            "building/searching the descriptor must never touch the file"
        );

        // Only now does the file come into existence, and only `activate`
        // reads it.
        std::fs::write(&path, "do the thing").unwrap();
        let activated = skill.activate().unwrap();
        assert_eq!(
            activated,
            Activated::SkillInstructions("do the thing".to_string())
        );
    }

    #[test]
    fn a_file_backed_skills_default_revision_comes_from_its_path_not_its_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("SKILL.md");
        let skill = Skill::from_file("deploy", "Deploy the app", &path);
        // Computing this must not require the file to exist.
        let descriptor = skill.descriptor();
        assert!(!descriptor.content_revision().as_str().is_empty());
    }

    #[test]
    fn an_explicit_content_revision_overrides_the_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("SKILL.md");
        let skill = Skill::from_file("deploy", "Deploy the app", &path)
            .with_content_revision(RegistryRevision::new("packaged-v3"));
        assert_eq!(
            skill.descriptor().content_revision().as_str(),
            "packaged-v3"
        );
    }
}

//! Composable system-prompt assembly, folded from the retired standalone
//! `agent-runtime-prompt` package.
//!
//! [`SystemPromptBuilder`] holds named, addressable [`SectionBuilder`]s that
//! render lazily (a [`FileSection`] picks up live edits, an [`FnSection`]
//! reflects current state) and upserts/reorders/enables them by name. Folding
//! this into the context crate is not just a file move: [`SystemPromptBuilder::into_fragments`]
//! turns rendered sections into versioned [`FragmentKind::SystemInstruction`]
//! fragments, so a host that used to render a prompt string in isolation now
//! feeds the same sections through the one authoritative [`crate::planner::ContextPlanner`]
//! and [`crate::sizing::RequestSizer`] — there is no second, parallel
//! token-budget path. [`SystemPromptBuilder::build`]/[`SystemPromptBuilder::render_messages`]
//! remain for a host that only wants the rendered text or message, without
//! going through planning.
//!
//! This module ships **no** product prompt text: every section's content is
//! host-supplied (a string, a workspace file, or a closure). The *assembly
//! mechanism* is shared; the *wording* is policy that stays in the consuming
//! host.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agent_runtime_core::content::Message;
use agent_runtime_registry::RegistryRevision;
use serde::{Deserialize, Serialize};

use crate::fragment::{CacheClass, ContextFragment, FragmentContent, FragmentKind, FragmentSource};

/// A named, lazily-rendered prompt section.
pub trait SectionBuilder: Send + Sync + fmt::Debug {
    /// The section's stable, unique name (used as its addressable key and
    /// rendered header).
    fn name(&self) -> &str;

    /// Produces the section's content, or `None`/empty to omit it entirely.
    fn render(&self) -> Option<String>;
}

/// Fixed content known at construction time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticSection {
    name: String,
    content: String,
}

impl StaticSection {
    /// A section with fixed `content`.
    pub fn new(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
        }
    }
}

impl SectionBuilder for StaticSection {
    fn name(&self) -> &str {
        &self.name
    }

    fn render(&self) -> Option<String> {
        non_empty(self.content.trim())
    }
}

/// Reads a file at render time, so live edits are reflected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSection {
    name: String,
    path: PathBuf,
}

impl FileSection {
    /// A section backed by the file at `path`.
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
        }
    }

    /// The backing file path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SectionBuilder for FileSection {
    fn name(&self) -> &str {
        &self.name
    }

    fn render(&self) -> Option<String> {
        let content = fs::read_to_string(&self.path).ok()?;
        non_empty(content.trim())
    }
}

/// Like [`FileSection`] but enforces a maximum character budget.
///
/// When the file exceeds `max_chars`, the output is truncated at the last
/// complete line boundary before the limit and a neutral notice is appended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BudgetedFileSection {
    name: String,
    path: PathBuf,
    max_chars: usize,
}

impl BudgetedFileSection {
    /// A budgeted section backed by `path`, capped at `max_chars` characters.
    pub fn new(name: impl Into<String>, path: impl Into<PathBuf>, max_chars: usize) -> Self {
        Self {
            name: name.into(),
            path: path.into(),
            max_chars,
        }
    }
}

impl SectionBuilder for BudgetedFileSection {
    fn name(&self) -> &str {
        &self.name
    }

    fn render(&self) -> Option<String> {
        let content = fs::read_to_string(&self.path).ok()?;
        let trimmed = content.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(budgeted_content(&self.name, trimmed, self.max_chars))
    }
}

/// Caps `content` at `max_chars` characters, cutting at the last complete line
/// boundary before the cap and appending a notice telling the agent to slim the
/// source. Content at or under the cap is returned unchanged. UTF-8 safe.
pub fn budgeted_content(name: &str, content: &str, max_chars: usize) -> String {
    if content.chars().count() <= max_chars {
        return content.to_string();
    }
    // Convert the char cap into a UTF-8-safe byte offset.
    let byte_cap = content
        .char_indices()
        .nth(max_chars)
        .map(|(idx, _)| idx)
        .unwrap_or(content.len());
    let truncation_point = content[..byte_cap].rfind('\n').unwrap_or(byte_cap);
    let mut truncated = content[..truncation_point].to_string();
    truncated.push_str(&format!(
        "\n\n⚠ {name} exceeded {max_chars} chars — truncated at a line boundary. Trim this source so the full text fits again.",
    ));
    truncated
}

/// A closure-backed section for dynamic content.
pub struct FnSection {
    name: String,
    render_fn: Box<dyn Fn() -> Option<String> + Send + Sync>,
}

impl FnSection {
    /// A section whose content is produced by `render_fn` at render time.
    pub fn new(
        name: impl Into<String>,
        render_fn: impl Fn() -> Option<String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            render_fn: Box::new(render_fn),
        }
    }
}

impl fmt::Debug for FnSection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FnSection")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl SectionBuilder for FnSection {
    fn name(&self) -> &str {
        &self.name
    }

    fn render(&self) -> Option<String> {
        (self.render_fn)()
    }
}

fn non_empty(trimmed: &str) -> Option<String> {
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// A rendered, named prompt section: a header name and its trimmed content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSection {
    /// The section header / addressable name.
    pub name: String,
    /// The section body.
    pub content: String,
}

impl PromptSection {
    /// A section with the given name and content.
    pub fn new(name: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            content: content.into(),
        }
    }
}

/// Formats one section as a delimited block: `--- NAME ---\n<content>\n\n`.
///
/// This is the neutral default framing used by [`SystemPromptBuilder::build`].
/// Hosts wanting different framing render [`SystemPromptBuilder::render_sections`]
/// themselves.
pub fn format_section_block(section: &PromptSection) -> String {
    format!(
        "--- {} ---\n{}\n\n",
        section.name.trim(),
        section.content.trim()
    )
}

#[derive(Clone, Debug)]
struct SectionSlot {
    section: Arc<dyn SectionBuilder>,
    enabled: bool,
}

/// A composable system prompt assembler with named, addressable sections.
///
/// Sections are uniquely keyed by [`SectionBuilder::name`]. Adding a section
/// whose name already exists replaces it in place, preserving order. Sections
/// can be repositioned, enabled/disabled, or removed by name.
#[derive(Clone, Debug, Default)]
pub struct SystemPromptBuilder {
    slots: Vec<SectionSlot>,
}

impl SystemPromptBuilder {
    /// An empty builder.
    pub fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// Adds or replaces a section. An existing section with the same name is
    /// replaced in place (and re-enabled); otherwise it is appended.
    pub fn add(&mut self, section: Arc<dyn SectionBuilder>) -> &mut Self {
        let name = section.name();
        if let Some(slot) = self.slots.iter_mut().find(|s| s.section.name() == name) {
            slot.section = section;
            slot.enabled = true;
        } else {
            self.slots.push(SectionSlot {
                section,
                enabled: true,
            });
        }
        self
    }

    /// Adds many sections, each upserted via [`Self::add`].
    pub fn extend(
        &mut self,
        sections: impl IntoIterator<Item = Arc<dyn SectionBuilder>>,
    ) -> &mut Self {
        for section in sections {
            self.add(section);
        }
        self
    }

    /// Removes a section by name. Returns whether one was removed.
    pub fn remove(&mut self, name: &str) -> bool {
        let before = self.slots.len();
        self.slots.retain(|s| s.section.name() != name);
        self.slots.len() != before
    }

    /// Whether a section with `name` exists.
    pub fn has(&self, name: &str) -> bool {
        self.slots.iter().any(|s| s.section.name() == name)
    }

    /// Retrieves a section by name.
    pub fn get(&self, name: &str) -> Option<&Arc<dyn SectionBuilder>> {
        self.slots
            .iter()
            .find(|s| s.section.name() == name)
            .map(|s| &s.section)
    }

    /// Inserts a section immediately before `anchor` (moving it if it already
    /// exists). Appends when the anchor is absent.
    pub fn insert_before(&mut self, anchor: &str, section: Arc<dyn SectionBuilder>) -> &mut Self {
        self.insert_relative(anchor, section, 0)
    }

    /// Inserts a section immediately after `anchor` (moving it if it already
    /// exists). Appends when the anchor is absent.
    pub fn insert_after(&mut self, anchor: &str, section: Arc<dyn SectionBuilder>) -> &mut Self {
        self.insert_relative(anchor, section, 1)
    }

    fn insert_relative(
        &mut self,
        anchor: &str,
        section: Arc<dyn SectionBuilder>,
        offset: usize,
    ) -> &mut Self {
        let name = section.name().to_string();
        self.slots.retain(|s| s.section.name() != name);
        let slot = SectionSlot {
            section,
            enabled: true,
        };
        if let Some(idx) = self.slots.iter().position(|s| s.section.name() == anchor) {
            self.slots.insert(idx + offset, slot);
        } else {
            self.slots.push(slot);
        }
        self
    }

    /// Disables a section by name; it is retained but skipped when building.
    pub fn disable(&mut self, name: &str) -> &mut Self {
        if let Some(slot) = self.slots.iter_mut().find(|s| s.section.name() == name) {
            slot.enabled = false;
        }
        self
    }

    /// Re-enables a previously disabled section.
    pub fn enable(&mut self, name: &str) -> &mut Self {
        if let Some(slot) = self.slots.iter_mut().find(|s| s.section.name() == name) {
            slot.enabled = true;
        }
        self
    }

    /// Section names in current order (including disabled).
    pub fn section_names(&self) -> Vec<&str> {
        self.slots.iter().map(|s| s.section.name()).collect()
    }

    /// Number of sections (including disabled).
    pub fn len(&self) -> usize {
        self.slots.len()
    }

    /// Whether no sections have been added.
    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    // -- convenience constructors that avoid Arc::new boilerplate ------------

    /// Shorthand for adding a [`StaticSection`].
    pub fn section(&mut self, name: impl Into<String>, content: impl Into<String>) -> &mut Self {
        self.add(Arc::new(StaticSection::new(name, content)))
    }

    /// Shorthand for adding a [`FileSection`].
    pub fn file(
        &mut self,
        name: impl Into<String>,
        path: impl Into<std::path::PathBuf>,
    ) -> &mut Self {
        self.add(Arc::new(FileSection::new(name, path)))
    }

    /// Shorthand for adding a [`FnSection`].
    pub fn fn_section(
        &mut self,
        name: impl Into<String>,
        f: impl Fn() -> Option<String> + Send + Sync + 'static,
    ) -> &mut Self {
        self.add(Arc::new(FnSection::new(name, f)))
    }

    /// Renders enabled, non-empty sections as ordered [`PromptSection`]s.
    pub fn render_sections(&self) -> Vec<PromptSection> {
        let mut sections = Vec::new();
        for slot in &self.slots {
            if !slot.enabled {
                continue;
            }
            let Some(content) = slot.section.render() else {
                continue;
            };
            let trimmed = content.trim();
            if trimmed.is_empty() {
                continue;
            }
            sections.push(PromptSection::new(slot.section.name().trim(), trimmed));
        }
        sections
    }

    /// Renders all enabled, non-empty sections into a single prompt string, or
    /// `None` when nothing renders.
    pub fn build(&self) -> Option<String> {
        let mut output = String::new();
        for section in self.render_sections() {
            output.push_str(&format_section_block(&section));
        }
        if output.is_empty() {
            None
        } else {
            Some(output)
        }
    }

    /// Builds the prompt as a single system [`Message`], or `None` when empty.
    pub fn build_message(&self) -> Option<Message> {
        self.build().map(Message::system)
    }

    /// Builds the prompt as a `Vec<Message>` (zero or one system message),
    /// convenient for splicing into a request's message list.
    pub fn render_messages(&self) -> Vec<Message> {
        self.build_message().into_iter().collect()
    }

    /// Converts every enabled, non-empty section into a versioned
    /// [`FragmentKind::SystemInstruction`] [`ContextFragment`], in section
    /// order.
    ///
    /// Each fragment's id is the section's name, its content revision is
    /// derived from the rendered content (so a changed file or closure result
    /// changes the fragment and the plan fingerprint it feeds), its priority
    /// is the section's position in this builder (so canonical fragment order
    /// — [`ContextFragment::sort_key`] — matches configured section order),
    /// and its cache class is [`CacheClass::Stable`]: named sections model
    /// stable host instructions, the same default a hand-built
    /// system-instruction fragment gets. This is the seam that lets a host
    /// migrate named prompt sections onto the one authoritative
    /// [`crate::planner::ContextPlanner`] instead of rendering a prompt string
    /// in isolation.
    pub fn into_fragments(&self) -> Vec<ContextFragment> {
        self.render_sections()
            .into_iter()
            .enumerate()
            .map(|(index, section)| {
                let revision = RegistryRevision::from_content(&section.content);
                ContextFragment::new(
                    section.name,
                    FragmentKind::SystemInstruction,
                    FragmentSource::Host,
                    revision,
                    FragmentContent::Text(section.content),
                )
                .with_priority(index as i32)
                .with_cache_class(CacheClass::Stable)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn section_block_framing() {
        let block = format_section_block(&PromptSection::new("  A  ", "  body  "));
        assert_eq!(block, "--- A ---\nbody\n\n");
    }

    #[test]
    fn prompt_section_roundtrips_through_json() {
        let section = PromptSection::new("A", "body");
        let json = serde_json::to_string(&section).unwrap();
        let back: PromptSection = serde_json::from_str(&json).unwrap();
        assert_eq!(section, back);
    }

    #[test]
    fn static_section_trims_and_omits_blank() {
        assert_eq!(
            StaticSection::new("A", "  hi  ").render().as_deref(),
            Some("hi")
        );
        assert_eq!(StaticSection::new("A", "   ").render(), None);
    }

    #[test]
    fn budgeted_content_passes_through_under_limit() {
        assert_eq!(budgeted_content("X", "short", 100), "short");
    }

    #[test]
    fn budgeted_content_truncates_at_line_boundary() {
        let out = budgeted_content("BIG", "line one\nline two\nline three", 12);
        assert!(out.starts_with("line one"));
        assert!(out.contains("⚠ BIG exceeded 12 chars"));
        assert!(!out.contains("line three"));
    }

    #[test]
    fn budgeted_content_is_multibyte_safe() {
        let content = format!("{}\n{}", "记".repeat(30), "忆".repeat(30));
        let out = budgeted_content("CJK", &content, 40);
        assert!(out.starts_with(&"记".repeat(30)));
        assert!(!out.contains('忆'));
    }

    #[test]
    fn fn_section_reflects_closure() {
        let section = FnSection::new("T", || Some("now".to_string()));
        assert_eq!(section.render().as_deref(), Some("now"));
        assert!(format!("{section:?}").contains("FnSection"));
    }

    #[test]
    fn builds_in_insertion_order_with_section_blocks() {
        let mut builder = SystemPromptBuilder::new();
        builder.section("SOUL", "first");
        builder.section("IDENTITY", "second");
        assert_eq!(
            builder.build(),
            Some("--- SOUL ---\nfirst\n\n--- IDENTITY ---\nsecond\n\n".to_string())
        );
    }

    #[test]
    fn add_upserts_and_preserves_position() {
        let mut builder = SystemPromptBuilder::new();
        builder.section("A", "a");
        builder.section("B", "b");
        builder.section("C", "c");
        builder.add(Arc::new(StaticSection::new("B", "b2")));
        assert_eq!(builder.section_names(), vec!["A", "B", "C"]);
        assert!(builder.build().unwrap().contains("b2"));
        assert_eq!(builder.len(), 3);
    }

    #[test]
    fn insert_before_and_after_move_existing() {
        let mut builder = SystemPromptBuilder::new();
        builder.section("A", "a");
        builder.section("B", "b");
        builder.section("C", "c");
        builder.insert_before("A", Arc::new(StaticSection::new("C", "c2")));
        assert_eq!(builder.section_names(), vec!["C", "A", "B"]);
        builder.insert_after("B", Arc::new(StaticSection::new("C", "c3")));
        assert_eq!(builder.section_names(), vec!["A", "B", "C"]);
    }

    #[test]
    fn disable_skips_but_retains() {
        let mut builder = SystemPromptBuilder::new();
        builder.section("A", "a");
        builder.disable("A");
        assert_eq!(builder.build(), None);
        assert!(builder.has("A"));
        builder.enable("A");
        assert!(builder.build().unwrap().contains("--- A ---"));
    }

    #[test]
    fn render_messages_yields_one_system_message() {
        let mut builder = SystemPromptBuilder::new();
        builder.section("A", "hello");
        let messages = builder.render_messages();
        assert_eq!(messages.len(), 1);
        assert!(messages[0].joined_text().contains("hello"));

        let empty = SystemPromptBuilder::new();
        assert!(empty.render_messages().is_empty());
    }

    /// Requirement "Context supersedes standalone prompt assembly", scenario
    /// "Host uses named prompt sections".
    #[test]
    fn named_prompt_sections_become_versioned_context_fragments() {
        let mut prompt = SystemPromptBuilder::new();
        prompt.section("HARNESS", "You are a terminal coding assistant.");
        prompt.section("WORKSPACE", "/repo");
        prompt.section("HIDDEN", "never rendered");
        prompt.disable("HIDDEN");

        let fragments = prompt.into_fragments();

        assert_eq!(fragments.len(), 2, "the disabled section is omitted");
        assert!(
            fragments
                .iter()
                .all(|f| f.kind == FragmentKind::SystemInstruction)
        );
        assert!(
            fragments
                .iter()
                .all(|f| f.cache_class == CacheClass::Stable)
        );
        assert_eq!(fragments[0].id.as_str(), "HARNESS");
        assert_eq!(fragments[0].priority, 0);
        assert_eq!(fragments[1].id.as_str(), "WORKSPACE");
        assert_eq!(fragments[1].priority, 1);
        assert_eq!(
            fragments[0].revision,
            RegistryRevision::from_content("You are a terminal coding assistant.")
        );

        // A changed section's content changes its fragment's revision, which
        // is what makes a later plan fingerprint reflect the edit.
        let mut edited = SystemPromptBuilder::new();
        edited.section("HARNESS", "You are a careful terminal coding assistant.");
        let edited_fragments = edited.into_fragments();
        assert_ne!(fragments[0].revision, edited_fragments[0].revision);
    }

    /// Same requirement/scenario: the converted fragments' tokens, revisions,
    /// priority, and cache classification reach the authoritative plan
    /// produced by [`crate::planner::ContextPlanner`], not a separate
    /// estimator.
    #[test]
    fn prompt_section_fragments_flow_through_the_authoritative_context_planner() {
        use std::collections::BTreeMap;

        use agent_runtime_core::catalog::{Modality, ModelLimits, ResolvedModelProfile};
        use agent_runtime_core::provider::{Capabilities, ModelId};

        use crate::budget::ContextPolicy;
        use crate::planner::ContextPlanner;
        use crate::sizing::CharRatioSizer;

        let mut prompt = SystemPromptBuilder::new();
        prompt.section("HARNESS", "be a helpful assistant");
        prompt.section("WORKSPACE", "/repo");
        let mut fragments = prompt.into_fragments();
        fragments.push(ContextFragment::new(
            "input",
            FragmentKind::UserInput,
            FragmentSource::Host,
            RegistryRevision::new("u1"),
            FragmentContent::Text("hi".to_owned()),
        ));

        let profile = ResolvedModelProfile {
            provider: "test".to_owned(),
            model: ModelId::new("test-model"),
            aliases: Vec::new(),
            limits: ModelLimits::new(10_000, 10_000, 100),
            input_modalities: vec![Modality::Text],
            output_modalities: vec![Modality::Text],
            capabilities: Capabilities::basic_streaming(),
            tokenizer: None,
            request_adapter: None,
            cache_policy: None,
            provenance: BTreeMap::new(),
        };
        let sizer = CharRatioSizer::default();
        let policy = ContextPolicy::new(RegistryRevision::new("policy-1"), 100, 0);
        let planner = ContextPlanner::new(&profile, &sizer, policy);

        let plan = planner.plan(fragments).unwrap();

        assert_eq!(plan.segments().len(), 3, "two sections plus the input");
        assert_eq!(
            plan.messages().len(),
            2,
            "merged system message, then input"
        );
        let system_text = plan.messages()[0].joined_text();
        assert!(system_text.contains("be a helpful assistant"));
        assert!(system_text.contains("/repo"));
        let harness_segment = plan
            .segments()
            .iter()
            .find(|s| s.fragment.as_str() == "HARNESS")
            .expect("the HARNESS fragment survived planning");
        assert!(harness_segment.tokens > 0, "the sizer counted its tokens");
        assert_eq!(harness_segment.cache_class, CacheClass::Stable);
    }
}

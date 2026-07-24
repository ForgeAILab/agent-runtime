//! Demonstrates composing the facade's registry, ability, and context
//! mechanism directly: `cargo run -p agent-runtime --example facade_composition`.
//!
//! A host that wants to inspect what would reach a provider — without
//! starting a session — builds an ability catalog, assembles named
//! system-prompt sections, folds them into versioned context fragments
//! alongside the current turn's input, and compiles an immutable
//! `ContextPlan` through the one authoritative `ContextPlanner`. Every type
//! used here comes from the facade's re-exported `registry`, `ability`, and
//! `context` modules — an ordinary host needs only the `agent-runtime`
//! dependency to do this.

use std::collections::BTreeMap;
use std::sync::Arc;

use agent_runtime::ability::{AbilityRegistry, Skill};
use agent_runtime::context::{
    CharRatioSizer, ContextFragment, ContextPlanner, ContextPolicy, FragmentContent, FragmentKind,
    FragmentSource, SystemPromptBuilder,
};
use agent_runtime::registry::RegistryRevision;
use agent_runtime_core::catalog::{Modality, ModelLimits, ResolvedModelProfile};
use agent_runtime_core::provider::{Capabilities, ModelId};

fn main() {
    // 1. Register the abilities this host wants to advertise. Sealing freezes
    //    the catalog into an immutable, cheaply-shared set.
    let mut abilities = AbilityRegistry::new();
    abilities
        .register(Arc::new(Skill::inline(
            "web-search",
            "Searches the web for current information.",
            "Use this skill whenever the user asks about current events.",
        )))
        .expect("ability names are unique");
    let sealed = abilities.seal();
    println!("registered {} ability/-ies", sealed.len());
    for descriptor in sealed.descriptors() {
        println!("  - {} ({})", descriptor.id(), descriptor.card().summary);
    }

    // 2. Assemble named, host-authored system-prompt sections and fold them
    //    into versioned `FragmentKind::SystemInstruction` fragments — the
    //    same mechanism that used to live in the retired standalone
    //    `agent-runtime-prompt` crate, now part of the context engine.
    let mut prompt = SystemPromptBuilder::new();
    prompt.section("HARNESS", "You are a terminal coding assistant.");
    prompt.section("WORKSPACE", "/repo");
    let mut fragments = prompt.into_fragments();

    // 3. Add the current turn's input as its own fragment.
    fragments.push(ContextFragment::new(
        "input",
        FragmentKind::UserInput,
        FragmentSource::Host,
        RegistryRevision::new("turn-1"),
        FragmentContent::Text("What does the web-search skill do?".to_owned()),
    ));

    // 4. Compile the immutable plan a real provider request would be built
    //    from, against a frozen model profile and a deterministic sizer.
    let profile = ResolvedModelProfile {
        provider: "fake".to_owned(),
        model: ModelId::new("fake"),
        aliases: Vec::new(),
        limits: ModelLimits::new(10_000, 10_000, 500),
        input_modalities: vec![Modality::Text],
        output_modalities: vec![Modality::Text],
        capabilities: Capabilities::basic_streaming(),
        tokenizer: None,
        request_adapter: None,
        cache_policy: None,
        provenance: BTreeMap::new(),
    };
    let sizer = CharRatioSizer::default();
    let policy = ContextPolicy::new(RegistryRevision::new("policy-1"), 500, 0);
    let planner = ContextPlanner::new(&profile, &sizer, policy);
    let plan = planner.plan(fragments).expect("fragments fit the budget");

    println!("\nplan carries {} message(s)", plan.messages().len());
    println!("plan input tokens: {}", plan.input_tokens());
    println!("plan confidence: {:?}", plan.confidence());
    println!("plan fingerprint: {}", plan.fingerprint());
}

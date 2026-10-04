## Context

`ContentPart::Reasoning { text, redacted, signature }` is written to canonical
history when an attempt commits and is replayed on every later request.
Signatures and redacted payloads are only meaningful to the provider (and
often the model) that issued them. Adapters cannot tell their own reasoning
from another provider's.

## Goals / Non-Goals

- Goals: any provider/model can continue any session; same-model requests do
  not change by a byte.
- Non-Goals: translating reasoning between providers; changing what an
  adapter does with its own reasoning; rewriting stored history.

## Decisions

- Decision: add `producer: Option<ReasoningProducer>` (provider and model, as
  the run manifest names them) to `ContentPart::Reasoning`, serde-defaulted
  and skipped when `None`, so older snapshots and journals load unchanged.
- Decision: the driver stamps the producer when it commits assistant output,
  from the turn's provider and model. Adapters keep decoding reasoning with
  `producer: None`; they are not responsible for provenance.
- Decision: the step that turns canonical history into
  `ProviderRequest.messages` omits every reasoning part whose producer is
  `Some` and differs from the request's provider and model. Parts with
  `None` pass through unchanged. An assistant message left with no content
  after omission keeps its other parts; a message whose only content was
  foreign reasoning is omitted only if the provider wire format requires
  non-empty assistant content, and the adapter tests decide that per format.
- Decision: the projection runs before context planning and cache planning,
  so the plan, its fingerprints, and the request describe the same messages.
  When nothing is omitted the messages are the same values as today.
- Decision: provider and model compare exactly. A model change within one
  provider also omits the earlier model's reasoning; reasoning is private
  scratch and the prompt cache does not survive a model change anyway.

## Risks / Trade-offs

- Old sessions keep today's behaviour until they produce new reasoning;
  cross-provider failures in pre-existing history remain possible for parts
  stamped `None`.
- A provider that requires its own earlier reasoning on a tool-call
  continuation still receives it, because a continuation is the same
  provider and model.

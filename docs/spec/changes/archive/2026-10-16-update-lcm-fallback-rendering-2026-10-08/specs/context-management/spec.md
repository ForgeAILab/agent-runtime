## ADDED Requirements

### Requirement: Deterministic Fallback Rendering
The deterministic fallback SHALL render its source so that the stored text
cannot be read as a reply form. It MUST keep tool names, truncated arguments
and truncated results as evidence, MUST render each tool call and tool result
as a bracketed note that is not attributed to the assistant role, MUST NOT
render a tool call as `call name(args)`, and MUST omit reasoning parts. It MUST
begin with a fixed label saying the text is an excerpt and not a summary
whenever the label fits inside the fallback target, and MUST fall back to the
unlabelled cut when it does not. The rendering used to build a summary prompt
is not changed by this requirement.

#### Scenario: Deterministic tool evidence
- **WHEN** model escalation reaches fallback for a source holding a tool call and its result
- **THEN** the stored text MUST include the tool name, its truncated arguments and its truncated result
- **AND** it MUST NOT contain `call name(args)` or an assistant-attributed tool call line.

#### Scenario: Reasoning is left out
- **WHEN** the fallback source holds a reasoning part
- **THEN** the stored text MUST NOT contain that reasoning.

#### Scenario: Label on a tiny source
- **WHEN** the fallback target is smaller than the label
- **THEN** the fallback MUST still strictly shrink the source, without the label.

## ADDED Requirements

### Requirement: Child-completion delivery is budgeted and never refused for size

Automatic child-completion delivery SHALL inline completed child replies in
canonical batch order while each fits the remaining
`DelegationConfig::delivery_inline_bytes`; a reply of at most 1,024 bytes
SHALL always be inlined. A reply that does not fit SHALL be delivered as its
opening 1,024 bytes plus the reference of its parent-owned stored copy, or
with a pointer to the child's result when no copy exists. Runtime MUST NOT
refuse, defer, or leave ready an admission because of the size of its
replies.

#### Scenario: A long reply is delivered by reference

- **GIVEN** a factory with an artifact store and a child reply over the budget
- **WHEN** the ready batch is admitted
- **THEN** the delivery carries the reply's opening bytes and the stored
  reference
- **AND** the stored copy is owned by the parent session and holds the exact
  reply

#### Scenario: A batch over the child-completion ceiling

- **GIVEN** a ready batch whose inline form exceeds
  `MAX_CHILD_COMPLETION_TURN_CHARS`
- **WHEN** the batch is admitted
- **THEN** each outcome is delivered as one compact line
- **AND** the admission is accepted

#### Scenario: A short reply keeps the existing text

- **GIVEN** a reply within the budget and no transferred artifacts
- **WHEN** it is delivered
- **THEN** the delivery text is unchanged from previous releases

### Requirement: Delivered outcomes name their transferred artifacts

Each completed outcome in an automatic delivery SHALL list the artifacts
transferred to the parent from that child turn, giving the parent-owned id,
the child-owned source id, media type, and byte length. At most 32 SHALL be
listed per outcome, followed by the count of the rest.

#### Scenario: A transferred tool-output artifact

- **GIVEN** a child turn whose offloaded tool output was transferred to the
  parent
- **WHEN** its outcome is delivered automatically
- **THEN** the delivery names the parent-owned copy and its child-owned source

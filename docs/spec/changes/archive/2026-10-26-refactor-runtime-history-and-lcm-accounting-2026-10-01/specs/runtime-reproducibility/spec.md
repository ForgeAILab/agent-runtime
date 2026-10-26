## ADDED Requirements

### Requirement: History sharing preserves independent public values

Internal history sharing SHALL preserve the existing owned public history,
snapshot, request, and checkpoint value contracts and serialized forms.
Subsequent appends, projections, compaction, or recovery MUST NOT mutate a
previously returned snapshot or a frozen request, and equivalent execution
MUST retain exact provider continuation, prepared actions, and fingerprints.

#### Scenario: Caller retains an old snapshot while another turn runs

- **WHEN** a caller holds snapshot/history values while the session appends or compacts later history
- **THEN** the held values retain their original content, ordering, and serde form
- **AND** the session's new values reflect only its validated later execution

#### Scenario: Provider request and checkpoint cross an asynchronous boundary

- **WHEN** a frozen request or prepared-action checkpoint is retained while runtime state advances
- **THEN** its messages, signed continuation, exact action arguments, and operation fingerprint remain unchanged
- **AND** recovery still avoids repeating committed provider or tool work

#### Scenario: Existing consumer reads public history fields

- **WHEN** a consumer uses ProviderRequest.messages or SessionSnapshot.history as Vec<Message>, HistoryView.history as Arc<[Message]>, or the existing history/with_history methods
- **THEN** it compiles and retains its existing value semantics
- **AND** no consumer adopts a new history wrapper or serde migration

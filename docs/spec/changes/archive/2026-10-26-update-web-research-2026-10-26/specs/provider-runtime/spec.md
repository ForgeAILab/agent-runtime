## ADDED Requirements

### Requirement: Hosted citations link each source once per message

A hosted-search adapter SHALL link a cited source at most once per assistant
message: a source linked in an earlier text block of the same message SHALL
NOT be linked again in a later block, in addition to the existing rule that a
block already containing the URL gets no link.

#### Scenario: A page cited in two blocks

- **GIVEN** a response whose two text blocks both cite `https://releases.rs/`
- **WHEN** the adapter renders the citations
- **THEN** only the first block carries a link to `https://releases.rs/`

### Requirement: Gemini grounding redirects resolve through a host resolver

The Gemini Interactions adapter SHALL, when a host installed a citation
resolver on it, resolve each unique grounding redirect a hosted-search
response cites at the end of that response and before rendering, at most
64 per response with at most 8 in flight and 3 seconds in total, stopping on
cancellation or the request deadline. Rendered citations and hosted-search
sources SHALL carry the resolved page URL; a redirect the resolver does not
resolve in time SHALL be kept unchanged. The runtime's resolver SHALL issue one
GET per redirect, only to the grounding redirect origin, without following the
redirect, reading a body, or sending a credential, and SHALL accept only an
absolute HTTP(S) `Location` without userinfo. Without a resolver the adapter's
output SHALL be unchanged.

#### Scenario: Redirects resolve to one page

- **GIVEN** two grounding redirects that both resolve to
  `https://releases.rs/` and a third the resolver cannot resolve
- **WHEN** a response citing all three in two text blocks is rendered
- **THEN** `https://releases.rs/` is linked once, the third keeps its redirect
  link, and the hosted-search sources list each page once
- **AND** each redirect was looked up once

#### Scenario: A lookup reads only the redirect

- **GIVEN** a server answering `302` with `Location: https://releases.rs/`
- **WHEN** the transport looks up a redirect on that origin
- **THEN** it returns `https://releases.rs/` from one GET
- **AND** a relative `Location` or a `200` answer returns nothing
- **AND** a lookup outside the pinned origin is refused

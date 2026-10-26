## ADDED Requirements

### Requirement: Text artifact pages never split a character

`artifact.read` SHALL return a page of a text artifact (media type `text/*`,
JSON, or XML) as UTF-8 text when an edge of the page falls inside a
multi-byte character, leaving the partial character out, and SHALL report the
`offset` and `next_offset` of the bytes it returned so following `next_offset`
reads every character exactly once. A page of another media type, a page that
is not UTF-8 inside its bounds, or a page too short to hold one whole character
SHALL be returned as bytes unchanged.

#### Scenario: The last byte starts a character

- **GIVEN** a tool-outcome artifact of two-byte characters
- **WHEN** a 2,047-byte page is read from offset 0
- **THEN** it is returned as text of 1,023 characters with `next_offset` 2,046

#### Scenario: The offset falls inside a character

- **GIVEN** a `text/plain` artifact of two-byte characters
- **WHEN** a page is read from offset 1
- **THEN** it is returned as text with `offset` 2

#### Scenario: Binary media stays bytes

- **GIVEN** an `image/png` artifact
- **WHEN** a page is read
- **THEN** it is returned as bytes

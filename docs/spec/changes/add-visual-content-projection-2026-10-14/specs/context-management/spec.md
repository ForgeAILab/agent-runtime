## ADDED Requirements

### Requirement: Image parts are projected per request

The context planner SHALL project image parts before sizing and before
computing cache identity. Canonical history SHALL remain unchanged. A
projected image SHALL become a text part
`[image <id>: <mime>[ <w>×<h>] not included in this request]`, where `<id>`
is the first 8 lowercase hex digits of the SHA-256 digest of its data URL.
Dimensions SHALL be included when the PNG, JPEG, GIF, or WebP header
decodes. When the profile's `input_modalities` lacks `image`, every image
part SHALL be projected. Otherwise, when `ContextPolicy` carries an
`ImageWindow { inline: K }`:

- Every image in the newest user message SHALL stay inline and SHALL NOT be
  counted.
- The remaining images, numbered `0..T` in order, SHALL stay inline exactly
  when `i >= K * floor(max(T - K, 0) / K)`.

The plan SHALL report how many images it projected, separately for text-only
and window reasons.

#### Scenario: Text-only profile

- **GIVEN** a profile with `input_modalities = [text]` and history holding a
  tool-result image
- **WHEN** a plan is built
- **THEN** no planned message contains an image part
- **AND** the plan reports one image projected as text-only

#### Scenario: Stable prefix between advances

- **GIVEN** `K = 10` and 14 window-counted images in history
- **WHEN** a 15th image is appended and a new plan is built
- **THEN** all 15 stay inline
- **AND** the planned messages before the new one are identical to the
  previous plan's

#### Scenario: Window advance

- **GIVEN** `K = 10` and 20 window-counted images
- **WHEN** a plan is built
- **THEN** images 10 through 19 are inline and 0 through 9 are placeholders

#### Scenario: Newest user message is exempt

- **GIVEN** `K = 3` and a newest user message with 6 images
- **WHEN** a plan is built
- **THEN** all 6 are inline

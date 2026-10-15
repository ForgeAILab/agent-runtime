## ADDED Requirements

### Requirement: MCP image content becomes image parts

The MCP adapter SHALL translate an MCP `image` content block into a
`ContentPart::Image` whose URL is `data:<mime>;base64,<data>`. Its `detail`
SHALL be the host-supplied default. The adapter SHALL apply host-set bounds
on decoded bytes per image and images per result, and SHALL accept only PNG,
JPEG, GIF, and WebP. An image outside these SHALL become a text part naming
its media type, its size, and the reason. Image parts SHALL NOT count toward
the text output limit.

#### Scenario: Image within bounds

- **GIVEN** a tool result with one text block and one 900 KB PNG block
- **WHEN** the adapter translates it
- **THEN** the outcome content is the text part followed by an image part with
  a `data:image/png;base64,` URL

#### Scenario: Image over the byte bound

- **GIVEN** `max_image_bytes` of 5 MiB and a 12 MiB PNG block
- **WHEN** the adapter translates it
- **THEN** the outcome holds a text part naming `image/png`, the size, and the
  exceeded bound, and no image part

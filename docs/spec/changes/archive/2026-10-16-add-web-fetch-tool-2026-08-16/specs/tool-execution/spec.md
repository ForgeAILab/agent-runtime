## ADDED Requirements

### Requirement: Web fetch tool schema and content normalization

A built-in or host-provided web fetch tool SHALL accept an absolute HTTP or HTTPS URL, validate the URL scheme, and retrieve the remote resource. When requested in markdown format, the tool MUST convert HTML responses into clean Markdown while removing non-content elements (such as scripts, styles, and navigational elements). When requested in raw or text format, it MUST return the unparsed textual content.

#### Scenario: Model fetches an HTML web page in markdown format

- **GIVEN** a web fetch tool and an accessible HTTP URL serving HTML content
- **WHEN** the model invokes the tool with the URL and default or `"markdown"` format
- **THEN** HTML elements are translated into Markdown headings, links, code blocks, lists, and paragraphs
- **AND** script, style, and navigation tags are excluded from the output

#### Scenario: Model fetches a raw code or data file

- **GIVEN** a web fetch tool and an accessible URL serving plain text or code
- **WHEN** the model invokes the tool with format `"raw"` or `"text"`
- **THEN** the raw document content is returned verbatim without HTML transformation

### Requirement: Untrusted web content injection safety prefix

All fetched external web content returned by the fetch tool SHALL include an explicit safety notice prefix identifying the returned body as untrusted external data that may contain prompt injection or adversarial instructions.

#### Scenario: Tool returns fetched web content with safety boundary prefix

- **GIVEN** a successful fetch of remote content
- **WHEN** the tool outcome is generated
- **THEN** the output begins with an untrusted content safety notice
- **AND** instructs the model that external web content is data, not authority or instruction

### Requirement: Web fetch permission scoping and SSRF protection

The web fetch tool SHALL declare `Effect::Network` and prepare its concrete security resource as `SecurityResource::network(url)` requiring `Permission::NetHttp`. Invocations attempting to fetch non-HTTP/HTTPS schemes (such as `file:`, `ftp:`, `gopher:`) or unauthorized internal IP/loopback addresses under restrictive policy MUST fail validation before issuing network I/O.

#### Scenario: Tool invocation specifies an unsupported scheme

- **GIVEN** a fetch tool call with URL `file:///etc/passwd`
- **WHEN** the tool validates and prepares the invocation
- **THEN** the call fails closed with an invalid URL scheme error
- **AND** no filesystem or network operation is performed

#### Scenario: Tool invocation prepares network authorization

- **GIVEN** a fetch tool call with URL `https://docs.example.com/api`
- **WHEN** the invocation is prepared
- **THEN** it claims `Permission::NetHttp` scoped to the exact canonical URL resource
- **AND** authorization evaluates against the declared network permission boundary

### Requirement: Web fetch respects deadlines, cancellation, and output bounds

The web fetch execution SHALL derive its network timeout from the invocation context's deadline, observe turn cancellation, and enforce maximum response size limits. If the response exceeds configured output limits, the result MUST be safely truncated with an explicit marker rather than blowing model context.

#### Scenario: Turn is cancelled during active network request

- **GIVEN** an in-flight web fetch request
- **WHEN** turn cancellation is triggered
- **THEN** the network request is aborted
- **AND** the tool returns promptly without blocking turn completion

#### Scenario: Response exceeds maximum allowed size

- **GIVEN** a fetched resource whose size exceeds the configured maximum byte or character bound
- **WHEN** the content is normalized
- **THEN** the tool returns output bounded to the limit
- **AND** appends a visible truncation notice

//! Request-local image placeholders and the stepped historical image window.
//!
//! Projection touches only the planner's owned fragments. Digests and header
//! dimensions are memoized per URL, and image payloads are decoded only far
//! enough to read their headers; nothing here fetches a remote URL.

use std::collections::HashMap;

use agent_runtime_core::content::{ContentPart, Role};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::fragment::{ContextFragment, FragmentContent, FragmentKind};

/// The stepped window for images outside the newest end-user message.
///
/// Zero is treated as one, including values supplied by struct literals or
/// deserialization. The window retains between `inline` and `2 * inline - 1`
/// counted images once history contains at least `inline` images.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageWindow {
    /// The minimum number of counted images retained at each window advance.
    #[serde(default = "minimum_inline")]
    pub inline: u32,
}

fn minimum_inline() -> u32 {
    1
}

/// Counts of images replaced before budgeting and compaction in one request.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageProjectionReport {
    /// Images replaced because the model does not accept image input.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub text_only: u32,
    /// Images replaced because they precede the stepped historical window.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub window: u32,
}

fn is_zero(value: &u32) -> bool {
    *value == 0
}

impl ImageProjectionReport {
    pub(crate) fn is_empty(&self) -> bool {
        self.text_only == 0 && self.window == 0
    }
}

pub(crate) fn project_fragments(
    fragments: &mut [ContextFragment],
    accepts_images: bool,
    window: Option<ImageWindow>,
) -> ImageProjectionReport {
    let mut report = ImageProjectionReport::default();
    if accepts_images && window.is_none() {
        return report;
    }
    let newest_user = fragments
        .iter()
        .rposition(|fragment| match &fragment.content {
            FragmentContent::Message(message) => {
                message.role == Role::User
                    && !message
                        .content
                        .iter()
                        .any(|part| matches!(part, ContentPart::ToolResult(_)))
            }
            FragmentContent::Text(_) => fragment.kind == FragmentKind::UserInput,
            FragmentContent::Tool(_) => false,
        });
    let total = fragments
        .iter()
        .enumerate()
        .filter(|(index, _)| Some(*index) != newest_user)
        .map(|(_, fragment)| match &fragment.content {
            FragmentContent::Message(message) => count_images(&message.content),
            _ => 0,
        })
        .fold(0usize, usize::saturating_add);
    let k = window.map_or(1, |window| window.inline.max(1) as usize);
    let cutoff = k * (total.saturating_sub(k) / k);
    let mut index = 0usize;
    let mut memo = HashMap::new();
    for (message_index, fragment) in fragments.iter_mut().enumerate() {
        let FragmentContent::Message(message) = &mut fragment.content else {
            continue;
        };
        if accepts_images && Some(message_index) == newest_user {
            continue;
        }
        if project_parts(
            &mut message.content,
            accepts_images,
            cutoff,
            &mut index,
            &mut memo,
            &mut report,
        ) {
            // A hint about the original image-bearing fragment must not
            // override sizing or compaction of its projected replacement.
            fragment.token_hint = None;
        }
    }
    report
}

fn count_images(parts: &[ContentPart]) -> usize {
    parts.iter().fold(0usize, |count, part| {
        count.saturating_add(match part {
            ContentPart::Image { .. } => 1,
            ContentPart::ToolResult(result) => count_images(&result.content),
            _ => 0,
        })
    })
}

fn project_parts(
    parts: &mut [ContentPart],
    accepts_images: bool,
    cutoff: usize,
    index: &mut usize,
    memo: &mut HashMap<String, String>,
    report: &mut ImageProjectionReport,
) -> bool {
    let mut changed = false;
    for part in parts {
        match part {
            ContentPart::Image { url, .. } => {
                let replace = !accepts_images || *index < cutoff;
                *index = index.saturating_add(1);
                if replace {
                    let text = memo
                        .entry(url.clone())
                        .or_insert_with_key(|url| placeholder(url))
                        .clone();
                    *part = ContentPart::text(text);
                    if accepts_images {
                        report.window = report.window.saturating_add(1);
                    } else {
                        report.text_only = report.text_only.saturating_add(1);
                    }
                    changed = true;
                }
            }
            ContentPart::ToolResult(result) => {
                changed |= project_parts(
                    &mut result.content,
                    accepts_images,
                    cutoff,
                    index,
                    memo,
                    report,
                );
            }
            _ => {}
        }
    }
    changed
}

fn placeholder(url: &str) -> String {
    let digest = Sha256::digest(url.as_bytes());
    let id = format!(
        "{:02x}{:02x}{:02x}{:02x}",
        digest[0], digest[1], digest[2], digest[3]
    );
    let data = url
        .strip_prefix("data:")
        .and_then(|data| data.split_once(','));
    let mime = data.map_or("image", |(header, _)| {
        header
            .split(';')
            .next()
            .filter(|mime| !mime.is_empty())
            .unwrap_or("image")
    });
    let dimensions = data.and_then(|(header, payload)| {
        header
            .split(';')
            .any(|parameter| parameter == "base64")
            .then(|| image_dimensions(mime, payload))
            .flatten()
    });
    match dimensions {
        Some((width, height)) => {
            format!("[image {id}: {mime} {width}×{height} not included in this request]")
        }
        None => format!("[image {id}: {mime} not included in this request]"),
    }
}

/// Incrementally decodes complete base64 quanta, with at most two extra bytes.
struct HeaderPrefix<'a> {
    encoded: &'a [u8],
    offset: usize,
    bytes: Vec<u8>,
}

impl<'a> HeaderPrefix<'a> {
    fn new(encoded: &'a str) -> Self {
        Self {
            encoded: encoded.as_bytes(),
            offset: 0,
            bytes: Vec::new(),
        }
    }

    fn read(&mut self, needed: usize) -> Option<&[u8]> {
        if self.bytes.len() < needed {
            let quanta = (needed - self.bytes.len()).div_ceil(3);
            let end = self
                .offset
                .saturating_add(quanta.saturating_mul(4))
                .min(self.encoded.len());
            let encoded = &self.encoded[self.offset..end];
            let decoded = STANDARD
                .decode(encoded)
                .or_else(|_| STANDARD_NO_PAD.decode(encoded))
                .ok()?;
            self.bytes.extend_from_slice(&decoded);
            self.offset = end;
        }
        self.bytes.get(..needed)
    }
}

fn image_dimensions(mime: &str, payload: &str) -> Option<(u32, u32)> {
    let mut prefix = HeaderPrefix::new(payload);
    let dimensions = match mime {
        "image/png" => {
            let bytes = prefix.read(24)?;
            if &bytes[..8] != b"\x89PNG\r\n\x1a\n"
                || &bytes[12..16] != b"IHDR"
                || be_u32(&bytes[8..12]) != 13
            {
                return None;
            }
            (be_u32(&bytes[16..20]), be_u32(&bytes[20..24]))
        }
        "image/jpeg" => jpeg_dimensions(&mut prefix)?,
        "image/gif" => {
            let bytes = prefix.read(10)?;
            if &bytes[..6] != b"GIF87a" && &bytes[..6] != b"GIF89a" {
                return None;
            }
            (le_u16(&bytes[6..8]), le_u16(&bytes[8..10]))
        }
        "image/webp" => webp_dimensions(&mut prefix)?,
        _ => return None,
    };
    (dimensions.0 > 0 && dimensions.1 > 0).then_some(dimensions)
}

fn be_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}

fn le_u16(bytes: &[u8]) -> u32 {
    u32::from(u16::from_le_bytes([bytes[0], bytes[1]]))
}

fn le_u24(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0])
}

fn jpeg_dimensions(prefix: &mut HeaderPrefix<'_>) -> Option<(u32, u32)> {
    if prefix.read(2)? != b"\xff\xd8" {
        return None;
    }
    let mut offset = 2usize;
    loop {
        if prefix.read(offset.checked_add(1)?)?[offset] != 0xff {
            return None;
        }
        offset = offset.checked_add(1)?;
        while prefix.read(offset.checked_add(1)?)?[offset] == 0xff {
            offset = offset.checked_add(1)?;
        }
        let marker = prefix.read(offset.checked_add(1)?)?[offset];
        offset = offset.checked_add(1)?;
        match marker {
            0x00 | 0xd9 | 0xda => return None,
            0x01 | 0xd0..=0xd8 => continue,
            _ => {}
        }
        let bytes = prefix.read(offset.checked_add(2)?)?;
        let length = usize::from(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]));
        if length < 2 {
            return None;
        }
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            if length < 8 {
                return None;
            }
            let bytes = prefix.read(offset.checked_add(7)?)?;
            let height = u16::from_be_bytes([bytes[offset + 3], bytes[offset + 4]]);
            let width = u16::from_be_bytes([bytes[offset + 5], bytes[offset + 6]]);
            return Some((u32::from(width), u32::from(height)));
        }
        offset = offset.checked_add(length)?;
    }
}

fn webp_dimensions(prefix: &mut HeaderPrefix<'_>) -> Option<(u32, u32)> {
    let bytes = prefix.read(12)?;
    if &bytes[..4] != b"RIFF" || &bytes[8..12] != b"WEBP" {
        return None;
    }
    let mut offset = 12usize;
    loop {
        let bytes = prefix.read(offset.checked_add(8)?)?;
        let kind: [u8; 4] = bytes[offset..offset + 4].try_into().ok()?;
        let size = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().ok()?) as usize;
        let start = offset.checked_add(8)?;
        match &kind {
            b"VP8X" if size >= 10 => {
                let bytes = prefix.read(start.checked_add(10)?)?;
                return Some((
                    1 + le_u24(&bytes[start + 4..start + 7]),
                    1 + le_u24(&bytes[start + 7..start + 10]),
                ));
            }
            b"VP8L" if size >= 5 => {
                let bytes = &prefix.read(start.checked_add(5)?)?[start..];
                if bytes[0] != 0x2f {
                    return None;
                }
                let bits = u32::from_le_bytes(bytes[1..5].try_into().ok()?);
                return Some((1 + (bits & 0x3fff), 1 + ((bits >> 14) & 0x3fff)));
            }
            b"VP8 " if size >= 10 => {
                let bytes = &prefix.read(start.checked_add(10)?)?[start..];
                if bytes[0] & 1 != 0 || &bytes[3..6] != b"\x9d\x01\x2a" {
                    return None;
                }
                return Some((
                    le_u16(&bytes[6..8]) & 0x3fff,
                    le_u16(&bytes[8..10]) & 0x3fff,
                ));
            }
            _ => offset = start.checked_add(size)?.checked_add(size % 2)?,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fragment::{FragmentKind, FragmentSource};
    use agent_runtime_core::content::{Message, ToolResultBlock};
    use agent_runtime_core::ids::ToolCallId;
    use agent_runtime_registry::RegistryRevision;

    fn image(url: &str) -> ContentPart {
        ContentPart::Image {
            url: url.into(),
            detail: Some("high".into()),
        }
    }

    fn fragment(index: usize, message: Message) -> ContextFragment {
        ContextFragment::new(
            format!("image:{index}"),
            FragmentKind::History,
            FragmentSource::History,
            RegistryRevision::new("1"),
            FragmentContent::Message(message),
        )
    }

    #[test]
    fn deterministic_projection_preserves_canonical_history_and_ids_after_prefix_removal() {
        let canonical = vec![
            fragment(0, Message::assistant(vec![image("earlier")])),
            fragment(1, Message::user("question")),
            fragment(
                2,
                Message::assistant(vec![image("https://example.com/image.png")]),
            ),
        ];
        let original = canonical.clone();
        let mut first = canonical.clone();
        let mut second = canonical.clone();
        assert_eq!(
            project_fragments(&mut first, false, None),
            project_fragments(&mut second, false, None)
        );
        assert_eq!(first, second);
        assert_eq!(canonical, original);
        let mut shortened = canonical[1..].to_vec();
        project_fragments(&mut shortened, false, None);
        assert_eq!(first[2].content, shortened[1].content);
    }

    #[test]
    fn text_only_replaces_all_roles_and_recursively_nested_results() {
        let nested = ContentPart::ToolResult(ToolResultBlock {
            call_id: ToolCallId::new("call"),
            name: "screenshot".into(),
            content: vec![
                image("same"),
                ContentPart::ToolResult(ToolResultBlock {
                    call_id: ToolCallId::new("nested"),
                    name: "nested".into(),
                    content: vec![image("same")],
                    is_error: false,
                }),
            ],
            is_error: false,
        });
        let mut fragments = vec![
            fragment(0, Message::assistant(vec![image("same")])),
            fragment(
                1,
                Message {
                    role: Role::User,
                    content: vec![image("same")],
                },
            ),
            fragment(
                2,
                Message {
                    role: Role::Tool,
                    content: vec![nested],
                },
            ),
        ];
        let report = project_fragments(&mut fragments, false, Some(ImageWindow { inline: 3 }));
        assert_eq!(
            report,
            ImageProjectionReport {
                text_only: 4,
                window: 0
            }
        );
        for fragment in fragments {
            let FragmentContent::Message(message) = fragment.content else {
                panic!("expected message")
            };
            assert_eq!(count_images(&message.content), 0);
        }
    }

    #[test]
    fn memoizes_one_placeholder_per_distinct_url() {
        let mut parts = vec![image("same"), image("same"), image("different")];
        let mut memo = HashMap::new();
        let mut report = ImageProjectionReport::default();
        project_parts(&mut parts, false, 0, &mut 0, &mut memo, &mut report);
        assert_eq!(memo.len(), 2);
        assert_eq!(parts[0], parts[1]);
        assert_eq!(report.text_only, 3);
    }

    #[test]
    fn user_tool_result_carrier_is_counted_and_does_not_take_the_exemption() {
        let mut fragments = vec![
            fragment(
                0,
                Message {
                    role: Role::User,
                    content: vec![image("user"), image("user")],
                },
            ),
            fragment(1, Message::assistant(vec![image("old")])),
            fragment(
                2,
                Message {
                    role: Role::User,
                    content: vec![ContentPart::ToolResult(ToolResultBlock {
                        call_id: ToolCallId::new("call"),
                        name: "screenshot".into(),
                        content: vec![image("tool")],
                        is_error: false,
                    })],
                },
            ),
        ];
        let report = project_fragments(&mut fragments, true, Some(ImageWindow { inline: 1 }));
        assert_eq!(report.window, 1);
        let FragmentContent::Message(message) = &fragments[0].content else {
            panic!("expected message")
        };
        assert_eq!(count_images(&message.content), 2);
        let FragmentContent::Message(message) = &fragments[1].content else {
            panic!("expected message")
        };
        assert_eq!(count_images(&message.content), 0);
    }

    #[test]
    fn tiny_png_and_jpeg_headers_produce_dimensions_without_decoding_payload() {
        let mut png = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        png.extend_from_slice(&3u32.to_be_bytes());
        png.extend_from_slice(&2u32.to_be_bytes());
        let encoded = STANDARD.encode(&png);
        assert_eq!(
            image_dimensions("image/png", &format!("{encoded}invalid unread tail")),
            Some((3, 2))
        );
        let url = format!("data:image/png;base64,{encoded}");
        assert!(placeholder(&url).ends_with(": image/png 3×2 not included in this request]"));

        // SOI, a short APP0 segment, SOF2 (progressive), precision, height,
        // width, one component. Only the prefix through width is needed.
        let jpeg = [
            0xff, 0xd8, 0xff, 0xe0, 0, 4, 0, 0, 0xff, 0xc2, 0, 11, 8, 0, 2, 0, 3, 1, 1, 0x11, 0,
        ];
        let encoded = STANDARD.encode(jpeg);
        assert_eq!(image_dimensions("image/jpeg", &encoded), Some((3, 2)));
        let url = format!("data:image/jpeg;base64,{encoded}");
        assert!(placeholder(&url).ends_with(": image/jpeg 3×2 not included in this request]"));
    }

    #[test]
    fn gif_and_all_three_webp_headers_produce_dimensions() {
        assert_eq!(
            image_dimensions("image/gif", &STANDARD.encode(b"GIF89a\x03\0\x02\0")),
            Some((3, 2))
        );
        for (kind, data) in [
            (*b"VP8X", vec![0, 0, 0, 0, 2, 0, 0, 1, 0, 0]),
            (*b"VP8L", vec![0x2f, 2, 0x40, 0, 0]),
            (*b"VP8 ", vec![0, 0, 0, 0x9d, 1, 0x2a, 3, 0, 2, 0]),
        ] {
            let mut webp = b"RIFF".to_vec();
            webp.extend_from_slice(&(12 + data.len() as u32).to_le_bytes());
            webp.extend_from_slice(b"WEBP");
            webp.extend_from_slice(&kind);
            webp.extend_from_slice(&(data.len() as u32).to_le_bytes());
            webp.extend_from_slice(&data);
            assert_eq!(
                image_dimensions("image/webp", &STANDARD.encode(webp)),
                Some((3, 2))
            );
        }
    }

    #[test]
    fn malformed_headers_and_non_data_urls_omit_dimensions() {
        for mime in ["image/png", "image/jpeg", "image/gif", "image/webp"] {
            assert_eq!(image_dimensions(mime, "bad"), None);
            assert_eq!(image_dimensions(mime, &STANDARD.encode([0; 32])), None);
        }
        let text = placeholder("https://example.com/image.png");
        assert_eq!(text, "[image 99a19c21: image not included in this request]");
        let text = placeholder("data:image/png;base64,bad");
        assert_eq!(
            text,
            "[image 0e0d2804: image/png not included in this request]"
        );
    }

    #[test]
    fn header_decoder_only_decodes_the_requested_prefix() {
        let encoded = STANDARD.encode([0; 4096]);
        let mut prefix = HeaderPrefix::new(&encoded);
        assert_eq!(prefix.read(24).unwrap().len(), 24);
        assert_eq!(prefix.bytes.len(), 24);
        assert_eq!(prefix.offset, 32);
    }
}

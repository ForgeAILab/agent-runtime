//! Server-Sent Events frame parsing.
//!
//! Ported and adapted from Nyx `crates/nyx-provider/src/sse.rs`
//! (donor revision recorded in `PROVENANCE.md`). The parser is neutral: it
//! normalizes newlines, buffers partial input, splits frames on a blank line,
//! and joins multiple `data:` lines. It carries no vendor or product policy.

/// One parsed SSE frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseFrame {
    /// The `event:` field, if present.
    pub event: Option<String>,
    /// The joined `data:` payload.
    pub data: String,
}

/// An incremental SSE frame parser.
#[derive(Debug, Default)]
pub struct SseFrameParser {
    buffer: String,
    pending_cr: bool,
}

impl SseFrameParser {
    /// A new, empty parser.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends raw text, normalizing `\r\n` and `\r` to `\n`.
    pub fn push_str(&mut self, chunk: &str) {
        // Normalize newlines so frame splitting is uniform across servers,
        // including when an HTTP chunk boundary splits a CRLF pair.
        let mut chars = chunk.chars().peekable();
        if self.pending_cr {
            if chars.peek() == Some(&'\n') {
                chars.next();
            }
            self.buffer.push('\n');
            self.pending_cr = false;
        }
        while let Some(ch) = chars.next() {
            if ch != '\r' {
                self.buffer.push(ch);
                continue;
            }
            match chars.peek() {
                Some('\n') => {
                    chars.next();
                    self.buffer.push('\n');
                }
                Some(_) => self.buffer.push('\n'),
                None => self.pending_cr = true,
            }
        }
    }

    /// Current buffered byte length, for adapter-owned stream bounds.
    pub(crate) fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Drains all complete frames (those terminated by a blank line).
    pub fn drain_frames(&mut self) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        while let Some(idx) = self.buffer.find("\n\n") {
            let raw = self.buffer[..idx].to_string();
            self.buffer.drain(..idx + 2);
            if let Some(frame) = Self::parse_frame(&raw) {
                frames.push(frame);
            }
        }
        frames
    }

    /// Flushes a trailing partial frame (called at end-of-stream).
    pub fn finish(&mut self) -> Option<SseFrame> {
        if self.pending_cr {
            self.buffer.push('\n');
            self.pending_cr = false;
        }
        let raw = std::mem::take(&mut self.buffer);
        let trimmed = raw.trim_matches('\n');
        if trimmed.is_empty() {
            None
        } else {
            Self::parse_frame(trimmed)
        }
    }

    fn parse_frame(raw: &str) -> Option<SseFrame> {
        let mut event = None;
        let mut data_lines: Vec<&str> = Vec::new();
        for line in raw.split('\n') {
            if line.is_empty() || line.starts_with(':') {
                continue; // comment / keep-alive
            }
            if let Some(rest) = line.strip_prefix("event:") {
                event = Some(rest.strip_prefix(' ').unwrap_or(rest).to_string());
            } else if let Some(rest) = line.strip_prefix("data:") {
                data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
            }
        }
        if event.is_none() && data_lines.is_empty() {
            return None;
        }
        Some(SseFrame {
            event,
            data: data_lines.join("\n"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_frames_on_blank_line_and_joins_data() {
        let mut p = SseFrameParser::new();
        p.push_str("data: hello\r\ndata: world\r\n\r\ndata: [DONE]\n\n");
        let frames = p.drain_frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data, "hello\nworld");
        assert_eq!(frames[1].data, "[DONE]");
    }

    #[test]
    fn buffers_partial_frames_across_pushes() {
        let mut p = SseFrameParser::new();
        p.push_str("data: par");
        assert!(p.drain_frames().is_empty());
        p.push_str("tial\n\n");
        let frames = p.drain_frames();
        assert_eq!(frames[0].data, "partial");
    }

    #[test]
    fn normalizes_crlf_split_across_chunks_without_inventing_a_blank_line() {
        let mut p = SseFrameParser::new();
        p.push_str("data: first\r");
        assert!(p.drain_frames().is_empty());
        p.push_str("\ndata: second\r\n\r\n");
        let frames = p.drain_frames();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "first\nsecond");
    }

    #[test]
    fn finish_flushes_trailing_frame() {
        let mut p = SseFrameParser::new();
        p.push_str("data: tail");
        assert!(p.drain_frames().is_empty());
        assert_eq!(p.finish().unwrap().data, "tail");
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseFrame {
    pub event: Option<String>,
    pub data: String,
}

#[derive(Debug, Default)]
pub(crate) struct SseFrameParser {
    buffer: String,
}

impl SseFrameParser {
    pub(crate) fn push_str(&mut self, chunk: &str) {
        let normalized = chunk.replace("\r\n", "\n").replace('\r', "\n");
        self.buffer.push_str(&normalized);
    }

    pub(crate) fn drain_frames(&mut self) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        while let Some(pos) = self.buffer.find("\n\n") {
            let frame = self.buffer[..pos].to_string();
            self.buffer = self.buffer[pos + 2..].to_string();
            if let Some(parsed) = parse_frame(&frame) {
                frames.push(parsed);
            }
        }
        frames
    }

    pub(crate) fn finish(&mut self) -> Vec<SseFrame> {
        if self.buffer.trim().is_empty() {
            self.buffer.clear();
            return Vec::new();
        }
        let frame = std::mem::take(&mut self.buffer);
        parse_frame(&frame).into_iter().collect()
    }
}

fn parse_frame(frame: &str) -> Option<SseFrame> {
    let mut event = None;
    let mut data = String::new();

    for line in frame.lines() {
        if let Some(rest) = line.strip_prefix("event:") {
            event = Some(strip_sse_field_prefix(rest).to_string());
        } else if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            data.push_str(strip_sse_field_prefix(rest));
        }
    }

    (!data.is_empty()).then_some(SseFrame { event, data })
}

fn strip_sse_field_prefix(value: &str) -> &str {
    value.strip_prefix(' ').unwrap_or(value)
}

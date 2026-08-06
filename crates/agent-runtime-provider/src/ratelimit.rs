//! Rate-limit header parsing and exhaustion classification.
//!
//! Providers report how consumed an account's usage window is in response
//! headers, in three mutually unintelligible dialects. This module turns any
//! of them into the one normalized [`RateLimitSnapshot`] the rest of the
//! runtime speaks, and answers the one classification question the retry
//! policy cannot: is this 429 a burst throttle that will clear in a moment, or
//! an account that is spent until some later hour?
//!
//! Two rules govern everything here:
//!
//! * **Absence is not zero.** A header the provider did not send contributes
//!   nothing. A window with no reported percentage is a window whose
//!   consumption is unknown, which is a different claim from "0% used" and a
//!   very different claim from "exhausted".
//! * **Nothing is invented.** A relative reset stays relative, because this
//!   crate has no clock and converting one against a guessed "now" would put a
//!   fabricated timestamp into an observation whose only value is fidelity.
//!
//! Parsing is best-effort by design. Provider header families change without
//! notice, and an unrecognized or malformed header contributes nothing rather
//! than failing the attempt: drift degrades a meter to "unknown", never to a
//! wrong number and never to a broken turn.

use agent_runtime_core::provider::{
    ProviderError, ProviderErrorKind, RateLimitSnapshot, RateLimitWindow,
};

/// How far out a reported reset may be while the rejection still counts as a
/// momentary throttle.
///
/// A provider that says "try again in 12 seconds" is asking for backoff, which
/// the existing retry discipline already does correctly. A provider that says
/// "try again in 40 minutes" is saying the window is spent, and retrying it is
/// spending attempts on a certainty. Sixty seconds sits well above real burst
/// throttles and well below any published usage window, and misjudging toward
/// "transient" merely restores the behavior that predates this module.
const TRANSIENT_HORIZON_MS: u64 = 60_000;

/// A rejection classified as a spent usage window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exhaustion {
    /// When the window reopens, in Unix milliseconds, when the provider said.
    pub resets_at_ms: Option<u64>,
}

/// Builds a normalized snapshot from response headers.
///
/// Every known family is attempted, because one response may carry more than
/// one (a gateway adding `x-ratelimit-*` in front of a vendor's own). Windows
/// that report nothing are dropped, so an empty snapshot means exactly "the
/// provider reported nothing".
pub fn snapshot_from_headers(headers: &[(String, String)]) -> RateLimitSnapshot {
    let lookup = HeaderLookup(headers);
    let mut snapshot = RateLimitSnapshot::new();
    parse_anthropic(&lookup, &mut snapshot);
    parse_openai(&lookup, &mut snapshot);
    parse_codex(&lookup, &mut snapshot);
    snapshot
}

/// Decides whether a rejection means the usage window is spent.
///
/// Returns `None` for anything the existing retry discipline should keep
/// handling — including a 429 whose reported reset is near enough to wait out.
/// `now_ms` resolves windows that reported only a relative reset.
pub fn classify_rejection(
    status: u16,
    snapshot: &RateLimitSnapshot,
    retry_after_ms: Option<u64>,
    now_ms: u64,
) -> Option<Exhaustion> {
    if status != 429 {
        return None;
    }

    // A window the provider itself reports as fully consumed settles the
    // question regardless of how soon it claims to reset.
    if snapshot.is_exhausted() {
        let resets_at_ms = snapshot
            .windows
            .iter()
            .filter(|window| window.is_exhausted())
            .filter_map(|window| window.resets_at_ms_from(now_ms))
            .min();
        return Some(Exhaustion { resets_at_ms });
    }

    // Otherwise the reported wait decides. A reset the provider stated wins
    // over a `retry-after` hint, which is advisory backoff rather than a
    // statement about the window.
    let reported_reset = snapshot.soonest_reset_ms(now_ms);
    let wait_ms = match reported_reset {
        Some(at) => at.saturating_sub(now_ms),
        None => retry_after_ms?,
    };
    if wait_ms <= TRANSIENT_HORIZON_MS {
        return None;
    }
    Some(Exhaustion {
        resets_at_ms: reported_reset.or_else(|| retry_after_ms.map(|ms| now_ms.saturating_add(ms))),
    })
}

/// Rewrites a rate-limited error as a typed exhaustion when the headers say so.
///
/// Transports classify status codes before an adapter sees them, so this is
/// the seam where a `RateLimited` error becomes `LimitExhausted` without every
/// transport reimplementing the judgement.
pub fn apply_exhaustion(error: ProviderError, exhaustion: Exhaustion) -> ProviderError {
    let mut error = ProviderError {
        kind: ProviderErrorKind::LimitExhausted,
        // Exhaustion is not cleared by waiting out a backoff, so the retry
        // discipline must stop treating it as another attempt's problem.
        retryable: false,
        retry_after_ms: None,
        message: "the provider reports this credential's usage window is spent".to_owned(),
        ..error
    };
    if let Some(resets_at_ms) = exhaustion.resets_at_ms {
        error = error.limit_resets_at(resets_at_ms);
    }
    error
}

/// Case-insensitive access to a header list.
struct HeaderLookup<'a>(&'a [(String, String)]);

impl HeaderLookup<'_> {
    fn get(&self, name: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.trim())
            .filter(|value| !value.is_empty())
    }

    fn u64(&self, name: &str) -> Option<u64> {
        self.get(name)?.parse().ok()
    }

    fn f64(&self, name: &str) -> Option<f64> {
        let value: f64 = self.get(name)?.parse().ok()?;
        value.is_finite().then_some(value)
    }
}

/// Parses the `anthropic-ratelimit-*` family.
///
/// Anthropic reports limit/remaining counts plus an RFC 3339 reset instant,
/// per category, and a `unified` window on subscription-backed credentials.
fn parse_anthropic(headers: &HeaderLookup<'_>, snapshot: &mut RateLimitSnapshot) {
    for category in [
        "requests",
        "tokens",
        "input-tokens",
        "output-tokens",
        "unified",
    ] {
        let prefix = format!("anthropic-ratelimit-{category}");
        let mut window = RateLimitWindow::new(category);
        window.limit = headers.u64(&format!("{prefix}-limit"));
        window.remaining = headers.u64(&format!("{prefix}-remaining"));
        window.resets_at_ms = headers
            .get(&format!("{prefix}-reset"))
            .and_then(parse_rfc3339_ms);
        snapshot.push(window);
    }
}

/// Parses the `x-ratelimit-*` family used by OpenAI-compatible endpoints.
///
/// Resets arrive as Go duration strings (`"6m0s"`, `"12ms"`), which are
/// relative and stay that way.
fn parse_openai(headers: &HeaderLookup<'_>, snapshot: &mut RateLimitSnapshot) {
    for category in ["requests", "tokens"] {
        let mut window = RateLimitWindow::new(category);
        window.limit = headers.u64(&format!("x-ratelimit-limit-{category}"));
        window.remaining = headers.u64(&format!("x-ratelimit-remaining-{category}"));
        window.resets_in_ms = headers
            .get(&format!("x-ratelimit-reset-{category}"))
            .and_then(parse_go_duration_ms);
        snapshot.push(window);
    }
}

/// Parses the `x-codex-*` family, which reports consumption as a percentage.
///
/// This is the one family that states used-percent directly, and the shape the
/// normalized window was modeled on.
fn parse_codex(headers: &HeaderLookup<'_>, snapshot: &mut RateLimitSnapshot) {
    for category in ["primary", "secondary"] {
        let prefix = format!("x-codex-{category}");
        let mut window = RateLimitWindow::new(category);
        window.used_percent = headers.f64(&format!("{prefix}-used-percent"));
        window.window_seconds = headers
            .u64(&format!("{prefix}-window-minutes"))
            .map(|minutes| minutes.saturating_mul(60));
        window.resets_in_ms = headers
            .u64(&format!("{prefix}-reset-after-seconds"))
            .map(|seconds| seconds.saturating_mul(1_000));
        snapshot.push(window);
    }
}

/// Parses a Go duration string into milliseconds.
///
/// Handles the forms these headers actually carry — `"1s"`, `"6m0s"`,
/// `"1h2m3s"`, `"12ms"`, `"0s"` — and returns `None` for anything else rather
/// than guessing at a partially understood value.
fn parse_go_duration_ms(value: &str) -> Option<u64> {
    let mut total_ms: u64 = 0;
    let mut digits = String::new();
    let mut matched = false;
    let mut rest = value;

    while !rest.is_empty() {
        digits.clear();
        let mut chars = rest.char_indices();
        let mut split = rest.len();
        for (index, ch) in chars.by_ref() {
            if ch.is_ascii_digit() || ch == '.' {
                digits.push(ch);
            } else {
                split = index;
                break;
            }
        }
        if digits.is_empty() {
            return None;
        }
        let amount: f64 = digits.parse().ok()?;
        let unit_start = split;
        // The unit is the alphabetic run following the digits.
        let unit_end = rest[unit_start..]
            .find(|ch: char| ch.is_ascii_digit())
            .map_or(rest.len(), |offset| unit_start + offset);
        let unit = &rest[unit_start..unit_end];
        let millis = match unit {
            "ns" => amount / 1_000_000.0,
            "us" | "µs" => amount / 1_000.0,
            "ms" => amount,
            "s" => amount * 1_000.0,
            "m" => amount * 60_000.0,
            "h" => amount * 3_600_000.0,
            _ => return None,
        };
        total_ms = total_ms.saturating_add(millis as u64);
        matched = true;
        rest = &rest[unit_end..];
    }

    matched.then_some(total_ms)
}

/// Parses an RFC 3339 instant into Unix milliseconds.
///
/// Written out rather than pulled from a date crate because this is the only
/// date parsing in the package and the accepted shape is fixed: a provider
/// reset stamp. Anything outside that shape returns `None`, which normalizes
/// to "the provider reported no reset".
fn parse_rfc3339_ms(value: &str) -> Option<u64> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 || bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }
    if !matches!(bytes[10], b'T' | b't' | b' ') || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }

    let year: i64 = value.get(0..4)?.parse().ok()?;
    let month: i64 = value.get(5..7)?.parse().ok()?;
    let day: i64 = value.get(8..10)?.parse().ok()?;
    let hour: i64 = value.get(11..13)?.parse().ok()?;
    let minute: i64 = value.get(14..16)?.parse().ok()?;
    let second: i64 = value.get(17..19)?.parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    // Skip any fractional seconds, then read the zone.
    let mut rest = &value[19..];
    if let Some(stripped) = rest.strip_prefix('.') {
        let digits = stripped
            .find(|ch: char| !ch.is_ascii_digit())
            .unwrap_or(stripped.len());
        rest = &stripped[digits..];
    }

    let offset_seconds = match rest.as_bytes().first() {
        Some(b'Z' | b'z') => 0,
        Some(sign @ (b'+' | b'-')) => {
            let sign = if *sign == b'-' { -1 } else { 1 };
            let hours: i64 = rest.get(1..3)?.parse().ok()?;
            // Both `+01:00` and `+0100` occur in the wild.
            let minutes: i64 = match rest.as_bytes().get(3) {
                Some(b':') => rest.get(4..6)?.parse().ok()?,
                Some(_) => rest.get(3..5)?.parse().ok()?,
                None => return None,
            };
            sign * (hours * 3_600 + minutes * 60)
        }
        _ => return None,
    };

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + hour * 3_600 + minute * 60 + second - offset_seconds;
    u64::try_from(seconds).ok()?.checked_mul(1_000)
}

/// Days since the Unix epoch for a civil date (Howard Hinnant's algorithm).
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    #[test]
    fn no_recognized_headers_produce_no_snapshot() {
        let snapshot = snapshot_from_headers(&headers(&[
            ("content-type", "text/event-stream"),
            ("x-request-id", "req_123"),
        ]));
        assert!(snapshot.is_empty());
        assert!(!snapshot.is_exhausted());
        assert_eq!(snapshot.most_consumed(), None);
    }

    #[test]
    fn the_codex_family_reports_percent_window_and_relative_reset() {
        let snapshot = snapshot_from_headers(&headers(&[
            ("x-codex-primary-used-percent", "82.5"),
            ("x-codex-primary-window-minutes", "300"),
            ("x-codex-primary-reset-after-seconds", "3600"),
        ]));

        let window = snapshot.most_consumed().expect("a reported window");
        assert_eq!(window.id.as_deref(), Some("primary"));
        assert_eq!(window.used_percent, Some(82.5));
        assert_eq!(window.window_seconds, Some(18_000));
        assert_eq!(window.resets_in_ms, Some(3_600_000));
        // Relative stays relative until a caller with a clock resolves it.
        assert_eq!(window.resets_at_ms, None);
        assert_eq!(window.resets_at_ms_from(1_000), Some(3_601_000));
    }

    #[test]
    fn the_anthropic_family_reports_counts_and_an_absolute_reset() {
        let snapshot = snapshot_from_headers(&headers(&[
            ("anthropic-ratelimit-requests-limit", "1000"),
            ("anthropic-ratelimit-requests-remaining", "250"),
            ("anthropic-ratelimit-requests-reset", "2026-08-04T17:00:00Z"),
        ]));

        let window = snapshot.most_consumed().expect("a reported window");
        assert_eq!(window.limit, Some(1_000));
        assert_eq!(window.remaining, Some(250));
        // Not *reported* as a percentage, so it is only ever derived on demand.
        assert_eq!(window.used_percent, None);
        assert_eq!(window.used_percent_or_derived(), Some(75.0));
        assert_eq!(window.resets_at_ms, Some(1_785_862_800_000));
    }

    #[test]
    fn the_openai_family_reports_go_durations_as_relative_resets() {
        let snapshot = snapshot_from_headers(&headers(&[
            ("x-ratelimit-limit-tokens", "10000"),
            ("x-ratelimit-remaining-tokens", "10000"),
            ("x-ratelimit-reset-tokens", "6m0s"),
        ]));

        let window = &snapshot.windows[0];
        assert_eq!(window.resets_in_ms, Some(360_000));
        assert_eq!(window.used_percent_or_derived(), Some(0.0));
        assert!(!snapshot.is_exhausted());
    }

    #[test]
    fn go_durations_parse_across_the_forms_headers_use() {
        assert_eq!(parse_go_duration_ms("0s"), Some(0));
        assert_eq!(parse_go_duration_ms("1s"), Some(1_000));
        assert_eq!(parse_go_duration_ms("12ms"), Some(12));
        assert_eq!(parse_go_duration_ms("6m0s"), Some(360_000));
        assert_eq!(parse_go_duration_ms("1h2m3s"), Some(3_723_000));
        assert_eq!(parse_go_duration_ms("500us"), Some(0));
        // Unrecognized shapes contribute nothing rather than a wrong number.
        assert_eq!(parse_go_duration_ms("soon"), None);
        assert_eq!(parse_go_duration_ms("10"), None);
        assert_eq!(parse_go_duration_ms("10x"), None);
    }

    #[test]
    fn rfc3339_parses_zulu_offsets_and_fractional_seconds() {
        assert_eq!(
            parse_rfc3339_ms("2026-08-04T17:00:00Z"),
            Some(1_785_862_800_000)
        );
        assert_eq!(
            parse_rfc3339_ms("2026-08-04T17:00:00.250Z"),
            Some(1_785_862_800_000)
        );
        // 18:00 at +01:00 is the same instant as 17:00 UTC.
        assert_eq!(
            parse_rfc3339_ms("2026-08-04T18:00:00+01:00"),
            Some(1_785_862_800_000)
        );
        assert_eq!(parse_rfc3339_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_rfc3339_ms("not-a-date"), None);
        assert_eq!(parse_rfc3339_ms("2026-13-04T17:00:00Z"), None);
    }

    #[test]
    fn a_short_reset_stays_a_transient_throttle() {
        let snapshot = snapshot_from_headers(&headers(&[
            ("x-ratelimit-limit-requests", "100"),
            ("x-ratelimit-remaining-requests", "0"),
            ("x-ratelimit-reset-requests", "12s"),
        ]));
        // Remaining 0 of 100 derives to 100% used, which settles it regardless
        // of the short reset: the window itself is spent.
        assert!(snapshot.is_exhausted());

        let unspent = snapshot_from_headers(&headers(&[("x-ratelimit-reset-requests", "12s")]));
        assert_eq!(classify_rejection(429, &unspent, Some(12_000), 0), None);
    }

    #[test]
    fn a_distant_reset_is_exhaustion() {
        let snapshot =
            snapshot_from_headers(&headers(&[("x-codex-primary-reset-after-seconds", "3600")]));
        let verdict = classify_rejection(429, &snapshot, None, 1_000).expect("exhaustion");
        assert_eq!(verdict.resets_at_ms, Some(3_601_000));
    }

    #[test]
    fn a_spent_window_is_exhaustion_even_without_a_reset() {
        let snapshot = snapshot_from_headers(&headers(&[("x-codex-primary-used-percent", "100")]));
        let verdict = classify_rejection(429, &snapshot, None, 0).expect("exhaustion");
        assert_eq!(verdict.resets_at_ms, None);
    }

    #[test]
    fn only_a_429_is_ever_exhaustion() {
        let snapshot = snapshot_from_headers(&headers(&[("x-codex-primary-used-percent", "100")]));
        assert_eq!(classify_rejection(500, &snapshot, None, 0), None);
        assert_eq!(classify_rejection(401, &snapshot, None, 0), None);
    }

    #[test]
    fn a_rejection_with_nothing_reported_stays_transient() {
        let snapshot = RateLimitSnapshot::new();
        assert_eq!(classify_rejection(429, &snapshot, None, 0), None);
    }

    #[test]
    fn applying_exhaustion_clears_retryability_and_keeps_metadata() {
        let mut original =
            ProviderError::new(ProviderErrorKind::RateLimited, "429").retry_after(30);
        original.metadata.insert("http.status", 429i64);

        let error = apply_exhaustion(
            original,
            Exhaustion {
                resets_at_ms: Some(1_785_862_800_000),
            },
        );

        assert_eq!(error.kind, ProviderErrorKind::LimitExhausted);
        assert!(!error.retryable);
        assert_eq!(error.retry_after_ms, None);
        assert_eq!(error.limit_resets_at_ms, Some(1_785_862_800_000));
        // The transport's redaction-safe context survives reclassification.
        assert!(error.metadata.get("http.status").is_some());
    }

    #[test]
    fn the_most_consumed_window_wins_the_meter() {
        let snapshot = snapshot_from_headers(&headers(&[
            ("x-codex-primary-used-percent", "40"),
            ("x-codex-secondary-used-percent", "91"),
        ]));
        assert_eq!(
            snapshot.most_consumed().and_then(|w| w.id.as_deref()),
            Some("secondary")
        );
    }

    #[test]
    fn the_soonest_reset_wins_across_windows() {
        let snapshot = snapshot_from_headers(&headers(&[
            ("x-codex-primary-reset-after-seconds", "3600"),
            ("x-codex-secondary-reset-after-seconds", "600"),
        ]));
        assert_eq!(snapshot.soonest_reset_ms(0), Some(600_000));
    }
}

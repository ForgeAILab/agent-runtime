use nyx_core::{UsageFilter, UsageGroupBy};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

const WINDOW_PARSE_ERROR: &str = "window must be today|this_week|this_month|YYYY-MM-DD/YYYY-MM-DD";

pub fn parse_window_filter(window: Option<&str>) -> Result<UsageFilter, String> {
    let now = OffsetDateTime::now_utc();
    let now_ms = now.unix_timestamp_nanos() as u64 / 1_000_000;

    let (since, until) = match window.unwrap_or("this_month") {
        "today" => {
            let start = PrimitiveDateTime::new(now.date(), Time::MIDNIGHT).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(now_ms),
            )
        }
        "this_week" => {
            let offset_days = now.weekday().number_days_from_monday() as i64;
            let start_date = now
                .date()
                .checked_sub(time::Duration::days(offset_days))
                .unwrap_or(now.date());
            let start = PrimitiveDateTime::new(start_date, Time::MIDNIGHT).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(now_ms),
            )
        }
        "this_month" => {
            let date = now.date();
            let start_date = Date::from_calendar_date(date.year(), date.month(), 1)
                .map_err(|err| err.to_string())?;
            let start = PrimitiveDateTime::new(start_date, Time::MIDNIGHT).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(now_ms),
            )
        }
        value => {
            let (start_raw, end_raw) = value
                .split_once('/')
                .ok_or_else(|| WINDOW_PARSE_ERROR.to_string())?;
            let format =
                &time::format_description::parse("[year]-[month]-[day]").map_err(|err| err.to_string())?;
            let start_date = Date::parse(start_raw, format).map_err(|err| err.to_string())?;
            let end_date = Date::parse(end_raw, format).map_err(|err| err.to_string())?;
            let start = PrimitiveDateTime::new(start_date, Time::MIDNIGHT).assume_utc();
            let end = PrimitiveDateTime::new(end_date, Time::MAX).assume_utc();
            (
                Some(start.unix_timestamp_nanos() as u64 / 1_000_000),
                Some(end.unix_timestamp_nanos() as u64 / 1_000_000),
            )
        }
    };

    Ok(UsageFilter {
        since,
        until,
        channel_id: None,
        group_by: None,
    })
}

pub fn parse_group_by(value: Option<&str>) -> Result<Option<UsageGroupBy>, String> {
    match value {
        Some("channel") => Ok(Some(UsageGroupBy::Channel)),
        Some("model") => Ok(Some(UsageGroupBy::Model)),
        Some(other) => Err(format!("unsupported group_by value `{other}`")),
        None => Ok(None),
    }
}

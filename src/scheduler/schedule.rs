//! Cron helpers: compute a trigger's next fire time from its cron expression,
//! evaluated in the trigger's IANA timezone.
//!
//! Cron can't be evaluated in SQL, so the scheduler stores `next_fire_time`
//! (computed here) and the claim query selects rows where `next_fire_time <= now()`.
//!
//! Cron format is 6-field (sec min hour day-of-month month day-of-week), e.g.
//! `0 0 2 * * *` = 02:00 every day.

use std::str::FromStr;
use std::time::SystemTime;

use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use cron::Schedule;

/// Next fire strictly after `after`, for a cron evaluated in `tz_name` (IANA, e.g.
/// "America/New_York"; unknown/blank falls back to UTC). Returned as a UTC-naive
/// timestamp — what we store in and compare against `scheduled_trigger.next_fire_time`.
pub fn next_fire_after_tz(
    cron_expr: &str,
    tz_name: &str,
    after: DateTime<Utc>,
) -> Option<NaiveDateTime> {
    let schedule = Schedule::from_str(cron_expr).ok()?;
    let tz: Tz = tz_name.parse().unwrap_or(chrono_tz::UTC);
    let after_tz = after.with_timezone(&tz);
    let next = schedule.after(&after_tz).next()?;
    Some(next.with_timezone(&Utc).naive_utc())
}

/// Next fire relative to the current instant, in `tz_name`. Uses `SystemTime` (not
/// `chrono::Utc::now`) so it works with chrono's `clock` feature disabled.
pub fn next_fire_from_now_tz(cron_expr: &str, tz_name: &str) -> Option<NaiveDateTime> {
    next_fire_after_tz(cron_expr, tz_name, DateTime::<Utc>::from(SystemTime::now()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn computes_next_daily_2am_utc() {
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = next_fire_after_tz("0 0 2 * * *", "UTC", after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 2, 0, 0).unwrap().naive_utc());
    }

    #[test]
    fn after_2am_rolls_to_next_day() {
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 3, 0, 0).unwrap();
        let next = next_fire_after_tz("0 0 2 * * *", "UTC", after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 2, 2, 0, 0).unwrap().naive_utc());
    }

    #[test]
    fn every_30_min() {
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 10, 5, 0).unwrap();
        let next = next_fire_after_tz("0 0,30 * * * *", "UTC", after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 10, 30, 0).unwrap().naive_utc());
    }

    #[test]
    fn honors_timezone() {
        // 02:00 America/New_York on 2026-01-01 (EST, UTC-5) = 07:00 UTC.
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = next_fire_after_tz("0 0 2 * * *", "America/New_York", after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 7, 0, 0).unwrap().naive_utc());
    }

    #[test]
    fn invalid_cron_returns_none() {
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert!(next_fire_after_tz("not a cron", "UTC", after).is_none());
    }
}

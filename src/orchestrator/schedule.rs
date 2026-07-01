//! Cron helpers: compute a job's next fire time from its cron expression.
//!
//! Cron can't be evaluated in SQL, so the scheduler stores a `next_fire_time`
//! (computed here) and the claim query selects rows where `next_fire_time <=
//! now()`. After firing, `next_fire_time` is advanced via [`next_fire_after`].
//!
//! Cron format is 6-field (sec min hour day-of-month month day-of-week), e.g.
//! `0 0 2 * * *` = 02:00 every day — matching the finzly job-config contract.

use std::str::FromStr;
use std::time::SystemTime;

use chrono::{DateTime, Utc};
use cron::Schedule;

/// Next fire time strictly after `after`, or `None` if the cron is invalid or
/// has no future occurrence.
pub fn next_fire_after(cron_expr: &str, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let schedule = Schedule::from_str(cron_expr).ok()?;
    schedule.after(&after).next()
}

/// Next fire time relative to the current instant. Uses `SystemTime` (not
/// `chrono::Utc::now`) so it works with chrono's `clock` feature disabled.
pub fn next_fire_from_now(cron_expr: &str) -> Option<DateTime<Utc>> {
    next_fire_after(cron_expr, DateTime::<Utc>::from(SystemTime::now()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn computes_next_daily_2am() {
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = next_fire_after("0 0 2 * * *", after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 2, 0, 0).unwrap());
    }

    #[test]
    fn after_2am_rolls_to_next_day() {
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 3, 0, 0).unwrap();
        let next = next_fire_after("0 0 2 * * *", after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 2, 2, 0, 0).unwrap());
    }

    #[test]
    fn every_30_min() {
        // sec=0, min=0/30 -> :00 and :30 each hour
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 10, 5, 0).unwrap();
        let next = next_fire_after("0 0,30 * * * *", after).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 10, 30, 0).unwrap());
    }

    #[test]
    fn invalid_cron_returns_none() {
        let after = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        assert!(next_fire_after("not a cron", after).is_none());
    }
}

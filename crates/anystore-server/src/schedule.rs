//! Daily maintenance schedule.

use chrono::{DateTime, Days, FixedOffset, NaiveTime, Utc};

/// A wall-clock time of day in a fixed UTC offset, e.g. 03:00 at +08:00.
///
/// A fixed offset rather than a named zone: the deployment region observes no
/// daylight saving time, and chrono needs no time zone database for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DailySchedule {
    pub at: NaiveTime,
    pub offset: FixedOffset,
}

impl DailySchedule {
    /// The first scheduled instant strictly after `now`.
    pub fn next_after(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        let local = now.with_timezone(&self.offset);
        let today = local
            .date_naive()
            .and_time(self.at)
            .and_local_timezone(self.offset)
            .single()
            .expect("a fixed offset maps every local time to exactly one instant");
        let next = if today > local {
            today
        } else {
            today
                .checked_add_days(Days::new(1))
                .expect("the next day is representable")
        };
        next.with_timezone(&Utc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn three_am_shanghai() -> DailySchedule {
        DailySchedule {
            at: NaiveTime::from_hms_opt(3, 0, 0).unwrap(),
            offset: FixedOffset::east_opt(8 * 3600).unwrap(),
        }
    }

    fn utc(raw: &str) -> DateTime<Utc> {
        raw.parse().unwrap()
    }

    #[test]
    fn runs_later_the_same_local_day() {
        // 01:00 at +08:00.
        let next = three_am_shanghai().next_after(utc("2026-10-24T17:00:00Z"));
        assert_eq!(next, utc("2026-10-24T19:00:00Z"));
    }

    #[test]
    fn rolls_over_to_the_next_local_day() {
        // 10:00 at +08:00, and exactly 03:00 at +08:00.
        let schedule = three_am_shanghai();
        assert_eq!(
            schedule.next_after(utc("2026-10-24T02:00:00Z")),
            utc("2026-10-24T19:00:00Z")
        );
        assert_eq!(
            schedule.next_after(utc("2026-10-24T19:00:00Z")),
            utc("2026-10-25T19:00:00Z")
        );
    }
}

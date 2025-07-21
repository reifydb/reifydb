// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// An interval value representing a duration between two points in time.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Interval {
    months: i32,    // Store years*12 + months
    days: i32,      // Separate days (don't normalize to months due to variable month length)
    nanos: i64,     // All time components as nanoseconds
}

impl Default for Interval {
    fn default() -> Self {
        Self::zero()
    }
}

impl Interval {
    pub fn new(months: i32, days: i32, nanos: i64) -> Self {
        Self { months, days, nanos }
    }

    pub fn from_seconds(seconds: i64) -> Self {
        Self { 
            months: 0, 
            days: 0, 
            nanos: seconds * 1_000_000_000 
        }
    }

    pub fn from_milliseconds(milliseconds: i64) -> Self {
        Self { 
            months: 0, 
            days: 0, 
            nanos: milliseconds * 1_000_000 
        }
    }

    pub fn from_microseconds(microseconds: i64) -> Self {
        Self { 
            months: 0, 
            days: 0, 
            nanos: microseconds * 1_000 
        }
    }

    pub fn from_nanoseconds(nanoseconds: i64) -> Self {
        Self { 
            months: 0, 
            days: 0, 
            nanos: nanoseconds 
        }
    }

    pub fn from_minutes(minutes: i64) -> Self {
        Self { 
            months: 0, 
            days: 0, 
            nanos: minutes * 60 * 1_000_000_000 
        }
    }

    pub fn from_hours(hours: i64) -> Self {
        Self { 
            months: 0, 
            days: 0, 
            nanos: hours * 60 * 60 * 1_000_000_000 
        }
    }

    pub fn from_days(days: i64) -> Self {
        Self { 
            months: 0, 
            days: days as i32, 
            nanos: 0 
        }
    }

    pub fn from_weeks(weeks: i64) -> Self {
        Self { 
            months: 0, 
            days: (weeks * 7) as i32, 
            nanos: 0 
        }
    }

    pub fn from_months(months: i64) -> Self {
        Self { 
            months: months as i32, 
            days: 0, 
            nanos: 0 
        }
    }

    pub fn from_years(years: i64) -> Self {
        Self { 
            months: (years * 12) as i32, 
            days: 0, 
            nanos: 0 
        }
    }

    pub fn zero() -> Self {
        Self { months: 0, days: 0, nanos: 0 }
    }

    pub fn seconds(&self) -> i64 {
        self.nanos / 1_000_000_000
    }

    pub fn milliseconds(&self) -> i64 {
        self.nanos / 1_000_000
    }

    pub fn microseconds(&self) -> i64 {
        self.nanos / 1_000
    }

    pub fn nanoseconds(&self) -> i64 {
        self.nanos
    }

    pub fn get_months(&self) -> i32 {
        self.months
    }

    pub fn get_days(&self) -> i32 {
        self.days
    }

    pub fn get_nanos(&self) -> i64 {
        self.nanos
    }

    pub fn is_positive(&self) -> bool {
        self.months > 0 || self.days > 0 || self.nanos > 0
    }

    pub fn is_negative(&self) -> bool {
        self.months < 0 || self.days < 0 || self.nanos < 0
    }

    pub fn abs(&self) -> Self {
        Self { 
            months: self.months.abs(),
            days: self.days.abs(),
            nanos: self.nanos.abs()
        }
    }

    pub fn negate(&self) -> Self {
        Self { 
            months: -self.months,
            days: -self.days,
            nanos: -self.nanos
        }
    }
}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare months first
        match self.months.cmp(&other.months) {
            std::cmp::Ordering::Equal => {
                // Then days
                match self.days.cmp(&other.days) {
                    std::cmp::Ordering::Equal => {
                        // Finally nanos
                        self.nanos.cmp(&other.nanos)
                    }
                    other_order => other_order
                }
            }
            other_order => other_order
        }
    }
}

impl Interval {
    /// Convert to nanoseconds (time component only)
    /// This only returns the sub-day nanoseconds component
    pub fn to_nanos(&self) -> i64 {
        self.nanos
    }

    /// Create from nanoseconds (time component only)
    pub fn from_nanos(nanos: i64) -> Self {
        Self { months: 0, days: 0, nanos }
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // ISO 8601 duration format: P[n]Y[n]M[n]DT[n]H[n]M[n.n]S
        if self.months == 0 && self.days == 0 && self.nanos == 0 {
            return write!(f, "PT0S");
        }

        write!(f, "P")?;

        // Extract years and months
        let years = self.months / 12;
        let months = self.months % 12;

        if years != 0 {
            write!(f, "{}Y", years)?;
        }

        if months != 0 {
            write!(f, "{}M", months)?;
        }

        // Time components from nanos with normalization
        let total_seconds = self.nanos / 1_000_000_000;
        let remaining_nanos = self.nanos % 1_000_000_000;
        
        // Normalize to days if hours >= 24
        let extra_days = total_seconds / 86400; // 24 * 60 * 60
        let remaining_seconds = total_seconds % 86400;
        
        let display_days = self.days + extra_days as i32;
        let hours = remaining_seconds / 3600;
        let minutes = (remaining_seconds % 3600) / 60;
        let seconds = remaining_seconds % 60;

        if display_days != 0 {
            write!(f, "{}D", display_days)?;
        }

        if hours != 0 || minutes != 0 || seconds != 0 || remaining_nanos != 0 {
            write!(f, "T")?;

            if hours != 0 {
                write!(f, "{}H", hours)?;
            }

            if minutes != 0 {
                write!(f, "{}M", minutes)?;
            }

            if seconds != 0 || remaining_nanos != 0 {
                if remaining_nanos != 0 {
                    // Format fractional seconds with trailing zeros removed
                    let fractional = remaining_nanos as f64 / 1_000_000_000.0;
                    let total_seconds_f = seconds as f64 + fractional;
                    // Remove trailing zeros from fractional part
                    let formatted_str = format!("{:.9}", total_seconds_f);
                    let formatted = formatted_str.trim_end_matches('0').trim_end_matches('.');
                    write!(f, "{}S", formatted)?;
                } else {
                    write!(f, "{}S", seconds)?;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_display_zero() {
        let interval = Interval::zero();
        assert_eq!(format!("{}", interval), "PT0S");

        let interval = Interval::from_seconds(0);
        assert_eq!(format!("{}", interval), "PT0S");

        let interval = Interval::from_nanoseconds(0);
        assert_eq!(format!("{}", interval), "PT0S");

        let interval = Interval::default();
        assert_eq!(format!("{}", interval), "PT0S");
    }

    #[test]
    fn test_interval_display_seconds_only() {
        let interval = Interval::from_seconds(1);
        assert_eq!(format!("{}", interval), "PT1S");

        let interval = Interval::from_seconds(30);
        assert_eq!(format!("{}", interval), "PT30S");

        let interval = Interval::from_seconds(59);
        assert_eq!(format!("{}", interval), "PT59S");
    }

    #[test]
    fn test_interval_display_minutes_only() {
        let interval = Interval::from_minutes(1);
        assert_eq!(format!("{}", interval), "PT1M");

        let interval = Interval::from_minutes(30);
        assert_eq!(format!("{}", interval), "PT30M");

        let interval = Interval::from_minutes(59);
        assert_eq!(format!("{}", interval), "PT59M");
    }

    #[test]
    fn test_interval_display_hours_only() {
        let interval = Interval::from_hours(1);
        assert_eq!(format!("{}", interval), "PT1H");

        let interval = Interval::from_hours(12);
        assert_eq!(format!("{}", interval), "PT12H");

        let interval = Interval::from_hours(23);
        assert_eq!(format!("{}", interval), "PT23H");
    }

    #[test]
    fn test_interval_display_days_only() {
        let interval = Interval::from_days(1);
        assert_eq!(format!("{}", interval), "P1D");

        let interval = Interval::from_days(7);
        assert_eq!(format!("{}", interval), "P7D");

        let interval = Interval::from_days(365);
        assert_eq!(format!("{}", interval), "P365D");
    }

    #[test]
    fn test_interval_display_weeks_only() {
        let interval = Interval::from_weeks(1);
        assert_eq!(format!("{}", interval), "P7D");

        let interval = Interval::from_weeks(2);
        assert_eq!(format!("{}", interval), "P14D");

        let interval = Interval::from_weeks(52);
        assert_eq!(format!("{}", interval), "P364D");
    }

    #[test]
    fn test_interval_display_combined_time() {
        // Hours and minutes
        let interval = Interval::new(0, 0, (1 * 60 * 60 + 30 * 60) * 1_000_000_000);
        assert_eq!(format!("{}", interval), "PT1H30M");

        // Minutes and seconds
        let interval = Interval::new(0, 0, (5 * 60 + 45) * 1_000_000_000);
        assert_eq!(format!("{}", interval), "PT5M45S");

        // Hours, minutes, and seconds
        let interval = Interval::new(0, 0, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000);
        assert_eq!(format!("{}", interval), "PT2H30M45S");
    }

    #[test]
    fn test_interval_display_combined_date_time() {
        // Days and hours
        let interval = Interval::new(0, 1, 2 * 60 * 60 * 1_000_000_000);
        assert_eq!(format!("{}", interval), "P1DT2H");

        // Days and minutes
        let interval = Interval::new(0, 1, 30 * 60 * 1_000_000_000);
        assert_eq!(format!("{}", interval), "P1DT30M");

        // Days, hours, and minutes
        let interval = Interval::new(0, 1, (2 * 60 * 60 + 30 * 60) * 1_000_000_000);
        assert_eq!(format!("{}", interval), "P1DT2H30M");

        // Days, hours, minutes, and seconds
        let interval = Interval::new(0, 1, (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000);
        assert_eq!(format!("{}", interval), "P1DT2H30M45S");
    }

    #[test]
    fn test_interval_display_milliseconds() {
        let interval = Interval::from_milliseconds(123);
        assert_eq!(format!("{}", interval), "PT0.123S");

        let interval = Interval::from_milliseconds(1);
        assert_eq!(format!("{}", interval), "PT0.001S");

        let interval = Interval::from_milliseconds(999);
        assert_eq!(format!("{}", interval), "PT0.999S");

        let interval = Interval::from_milliseconds(1500);
        assert_eq!(format!("{}", interval), "PT1.5S");
    }

    #[test]
    fn test_interval_display_microseconds() {
        let interval = Interval::from_microseconds(123456);
        assert_eq!(format!("{}", interval), "PT0.123456S");

        let interval = Interval::from_microseconds(1);
        assert_eq!(format!("{}", interval), "PT0.000001S");

        let interval = Interval::from_microseconds(999999);
        assert_eq!(format!("{}", interval), "PT0.999999S");

        let interval = Interval::from_microseconds(1500000);
        assert_eq!(format!("{}", interval), "PT1.5S");
    }

    #[test]
    fn test_interval_display_nanoseconds() {
        let interval = Interval::from_nanoseconds(123456789);
        assert_eq!(format!("{}", interval), "PT0.123456789S");

        let interval = Interval::from_nanoseconds(1);
        assert_eq!(format!("{}", interval), "PT0.000000001S");

        let interval = Interval::from_nanoseconds(999999999);
        assert_eq!(format!("{}", interval), "PT0.999999999S");

        let interval = Interval::from_nanoseconds(1500000000);
        assert_eq!(format!("{}", interval), "PT1.5S");
    }

    #[test]
    fn test_interval_display_fractional_seconds_with_integers() {
        // Seconds with milliseconds
        let interval = Interval::new(0, 0, 1 * 1_000_000_000 + 500 * 1_000_000);
        assert_eq!(format!("{}", interval), "PT1.5S");

        // Seconds with microseconds
        let interval = Interval::new(0, 0, 2 * 1_000_000_000 + 123456 * 1_000);
        assert_eq!(format!("{}", interval), "PT2.123456S");

        // Seconds with nanoseconds
        let interval = Interval::new(0, 0, 3 * 1_000_000_000 + 123456789);
        assert_eq!(format!("{}", interval), "PT3.123456789S");
    }

    #[test]
    fn test_interval_display_complex_intervals() {
        // Complex interval with all components
        let interval = Interval::new(
            0, 
            1, 
            (2 * 60 * 60 + 30 * 60 + 45) * 1_000_000_000 + 123 * 1_000_000
        );
        assert_eq!(format!("{}", interval), "P1DT2H30M45.123S");

        // Another complex interval
        let interval = Interval::new(
            0, 
            7, 
            (12 * 60 * 60 + 45 * 60 + 30) * 1_000_000_000 + 456789 * 1_000
        );
        assert_eq!(format!("{}", interval), "P7DT12H45M30.456789S");
    }

    #[test]
    fn test_interval_display_trailing_zeros_removed() {
        // Test that trailing zeros are removed from fractional seconds
        let interval = Interval::from_nanoseconds(100000000); // 0.1 seconds
        assert_eq!(format!("{}", interval), "PT0.1S");

        let interval = Interval::from_nanoseconds(120000000); // 0.12 seconds
        assert_eq!(format!("{}", interval), "PT0.12S");

        let interval = Interval::from_nanoseconds(123000000); // 0.123 seconds
        assert_eq!(format!("{}", interval), "PT0.123S");

        let interval = Interval::from_nanoseconds(123400000); // 0.1234 seconds
        assert_eq!(format!("{}", interval), "PT0.1234S");

        let interval = Interval::from_nanoseconds(123450000); // 0.12345 seconds
        assert_eq!(format!("{}", interval), "PT0.12345S");

        let interval = Interval::from_nanoseconds(123456000); // 0.123456 seconds
        assert_eq!(format!("{}", interval), "PT0.123456S");

        let interval = Interval::from_nanoseconds(123456700); // 0.1234567 seconds
        assert_eq!(format!("{}", interval), "PT0.1234567S");

        let interval = Interval::from_nanoseconds(123456780); // 0.12345678 seconds
        assert_eq!(format!("{}", interval), "PT0.12345678S");

        let interval = Interval::from_nanoseconds(123456789); // 0.123456789 seconds
        assert_eq!(format!("{}", interval), "PT0.123456789S");
    }

    #[test]
    fn test_interval_display_negative_intervals() {
        // Test negative intervals
        let interval = Interval::from_seconds(-30);
        assert_eq!(format!("{}", interval), "PT-30S");

        let interval = Interval::from_minutes(-5);
        assert_eq!(format!("{}", interval), "PT-5M");

        let interval = Interval::from_hours(-2);
        assert_eq!(format!("{}", interval), "PT-2H");

        let interval = Interval::from_days(-1);
        assert_eq!(format!("{}", interval), "P-1D");
    }

    #[test]
    fn test_interval_display_large_values() {
        // Test large intervals
        let interval = Interval::from_days(1000);
        assert_eq!(format!("{}", interval), "P1000D");

        let interval = Interval::from_hours(25);
        assert_eq!(format!("{}", interval), "P1DT1H");

        let interval = Interval::from_minutes(1500); // 25 hours
        assert_eq!(format!("{}", interval), "P1DT1H");

        let interval = Interval::from_seconds(90000); // 25 hours
        assert_eq!(format!("{}", interval), "P1DT1H");
    }

    #[test]
    fn test_interval_display_edge_cases() {
        // Test edge cases with single nanosecond
        let interval = Interval::from_nanoseconds(1);
        assert_eq!(format!("{}", interval), "PT0.000000001S");

        // Test maximum nanoseconds in a second
        let interval = Interval::from_nanoseconds(999999999);
        assert_eq!(format!("{}", interval), "PT0.999999999S");

        // Test exactly 1 second
        let interval = Interval::from_nanoseconds(1000000000);
        assert_eq!(format!("{}", interval), "PT1S");

        // Test exactly 1 minute
        let interval = Interval::from_nanoseconds(60 * 1000000000);
        assert_eq!(format!("{}", interval), "PT1M");

        // Test exactly 1 hour
        let interval = Interval::from_nanoseconds(3600 * 1000000000);
        assert_eq!(format!("{}", interval), "PT1H");

        // Test exactly 1 day
        let interval = Interval::from_nanoseconds(86400 * 1000000000);
        assert_eq!(format!("{}", interval), "P1D");
    }

    #[test]
    fn test_interval_display_precision_boundaries() {
        // Test precision boundaries
        let interval = Interval::from_nanoseconds(100); // 0.0000001 seconds
        assert_eq!(format!("{}", interval), "PT0.0000001S");

        let interval = Interval::from_nanoseconds(10); // 0.00000001 seconds
        assert_eq!(format!("{}", interval), "PT0.00000001S");

        let interval = Interval::from_nanoseconds(1); // 0.000000001 seconds
        assert_eq!(format!("{}", interval), "PT0.000000001S");
    }

    #[test]
    fn test_interval_display_from_nanos() {
        // Test the from_nanos method
        let interval = Interval::from_nanos(123456789);
        assert_eq!(format!("{}", interval), "PT0.123456789S");

        let interval = Interval::from_nanos(3661000000000); // 1 hour 1 minute 1 second
        assert_eq!(format!("{}", interval), "PT1H1M1S");
    }

    #[test]
    fn test_interval_display_abs_and_negate() {
        // Test absolute value
        let interval = Interval::from_seconds(-30);
        let abs_interval = interval.abs();
        assert_eq!(format!("{}", abs_interval), "PT30S");

        // Test negation
        let interval = Interval::from_seconds(30);
        let neg_interval = interval.negate();
        assert_eq!(format!("{}", neg_interval), "PT-30S");
    }
}

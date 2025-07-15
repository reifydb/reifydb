// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// A date value representing a calendar date (year, month, day) without time information.
/// Always interpreted in UTC.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct Date {
    inner: NaiveDate,
}

impl Default for Date{
    fn default() -> Self {
        Self::new(1970, 1, 1).unwrap()
    }
}

impl Date {
    pub fn new(year: i32, month: u32, day: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, day).map(|inner| Self { inner })
    }

    pub fn from_ymd(year: i32, month: u32, day: u32) -> Result<Self, String> {
        Self::new(year, month, day).ok_or_else(|| format!("Invalid date: {}-{:02}-{:02}", year, month, day))
    }

    pub fn today() -> Self {
        Self::from_naive_date(chrono::Utc::now().date_naive())
    }

    pub fn from_naive_date(date: NaiveDate) -> Self {
        Self { inner: date }
    }

    pub fn year(&self) -> i32 {
        self.inner.year()
    }

    pub fn month(&self) -> u32 {
        self.inner.month()
    }

    pub fn day(&self) -> u32 {
        self.inner.day()
    }

    pub fn inner(&self) -> &NaiveDate {
        &self.inner
    }
}

impl Date {
    /// Convert to days since Unix epoch for storage
    pub fn to_days_since_epoch(&self) -> i32 {
        self.inner.signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32
    }

    /// Create from days since Unix epoch for storage
    pub fn from_days_since_epoch(days: i32) -> Option<Self> {
        NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(days as i64)))
            .map(|inner| Self { inner })
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner.format("%Y-%m-%d"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_display_standard_dates() {
        // Standard dates
        let date = Date::new(2024, 3, 15).unwrap();
        assert_eq!(format!("{}", date), "2024-03-15");
        
        let date = Date::new(2000, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "2000-01-01");
        
        let date = Date::new(1999, 12, 31).unwrap();
        assert_eq!(format!("{}", date), "1999-12-31");
    }
    
    #[test]
    fn test_date_display_edge_cases() {
        // Unix epoch
        let date = Date::new(1970, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "1970-01-01");
        
        // Leap year
        let date = Date::new(2024, 2, 29).unwrap();
        assert_eq!(format!("{}", date), "2024-02-29");
        
        // Single digit day/month
        let date = Date::new(2024, 1, 9).unwrap();
        assert_eq!(format!("{}", date), "2024-01-09");
        
        let date = Date::new(2024, 9, 1).unwrap();
        assert_eq!(format!("{}", date), "2024-09-01");
    }
    
    #[test]
    fn test_date_display_boundary_dates() {
        // Very early date
        let date = Date::new(1, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "0001-01-01");
        
        // Far future date
        let date = Date::new(9999, 12, 31).unwrap();
        assert_eq!(format!("{}", date), "9999-12-31");
        
        // Century boundaries
        let date = Date::new(1900, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "1900-01-01");
        
        let date = Date::new(2000, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "2000-01-01");
        
        let date = Date::new(2100, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "2100-01-01");
    }
    
    #[test]
    fn test_date_display_negative_years() {
        // Year 0 (1 BC)
        let date = Date::new(0, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "0000-01-01");
        
        // Negative years (BC)
        let date = Date::new(-1, 1, 1).unwrap();
        assert_eq!(format!("{}", date), "-0001-01-01");
        
        let date = Date::new(-100, 12, 31).unwrap();
        assert_eq!(format!("{}", date), "-0100-12-31");
    }
    
    #[test]
    fn test_date_display_default() {
        let date = Date::default();
        assert_eq!(format!("{}", date), "1970-01-01");
    }
    
    #[test]
    fn test_date_display_all_months() {
        let months = [
            (1, "01"), (2, "02"), (3, "03"), (4, "04"), (5, "05"), (6, "06"),
            (7, "07"), (8, "08"), (9, "09"), (10, "10"), (11, "11"), (12, "12"),
        ];
        
        for (month, expected) in months {
            let date = Date::new(2024, month, 15).unwrap();
            assert_eq!(format!("{}", date), format!("2024-{}-15", expected));
        }
    }
    
    #[test]
    fn test_date_display_days_in_month() {
        // Test first and last days of various months
        let test_cases = [
            (2024, 1, 1, "2024-01-01"),
            (2024, 1, 31, "2024-01-31"),
            (2024, 2, 1, "2024-02-01"),
            (2024, 2, 29, "2024-02-29"), // Leap year
            (2024, 4, 1, "2024-04-01"),
            (2024, 4, 30, "2024-04-30"),
            (2024, 12, 1, "2024-12-01"),
            (2024, 12, 31, "2024-12-31"),
        ];
        
        for (year, month, day, expected) in test_cases {
            let date = Date::new(year, month, day).unwrap();
            assert_eq!(format!("{}", date), expected);
        }
    }
}

// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafeDemote<T> {
    fn demote(self) -> Option<T>;
}

macro_rules! impl_safe_demote {
    ($($from:ty => $to:ty),* $(,)?) => {
        $(
            impl SafeDemote<$to> for $from {
                fn demote(self) -> Option<$to> {
                    <$to>::try_from(self).ok()
                }
            }
        )*
    };
}

impl_safe_demote!(
    i16 => i8,

    i32 => i16,
    i32 => i8,

    i64 => i32,
    i64 => i16,
    i64 => i8,

    i128 => i64,
    i128 => i32,
    i128 => i16,
    i128 => i8,

    u16 => u8,

    u32 => u16,
    u32 => u8,

    u64 => u32,
    u64 => u16,
    u64 => u8,

    u128 => u64,
    u128 => u32,
    u128 => u16,
    u128 => u8,
);

#[cfg(test)]
mod tests {
    use super::SafeDemote;

    #[test]
    fn test_demote_i16_to_i8() {
        let val: i16 = 1;
        let demoted: Option<i8> = val.demote();
        assert_eq!(demoted, Some(val as i8));
    }

    #[test]
    fn test_demote_i32_to_i16() {
        let val: i32 = 1;
        let demoted: Option<i16> = val.demote();
        assert_eq!(demoted, Some(val as i16));
    }

    #[test]
    fn test_demote_i32_to_i8() {
        let val: i32 = 1;
        let demoted: Option<i8> = val.demote();
        assert_eq!(demoted, Some(val as i8));
    }

    #[test]
    fn test_demote_i64_to_i32() {
        let val: i64 = 1;
        let demoted: Option<i32> = val.demote();
        assert_eq!(demoted, Some(val as i32));
    }

    #[test]
    fn test_demote_i64_to_i16() {
        let val: i64 = 1;
        let demoted: Option<i16> = val.demote();
        assert_eq!(demoted, Some(val as i16));
    }

    #[test]
    fn test_demote_i64_to_i8() {
        let val: i64 = 1;
        let demoted: Option<i8> = val.demote();
        assert_eq!(demoted, Some(val as i8));
    }

    #[test]
    fn test_demote_i128_to_i64() {
        let val: i128 = 1;
        let demoted: Option<i64> = val.demote();
        assert_eq!(demoted, Some(val as i64));
    }

    #[test]
    fn test_demote_i128_to_i32() {
        let val: i128 = 1;
        let demoted: Option<i32> = val.demote();
        assert_eq!(demoted, Some(val as i32));
    }

    #[test]
    fn test_demote_i128_to_i16() {
        let val: i128 = 1;
        let demoted: Option<i16> = val.demote();
        assert_eq!(demoted, Some(val as i16));
    }

    #[test]
    fn test_demote_i128_to_i8() {
        let val: i128 = 1;
        let demoted: Option<i8> = val.demote();
        assert_eq!(demoted, Some(val as i8));
    }

    #[test]
    fn test_demote_u16_to_u8() {
        let val: u16 = 1;
        let demoted: Option<u8> = val.demote();
        assert_eq!(demoted, Some(val as u8));
    }

    #[test]
    fn test_demote_u32_to_u16() {
        let val: u32 = 1;
        let demoted: Option<u16> = val.demote();
        assert_eq!(demoted, Some(val as u16));
    }

    #[test]
    fn test_demote_u32_to_u8() {
        let val: u32 = 1;
        let demoted: Option<u8> = val.demote();
        assert_eq!(demoted, Some(val as u8));
    }

    #[test]
    fn test_demote_u64_to_u32() {
        let val: u64 = 1;
        let demoted: Option<u32> = val.demote();
        assert_eq!(demoted, Some(val as u32));
    }

    #[test]
    fn test_demote_u64_to_u16() {
        let val: u64 = 1;
        let demoted: Option<u16> = val.demote();
        assert_eq!(demoted, Some(val as u16));
    }

    #[test]
    fn test_demote_u64_to_u8() {
        let val: u64 = 1;
        let demoted: Option<u8> = val.demote();
        assert_eq!(demoted, Some(val as u8));
    }

    #[test]
    fn test_demote_u128_to_u64() {
        let val: u128 = 1;
        let demoted: Option<u64> = val.demote();
        assert_eq!(demoted, Some(val as u64));
    }

    #[test]
    fn test_demote_u128_to_u32() {
        let val: u128 = 1;
        let demoted: Option<u32> = val.demote();
        assert_eq!(demoted, Some(val as u32));
    }

    #[test]
    fn test_demote_u128_to_u16() {
        let val: u128 = 1;
        let demoted: Option<u16> = val.demote();
        assert_eq!(demoted, Some(val as u16));
    }

    #[test]
    fn test_demote_u128_to_u8() {
        let val: u128 = 1;
        let demoted: Option<u8> = val.demote();
        assert_eq!(demoted, Some(val as u8));
    }
}

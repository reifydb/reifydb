// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later.

pub trait SafePromote<T> {
    fn promote(self) -> Option<T>;
}

macro_rules! impl_safe_promote {
    ($($from:ty => $to:ty),* $(,)?) => {
        $(
            impl SafePromote<$to> for $from {
                fn promote(self) -> Option<$to> {
				    <$to>::try_from(self).ok()
                }
            }
        )*
    };
}

impl_safe_promote!(
    i8 => i16,
    i8 => i32,
    i8 => i64,
    i8 => i128,

    i16 => i32,
    i16 => i64,
    i16 => i128,

    i32 => i64,
    i32 => i128,

    i64 => i128,

    u8 => u16,
    u8 => u32,
    u8 => u64,
    u8 => u128,

    u16 => u32,
    u16 => u64,
    u16 => u128,

    u32 => u64,
    u32 => u128,

    u64 => u128,
);


#[cfg(test)]
mod tests {
    use super::SafePromote;

    #[test]
    fn test_promote_i8_to_i16() {
        let val: i8 = 1;
        let promoted: Option<i16> = val.promote();
        assert_eq!(promoted, Some(val as i16));
    }

    #[test]
    fn test_promote_i8_to_i32() {
        let val: i8 = 1;
        let promoted: Option<i32> = val.promote();
        assert_eq!(promoted, Some(val as i32));
    }

    #[test]
    fn test_promote_i8_to_i64() {
        let val: i8 = 1;
        let promoted: Option<i64> = val.promote();
        assert_eq!(promoted, Some(val as i64));
    }

    #[test]
    fn test_promote_i8_to_i128() {
        let val: i8 = 1;
        let promoted: Option<i128> = val.promote();
        assert_eq!(promoted, Some(val as i128));
    }

    #[test]
    fn test_promote_i16_to_i32() {
        let val: i16 = 1;
        let promoted: Option<i32> = val.promote();
        assert_eq!(promoted, Some(val as i32));
    }

    #[test]
    fn test_promote_i16_to_i64() {
        let val: i16 = 1;
        let promoted: Option<i64> = val.promote();
        assert_eq!(promoted, Some(val as i64));
    }

    #[test]
    fn test_promote_i16_to_i128() {
        let val: i16 = 1;
        let promoted: Option<i128> = val.promote();
        assert_eq!(promoted, Some(val as i128));
    }

    #[test]
    fn test_promote_i32_to_i64() {
        let val: i32 = 1;
        let promoted: Option<i64> = val.promote();
        assert_eq!(promoted, Some(val as i64));
    }

    #[test]
    fn test_promote_i32_to_i128() {
        let val: i32 = 1;
        let promoted: Option<i128> = val.promote();
        assert_eq!(promoted, Some(val as i128));
    }

    #[test]
    fn test_promote_i64_to_i128() {
        let val: i64 = 1;
        let promoted: Option<i128> = val.promote();
        assert_eq!(promoted, Some(val as i128));
    }

    #[test]
    fn test_promote_u8_to_u16() {
        let val: u8 = 1;
        let promoted: Option<u16> = val.promote();
        assert_eq!(promoted, Some(val as u16));
    }

    #[test]
    fn test_promote_u8_to_u32() {
        let val: u8 = 1;
        let promoted: Option<u32> = val.promote();
        assert_eq!(promoted, Some(val as u32));
    }

    #[test]
    fn test_promote_u8_to_u64() {
        let val: u8 = 1;
        let promoted: Option<u64> = val.promote();
        assert_eq!(promoted, Some(val as u64));
    }

    #[test]
    fn test_promote_u8_to_u128() {
        let val: u8 = 1;
        let promoted: Option<u128> = val.promote();
        assert_eq!(promoted, Some(val as u128));
    }

    #[test]
    fn test_promote_u16_to_u32() {
        let val: u16 = 1;
        let promoted: Option<u32> = val.promote();
        assert_eq!(promoted, Some(val as u32));
    }

    #[test]
    fn test_promote_u16_to_u64() {
        let val: u16 = 1;
        let promoted: Option<u64> = val.promote();
        assert_eq!(promoted, Some(val as u64));
    }

    #[test]
    fn test_promote_u16_to_u128() {
        let val: u16 = 1;
        let promoted: Option<u128> = val.promote();
        assert_eq!(promoted, Some(val as u128));
    }

    #[test]
    fn test_promote_u32_to_u64() {
        let val: u32 = 1;
        let promoted: Option<u64> = val.promote();
        assert_eq!(promoted, Some(val as u64));
    }

    #[test]
    fn test_promote_u32_to_u128() {
        let val: u32 = 1;
        let promoted: Option<u128> = val.promote();
        assert_eq!(promoted, Some(val as u128));
    }

    #[test]
    fn test_promote_u64_to_u128() {
        let val: u64 = 1;
        let promoted: Option<u128> = val.promote();
        assert_eq!(promoted, Some(val as u128));
    }
}
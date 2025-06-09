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
    i16 => i32,
    i32 => i64,
    i64 => i128,

    u8 => u16,
    u16 => u32,
    u32 => u64,
    u64 => u128,
);

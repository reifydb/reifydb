// Copyright (c) nyanbot.com 2025.
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
    i64 => i32,
    i128 => i64,
    u16 => u8,
    u32 => u16,
    u64 => u32,
    u128 => u64,
);

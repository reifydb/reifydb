//! C types used inside crate
#![allow(non_camel_case_types)]

//Reference: https://github.com/rust-lang/rust/blob/999967a57dce987bbad353d152f03c3ef67d41f2/library/core/src/ffi/primitives.rs#L176
#[cfg(any(target_arch = "avr", target_arch = "msp430"))]
mod ints {
    ///C type `int`
    pub type c_int = i16;
    ///C type `unsigned int`
    pub type c_uint = u16;
}

#[cfg(not(any(target_arch = "avr", target_arch = "msp430")))]
mod ints {
    ///C type `int`
    pub type c_int = i32;
    ///C type `unsigned int`
    pub type c_uint = u32;
}

#[cfg(any(
    all(target_pointer_width = "64", not(windows)),
    //Reference: https://github.com/rust-lang/rust/blob/999967a57dce987bbad353d152f03c3ef67d41f2/library/core/src/ffi/primitives.rs#L139
    all(target_arch = "wasm32", target_os = "linux")
))]
mod longs {
    ///C type `unsigned long`
    pub type c_ulong = u64;
}

#[cfg(not(any(
    all(target_pointer_width = "64", not(windows)),
    //Reference: https://github.com/rust-lang/rust/blob/999967a57dce987bbad353d152f03c3ef67d41f2/library/core/src/ffi/primitives.rs#L139
    all(target_arch = "wasm32", target_os = "linux")
)))]
mod longs {
    ///C type `unsigned long`
    pub type c_ulong = u32;
}

pub use ints::*;
pub use longs::*;

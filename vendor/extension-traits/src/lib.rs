#![cfg_attr(feature = "better-docs",
    cfg_attr(all(), doc = include_str!("../README.md")),
)]
#![no_std]
#![forbid(unsafe_code)]

#[doc(inline)]
pub use ::ext_trait::extension;

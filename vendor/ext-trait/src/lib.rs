#![cfg_attr(feature = "better-docs",
    cfg_attr(all(), doc = include_str!("../README.md")),
)]
#![no_std]
#![forbid(unsafe_code)]

/// See [the main docs][crate] for more info.
///
/// ## Advanced features
///
/// ### Extending the trait's privacy
///
/// This can be achieved by prepending a `pub` annotation before the
/// `trait Name` argument:
///
/// ```rust ,no_run
/// #[macro_use]
/// extern crate ext_trait;
///
/// mod lib {
///     //          vvv
///     #[extension(pub trait NoOp)]
///     impl<T> T {
///         fn no_op(self) -> Self { self }
///     }
/// }
///
/// fn main ()
/// {
///     use lib::NoOp;
///     let x = 42.no_op().no_op().no_op().no_op().no_op().no_op();
/// }
/// ```

pub use ::ext_trait_proc_macros::extension;

// macro internals
#[doc(hidden)] /** Not part of the public API */ pub
mod __ {
    pub use ::core;
}

#[cfg_attr(feature = "ui-tests",
    cfg_attr(all(), doc = include_str!("compile_fail_tests.md")),
)]
mod _compile_fail_tests {}

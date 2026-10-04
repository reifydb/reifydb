//! # An ergonomic for loop for const contexts
//!
//! [![GitHub](https://img.shields.io/badge/GitHub-black?logo=github)](https://github.com/JENebel/const_for)
//! [![crates.io](https://img.shields.io/crates/v/const_for?logo=rust&logoColor=b7410e)](http://crates.io/crates/const_for)
//! [![Docs](https://img.shields.io/docsrs/const_for/latest?logo=Docs.rs)](https://docs.rs/const_for/latest)
//!
//! Regular for loops are not allowed in const contexts, because it relies on iterators, which are not available in const.\
//! This is rather annoying when writing const functions, as you need to write custom for loops using 'loop' or 'while'.
//!
//! This crate provides an ergonomic macro implementation of a for loop over a range, that is usable in const and `no_std` contexts.\
//! The aim is to imitate a regular for loop as closely as possible. It handles break and continue correctly, and the variable is immutable in the body.\
//! To make the for loop as versatile as possible, it comes with macro variants to handle .rev() and step_by(x), which imitates the respective function calls.
//! This is necessary, as normally they depend on non-const iterators. But they can be used here with identical syntax.
//!
//! The main restriction is that the macro only supports standard, exclusive, ranges, eg. 0..10 and -5..5, but not ..5 or 0..=10.
//!
//! ```
//! # use const_for::*;
//! let mut a = 0;
//! const_for!(i in 0..5 => {
//!     a += i
//! });
//! assert!(a == 10)
//! ```
//!
//! This is equivalent to the following regular for loop, except it is usable in const context.
//!
//! ```
//! # use const_for::*;
//! let mut a = 0;
//! for i in 0..5 {
//!     a += i
//! }
//! assert!(a == 10)
//! ```
//!
//! ## Custom step size
//!
//! A custom step size can be set:
//!
//! ```
//! # use const_for::*;
//! let mut v = Vec::new();
//! const_for!(i in (0..5).step_by(2) => {
//!     v.push(i)
//! });
//! assert!(v == vec![0, 2, 4])
//! ```
//!
//! The loop behaves as if the function was called on the range, but it is implemented by a macro.\
//! It is equivalent to the following non-const loop:
//!
//! ```
//! # use const_for::*;
//! let mut v = Vec::new();
//! for i in (0..5).step_by(2) {
//!     v.push(i)
//! }
//! assert!(v == vec![0, 2, 4])
//! ```
//!
//! ## Reversed
//!
//! Iteration can be reversed:
//!
//! ```
//! # use const_for::*;
//! let mut v = Vec::new();
//! const_for!(i in (0..5).rev() => {
//!     v.push(i)
//! });
//! assert!(v == vec![4, 3, 2, 1, 0])
//! ```
//!
//! The loop behaves as if the function was called on the range, but it is implemented by a macro.\
//! It is equivalent to the following non-const loop:
//!
//! ```
//! # use const_for::*;
//! let mut v = Vec::new();
//! for i in (0..5).rev() {
//!     v.push(i)
//! }
//! assert!(v == vec![4, 3, 2, 1, 0])
//! ```
//!
//! ## Reversed and custom step size
//!
//! It is possible to combine rev and step_by, but each can only be appended once. So the following two examples are the only legal combinations.
//!
//! ```
//! # use const_for::*;
//! // Reverse, then change step size
//! let mut v = Vec::new();
//! const_for!(i in (0..10).rev().step_by(4) => {
//!     v.push(i)
//! });
//! assert!(v == vec![9, 5, 1]);
//!
//! // Change step size, then reverse
//! let mut v = Vec::new();
//! const_for!(i in (0..10).step_by(4).rev() => {
//!     v.push(i)
//! });
//! assert!(v == vec![8, 4, 0])
//! ```
//!
//! ## Notes
//!
//! You can use mutable and wildcard variables as the loop variable, and they act as expected.
//!
//! ```
//! // Mutable variable
//! # use const_for::*;
//! let mut v = Vec::new();
//! const_for!(mut i in (0..4) => {
//!     i *= 2;
//!     v.push(i)
//! });
//! assert!(v == vec![0, 2, 4, 6]);
//!
//! // Wildcard variable
//! let mut a = 0;
//! const_for!(_ in 0..5 =>
//!    a += 1
//! );
//! assert!(a == 5)
//! ```
//!
//! The body of the loop can be any statement. This means that the following is legal, even though it is not in a regular for loop.
//!
//! ```
//! # use const_for::*;
//! let mut a = 0;
//! const_for!(_ in 0..5 => a += 1);
//!
//! unsafe fn unsafe_function() {}
//! const_for!(_ in 0..5 => unsafe {
//!    unsafe_function()
//! });
//! ```
//!
//! If adding the step to the current value overflows the integer type, the behavior is unspecified: it panics in debug builds and wraps in release builds.
//!
//! ### Real world example
//!
//! Here is an example of how this crate helped make some actual code much nicer and readable.
//!
//! The code was taken (and edited a bit for clarity) from the [Cadabra](https://github.com/JENebel/Cadabra/) chess engine.
//!
//! Before:
//!
//! ```
//! const fn gen_white_pawn_attacks() -> [u64; 64] {
//!     let mut masks = [0; 64];
//!     
//!     let mut rank: u8 = 0;
//!     while rank < 8 {
//!         let mut file: u8 = 0;
//!         while file < 8 {
//!             let index = (rank*8+file) as usize;
//!             if file != 7 { masks[index] |= (1 << index) >> 7 as u64 }
//!             if file != 0 { masks[index] |= (1 << index) >> 9 as u64 }
//!
//!             file += 1;
//!         }
//!         rank += 1;
//!     }
//!
//!     masks
//! }
//! ```
//!
//! After:
//!
//! ```
//! # use const_for::*;
//! const fn gen_white_pawn_attacks() -> [u64; 64] {
//!     let mut masks = [0; 64];
//!     
//!     const_for!(rank in 0..8 => {
//!         const_for!(file in 0..8 => {
//!             let index = (rank*8+file) as usize;
//!             if file != 7 { masks[index] |= (1 << index) >> 7 as u64 }
//!             if file != 0 { masks[index] |= (1 << index) >> 9 as u64 }
//!         })
//!     });
//!
//!     masks
//! }
//! ```

#![no_std]

mod const_for;

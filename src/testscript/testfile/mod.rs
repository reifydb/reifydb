// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/calder/rust-goldenfile (MIT License).
// Original MIT License Copyright (c) calder 2024.

//! Testfile tests generate one or more output files as they run. If any files
//! differ from their checked-in "golden" version, the test fails. This ensures
//! that behavioral changes are intentional, explicit, and version controlled.
//!
//! # Example
//!
//! ```rust
//! use std::io::Write;
//! use reifydb::testscript::testfile::Mint;
//!
//! let mut mint = Mint::new("src/testscript/testfile/fixture");
//! let mut file1 = mint.new_testfile("file1.txt").unwrap();
//! let mut file2 = mint.new_testfile("file2.txt").unwrap();
//!
//! writeln!(file1, "Hello world!").unwrap();
//! writeln!(file2, "Foo bar!").unwrap();
//! ```
//!
//! When the `Mint` goes out of scope, it compares the contents of each file
//! to its checked-in golden version and fails the test if they differ. To
//! update the checked-in versions, run:
//! ```sh
//! UPDATE_TESTFILES=1 cargo test
//! ```

pub mod differs;
pub mod mint;

pub use mint::*;

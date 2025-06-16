// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use reifydb_diagnostic::{Line, Offset, Span};

pub mod engine;
pub mod network;
pub mod tempdir;
pub mod testscript;
pub mod transaction;
pub mod util;

pub fn make_test_span() -> Span {
    Span { offset: Offset(0), line: Line(1), fragment: "test_span".to_string() }
}

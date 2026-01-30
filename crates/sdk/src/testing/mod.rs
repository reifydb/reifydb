// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Testing utilities for FFI operators
//!
//! This module provides a comprehensive test harness for testing FFI operators
//! without the complexity of the FFI boundary. It includes test contexts,
//! state stores, data builders, and assertion helpers.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_flow_operator_sdk::testing::*;
//!
//! #[test]
//! fn test_my_operator() {
//! 	let mut harness = OperatorTestHarness::<MyOperator>::builder()
//! 		.with_config([("key", Value::Utf8("value"))])
//! 		.build()?;
//!
//! 	let input = TestChangeBuilder::new()
//! 		.insert_row(RowNumber(1), vec![Value::Int8(42i64)])
//! 		.build();
//!
//! 	let output = harness.apply(input)?;
//!
//! 	assert_eq!(output.diffs.len(), 1);
//! 	harness.assert_state("my_key", Value::Int8(42i64));
//! }
//! ```

pub mod assertions;
pub mod builders;
pub mod callbacks;
pub mod context;
pub mod harness;
pub mod helpers;
pub mod state;
pub mod stateful;

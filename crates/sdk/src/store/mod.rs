// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Store access for FFI operators
//!
//! Provides read-only access to the underlying store,
//! allowing operators to query data beyond their own state.

mod ffi;

use std::ops::Bound;

use ffi::{raw_store_contains_key, raw_store_get, raw_store_prefix, raw_store_range};
use reifydb_core::value::encoded::{EncodedKey, EncodedValues};
use tracing::{instrument, Span};

use crate::{OperatorContext, error::Result};

/// Store accessor providing read-only access to the underlying store
pub struct Store<'a> {
	ctx: &'a mut OperatorContext,
}

impl<'a> Store<'a> {
	pub(crate) fn new(ctx: &'a mut OperatorContext) -> Self {
		Self {
			ctx,
		}
	}

	#[instrument(name = "flow::operator::store::get", level = "trace", skip(self), fields(
		key_len = key.as_bytes().len(),
		found
	))]
	pub fn get(&self, key: &EncodedKey) -> Result<Option<EncodedValues>> {
		let result = raw_store_get(self.ctx, key)?;
		Span::current().record("found", result.is_some());
		Ok(result)
	}

	#[instrument(name = "flow::operator::store::contains", level = "trace", skip(self), fields(
		key_len = key.as_bytes().len()
	))]
	pub fn contains(&self, key: &EncodedKey) -> Result<bool> {
		raw_store_contains_key(self.ctx, key)
	}

	#[instrument(name = "flow::operator::store::prefix", level = "trace", skip(self), fields(
		prefix_len = prefix.as_bytes().len(),
		result_count
	))]
	pub fn prefix(&self, prefix: &EncodedKey) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		let results = raw_store_prefix(self.ctx, prefix)?;
		Span::current().record("result_count", results.len());
		Ok(results)
	}

	#[instrument(
		name = "flow::operator::store::range",
		level = "trace",
		skip(self, start, end),
		fields(result_count)
	)]
	pub fn range(
		&self,
		start: Bound<&EncodedKey>,
		end: Bound<&EncodedKey>,
	) -> Result<Vec<(EncodedKey, EncodedValues)>> {
		let results = raw_store_range(self.ctx, start, end)?;
		Span::current().record("result_count", results.len());
		Ok(results)
	}
}

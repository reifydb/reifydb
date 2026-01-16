// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::auth::Identity;
use reifydb_engine::{
	bulk_insert::builder::{BulkInsertBuilder, Trusted, Validated},
	engine::StandardEngine,
};

pub struct CommandSession {
	pub(crate) engine: StandardEngine,
	pub(crate) identity: Identity,
}

impl CommandSession {
	/// Start a bulk insert operation with full validation.
	///
	/// This provides a fluent API for fast bulk inserts that bypasses RQL parsing.
	/// All inserts within a single builder execute in one transaction.
	/// Uses this session's identity for the operation.
	///
	/// # Example
	///
	/// ```ignore
	/// use reifydb_type::params;
	///
	/// session.bulk_insert()
	///     .table("users")
	///         .row(params!{ id: 1, name: "Alice" })
	///         .row(params!{ id: 2, name: "Bob" })
	///         .done()
	///     .execute()?;
	/// ```
	pub fn bulk_insert(&self) -> BulkInsertBuilder<'_, Validated> {
		self.engine.bulk_insert(&self.identity)
	}

	/// Start a bulk insert operation with validation disabled (trusted mode).
	///
	/// Use this for pre-validated internal data where constraint validation
	/// can be skipped for maximum performance. Uses this session's identity.
	pub fn bulk_insert_trusted(&self) -> BulkInsertBuilder<'_, Trusted> {
		self.engine.bulk_insert_trusted(&self.identity)
	}
}

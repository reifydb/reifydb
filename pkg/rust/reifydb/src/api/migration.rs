// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

/// A named migration script to be applied during database startup.
///
/// Migrations are stored in the database and executed in name order.
/// Each migration has a body (the forward script) and an optional rollback body.
///
/// # Example
/// ```ignore
/// use reifydb::Migration;
///
/// let migrations = vec![
///     Migration::new("001_create_users", "CREATE TABLE app.users { id: Int4, name: Utf8 };"),
///     Migration::with_rollback(
///         "002_add_email",
///         "ALTER TABLE app.users ADD COLUMN email Utf8;",
///         "ALTER TABLE app.users DROP COLUMN email;",
///     ),
/// ];
/// ```
#[derive(Debug, Clone)]
pub struct Migration {
	pub name: String,
	pub body: String,
	pub rollback_body: Option<String>,
}

impl Migration {
	pub fn new(name: impl Into<String>, body: impl Into<String>) -> Self {
		Self {
			name: name.into(),
			body: body.into(),
			rollback_body: None,
		}
	}

	pub fn with_rollback(
		name: impl Into<String>,
		body: impl Into<String>,
		rollback_body: impl Into<String>,
	) -> Self {
		Self {
			name: name.into(),
			body: body.into(),
			rollback_body: Some(rollback_body.into()),
		}
	}
}

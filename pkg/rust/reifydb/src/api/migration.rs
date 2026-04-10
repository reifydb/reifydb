// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// A named migration script to be applied during database startup.
///
/// Migrations are stored in the database and executed in name order.
/// Each migration has a body (one or more forward statements) and an optional rollback body.
///
/// # Example
/// ```ignore
/// use reifydb::Migration;
///
/// let migrations = vec![
///     Migration::new("001_create_users", vec![
///         "CREATE TABLE app.users { id: Int4, name: Utf8 }",
///     ]),
///     Migration::new("002_setup", vec![
///         "CREATE TABLE app.orders { id: Int4, user_id: Int4 }",
///         "CREATE TABLE app.items { id: Int4, name: Utf8 }",
///     ]),
///     Migration::with_rollback(
///         "003_add_email",
///         vec!["ALTER TABLE app.users ADD COLUMN email Utf8"],
///         vec!["ALTER TABLE app.users DROP COLUMN email"],
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
	pub fn new(name: impl Into<String>, statements: Vec<impl Into<String>>) -> Self {
		let body = statements.into_iter().map(|s| s.into()).collect::<Vec<_>>().join(";\n");
		Self {
			name: name.into(),
			body,
			rollback_body: None,
		}
	}

	pub fn with_rollback(
		name: impl Into<String>,
		statements: Vec<impl Into<String>>,
		rollback_statements: Vec<impl Into<String>>,
	) -> Self {
		let body = statements.into_iter().map(|s| s.into()).collect::<Vec<_>>().join(";\n");
		let rollback_body = rollback_statements.into_iter().map(|s| s.into()).collect::<Vec<_>>().join(";\n");
		Self {
			name: name.into(),
			body,
			rollback_body: Some(rollback_body),
		}
	}
}

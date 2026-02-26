// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt::{Display, Formatter};

use reifydb_type::{
	error::{Diagnostic, Error, IntoDiagnostic},
	fragment::Fragment,
	value::r#type::Type,
};

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogObjectKind {
	Namespace,
	Table,
	View,
	Flow,
	RingBuffer,
	Dictionary,
	Enum,
	Event,
	VirtualTable,
	Handler,
	Series,
	Tag,
	User,
	Role,
	SecurityPolicy,
	Migration,
}

impl Display for CatalogObjectKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		match self {
			CatalogObjectKind::Namespace => f.write_str("namespace"),
			CatalogObjectKind::Table => f.write_str("table"),
			CatalogObjectKind::View => f.write_str("view"),
			CatalogObjectKind::Flow => f.write_str("flow"),
			CatalogObjectKind::RingBuffer => f.write_str("ring buffer"),
			CatalogObjectKind::Dictionary => f.write_str("dictionary"),
			CatalogObjectKind::Enum => f.write_str("enum"),
			CatalogObjectKind::Event => f.write_str("event"),
			CatalogObjectKind::VirtualTable => f.write_str("virtual table"),
			CatalogObjectKind::Handler => f.write_str("handler"),
			CatalogObjectKind::Series => f.write_str("series"),
			CatalogObjectKind::Tag => f.write_str("tag"),
			CatalogObjectKind::User => f.write_str("user"),
			CatalogObjectKind::Role => f.write_str("role"),
			CatalogObjectKind::SecurityPolicy => f.write_str("security policy"),
			CatalogObjectKind::Migration => f.write_str("migration"),
		}
	}
}

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
	#[error("{kind} `{namespace}::{name}` already exists")]
	AlreadyExists {
		kind: CatalogObjectKind,
		namespace: String,
		name: String,
		fragment: Fragment,
	},

	#[error("{kind} `{namespace}::{name}` not found")]
	NotFound {
		kind: CatalogObjectKind,
		namespace: String,
		name: String,
		fragment: Fragment,
	},

	#[error("migration `{name}` has no rollback body")]
	MigrationNoRollbackBody {
		name: String,
		fragment: Fragment,
	},

	#[error("column `{column}` already exists in {kind} `{namespace}::{name}`")]
	ColumnAlreadyExists {
		kind: CatalogObjectKind,
		namespace: String,
		name: String,
		column: String,
		fragment: Fragment,
	},

	#[error(
		"column `{column}` type `{column_type}` does not match dictionary `{dictionary}` value type `{dictionary_value_type}`"
	)]
	DictionaryTypeMismatch {
		column: String,
		column_type: Type,
		dictionary: String,
		dictionary_value_type: Type,
		fragment: Fragment,
	},

	#[error("auto increment is not supported for type `{ty}`")]
	AutoIncrementInvalidType {
		column: String,
		ty: Type,
		fragment: Fragment,
	},

	#[error("policy `{policy}` already exists for column `{column}`")]
	ColumnPolicyAlreadyExists {
		policy: String,
		column: String,
	},

	#[error("{kind} `{namespace}` already has pending changes in this transaction")]
	AlreadyPendingInTransaction {
		kind: CatalogObjectKind,
		namespace: String,
		name: Option<String>,
		fragment: Fragment,
	},

	#[error("cannot update {kind} as it is marked for deletion")]
	CannotUpdateDeleted {
		kind: CatalogObjectKind,
		namespace: String,
		name: Option<String>,
		fragment: Fragment,
	},

	#[error("{kind} is already marked for deletion")]
	CannotDeleteAlreadyDeleted {
		kind: CatalogObjectKind,
		namespace: String,
		name: Option<String>,
		fragment: Fragment,
	},

	#[error("primary key must contain at least one column")]
	PrimaryKeyEmpty {
		fragment: Fragment,
	},

	#[error("column with ID {column_id} not found for primary key")]
	PrimaryKeyColumnNotFound {
		fragment: Fragment,
		column_id: u64,
	},

	#[error("subscription `{name}` already exists")]
	SubscriptionAlreadyExists {
		fragment: Fragment,
		name: String,
	},

	#[error("subscription `{name}` not found")]
	SubscriptionNotFound {
		fragment: Fragment,
		name: String,
	},

	#[error("column `{column}` not found in {kind} `{namespace}`.`{name}`")]
	ColumnNotFound {
		kind: CatalogObjectKind,
		namespace: String,
		name: String,
		column: String,
		fragment: Fragment,
	},

	#[error("cannot drop {kind} because it is in use")]
	InUse {
		kind: CatalogObjectKind,
		namespace: String,
		name: Option<String>,
		dependents: String,
		fragment: Fragment,
	},
}

impl IntoDiagnostic for CatalogError {
	fn into_diagnostic(self) -> Diagnostic {
		match self {
			CatalogError::AlreadyExists {
				kind,
				namespace,
				name,
				fragment,
			} => {
				let (code, kind_str, help) = match kind {
					CatalogObjectKind::Namespace => (
						"CA_001",
						"namespace",
						"choose a different name or drop the existing namespace first",
					),
					CatalogObjectKind::Table => (
						"CA_003",
						"table",
						"choose a different name, drop the existing table or create table in a different namespace",
					),
					CatalogObjectKind::View => (
						"CA_003",
						"view",
						"choose a different name, drop the existing view or create view in a different namespace",
					),
					CatalogObjectKind::Flow => (
						"CA_030",
						"flow",
						"choose a different name, drop the existing flow or create flow in a different namespace",
					),
					CatalogObjectKind::RingBuffer => (
						"CA_005",
						"ring buffer",
						"choose a different name, drop the existing ring buffer or create ring buffer in a different namespace",
					),
					CatalogObjectKind::Dictionary => (
						"CA_006",
						"dictionary",
						"choose a different name, drop the existing dictionary or create dictionary in a different namespace",
					),
					CatalogObjectKind::Enum => (
						"CA_003",
						"enum",
						"choose a different name or drop the existing enum first",
					),
					CatalogObjectKind::Event => (
						"CA_003",
						"event",
						"choose a different name or drop the existing event first",
					),
					CatalogObjectKind::VirtualTable => (
						"CA_022",
						"virtual table",
						"choose a different name or unregister the existing virtual table first",
					),
					CatalogObjectKind::Handler => (
						"CA_003",
						"handler",
						"choose a different name or drop the existing handler first",
					),
					CatalogObjectKind::Series => (
						"CA_003",
						"series",
						"choose a different name or drop the existing series first",
					),
					CatalogObjectKind::Tag => (
						"CA_003",
						"tag",
						"choose a different name or drop the existing tag first",
					),
					CatalogObjectKind::User => (
						"CA_040",
						"user",
						"choose a different name or drop the existing user first",
					),
					CatalogObjectKind::Role => (
						"CA_041",
						"role",
						"choose a different name or drop the existing role first",
					),
					CatalogObjectKind::SecurityPolicy => (
						"CA_042",
						"security policy",
						"choose a different name or drop the existing security policy first",
					),
					CatalogObjectKind::Migration => {
						("CA_046", "migration", "choose a different name for the migration")
					}
				};
				let message = if matches!(
					kind,
					CatalogObjectKind::Namespace | CatalogObjectKind::Migration
				) {
					format!("{} `{}` already exists", kind_str, name)
				} else {
					format!("{} `{}::{}` already exists", kind_str, namespace, name)
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					fragment,
					label: Some(format!("duplicate {} definition", kind_str)),
					help: Some(help.to_string()),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CatalogError::NotFound {
				kind,
				namespace,
				name,
				fragment,
			} => {
				let (code, kind_str, help) = match kind {
					CatalogObjectKind::Namespace => (
						"CA_002",
						"namespace",
						"make sure the namespace exists before using it or create it first".to_string(),
					),
					CatalogObjectKind::Table => (
						"CA_004",
						"table",
						"ensure the table exists or create it first using `CREATE TABLE`".to_string(),
					),
					CatalogObjectKind::View => (
						"CA_004",
						"view",
						"ensure the view exists or create it first using `CREATE VIEW`".to_string(),
					),
					CatalogObjectKind::Flow => (
						"CA_031",
						"flow",
						"ensure the flow exists or create it first using `CREATE FLOW`".to_string(),
					),
					CatalogObjectKind::RingBuffer => (
						"CA_006",
						"ring buffer",
						"ensure the ring buffer exists or create it first using `CREATE RING BUFFER`".to_string(),
					),
					CatalogObjectKind::Dictionary => (
						"CA_007",
						"dictionary",
						"ensure the dictionary exists or create it first using `CREATE DICTIONARY`".to_string(),
					),
					CatalogObjectKind::Enum => (
						"CA_002",
						"type",
						format!("create the enum first with `CREATE ENUM {}::{} {{ ... }}`", namespace, name),
					),
					CatalogObjectKind::Event => (
						"CA_002",
						"event",
						format!("create the event first with `CREATE EVENT {}::{} {{ ... }}`", namespace, name),
					),
					CatalogObjectKind::VirtualTable => (
						"CA_023",
						"virtual table",
						"ensure the virtual table is registered before using it".to_string(),
					),
					CatalogObjectKind::Handler => (
						"CA_004",
						"handler",
						"ensure the handler exists or create it first using `CREATE HANDLER`".to_string(),
					),
					CatalogObjectKind::Series => (
						"CA_004",
						"series",
						"ensure the series exists or create it first using `CREATE SERIES`".to_string(),
					),
					CatalogObjectKind::Tag => (
						"CA_002",
						"tag",
						format!("create the tag first with `CREATE TAG {}.{} {{ ... }}`", namespace, name),
					),
					CatalogObjectKind::User => (
						"CA_043",
						"user",
						"ensure the user exists or create it first using `CREATE USER`".to_string(),
					),
					CatalogObjectKind::Role => (
						"CA_044",
						"role",
						"ensure the role exists or create it first using `CREATE ROLE`".to_string(),
					),
					CatalogObjectKind::SecurityPolicy => (
						"CA_045",
						"security policy",
						"ensure the security policy exists or create it first".to_string(),
					),
					CatalogObjectKind::Migration => (
						"CA_047",
						"migration",
						"ensure the migration exists or create it first using `CREATE MIGRATION`".to_string(),
					),
				};
				let message = match kind {
					CatalogObjectKind::Namespace => {
						format!("{} `{}` not found", kind_str, namespace)
					}
					CatalogObjectKind::Migration => format!("{} `{}` not found", kind_str, name),
					_ => format!("{} `{}::{}` not found", kind_str, namespace, name),
				};
				let label_str = match kind {
					CatalogObjectKind::Namespace => "unknown namespace reference".to_string(),
					CatalogObjectKind::Enum => "unknown type".to_string(),
					CatalogObjectKind::Event => "unknown event reference".to_string(),
					_ => format!("unknown {} reference", kind_str),
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					fragment,
					label: Some(label_str),
					help: Some(help),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CatalogError::MigrationNoRollbackBody {
				name,
				fragment,
			} => Diagnostic {
				code: "CA_048".to_string(),
				statement: None,
				message: format!("migration `{}` has no rollback body", name),
				fragment,
				label: Some("no rollback body defined".to_string()),
				help: Some("define a ROLLBACK clause when creating the migration".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::ColumnAlreadyExists {
				kind,
				namespace,
				name,
				column,
				fragment,
			} => {
				let kind_str = match kind {
					CatalogObjectKind::Table => "table",
					CatalogObjectKind::View => "view",
					_ => "object",
				};
				Diagnostic {
					code: "CA_005".to_string(),
					statement: None,
					message: format!(
						"column `{}` already exists in {} `{}::{}`",
						column, kind_str, namespace, name
					),
					fragment,
					label: Some("duplicate column definition".to_string()),
					help: Some("choose a different column name or drop the existing one first"
						.to_string()),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CatalogError::DictionaryTypeMismatch {
				column,
				column_type,
				dictionary,
				dictionary_value_type,
				fragment,
			} => Diagnostic {
				code: "CA_008".to_string(),
				statement: None,
				message: format!(
					"column `{}` type `{}` does not match dictionary `{}` value type `{}`",
					column, column_type, dictionary, dictionary_value_type
				),
				fragment,
				label: Some("type mismatch".to_string()),
				help: Some(format!(
					"change the column type to `{}` to match the dictionary value type",
					dictionary_value_type
				)),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::AutoIncrementInvalidType {
				column,
				ty,
				fragment,
			} => Diagnostic {
				code: "CA_006".to_string(),
				statement: None,
				message: format!("auto increment is not supported for type `{}`", ty),
				fragment,
				label: Some("invalid auto increment usage".to_string()),
				help: Some(format!(
					"auto increment is only supported for integer types (int1-16, uint1-16), column `{}` has type `{}`",
					column, ty
				)),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::ColumnPolicyAlreadyExists {
				policy,
				column,
			} => Diagnostic {
				code: "CA_008".to_string(),
				statement: None,
				message: format!("policy `{:?}` already exists for column `{}`", policy, column),
				fragment: Fragment::None,
				label: Some("duplicate column policy".to_string()),
				help: Some("remove the existing policy first".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::AlreadyPendingInTransaction {
				kind,
				namespace,
				name,
				fragment,
			} => {
				let (code, message, label_str) = match kind {
					CatalogObjectKind::Namespace => (
						"CA_011",
						format!(
							"namespace `{}` already has pending changes in this transaction",
							namespace
						),
						"duplicate namespace modification in transaction",
					),
					CatalogObjectKind::Table => (
						"CA_012",
						format!(
							"table `{}::{}` already has pending changes in this transaction",
							namespace,
							name.as_deref().unwrap_or("")
						),
						"duplicate table modification in transaction",
					),
					CatalogObjectKind::View => (
						"CA_013",
						format!(
							"view `{}::{}` already has pending changes in this transaction",
							namespace,
							name.as_deref().unwrap_or("")
						),
						"duplicate view modification in transaction",
					),
					_ => (
						"CA_011",
						format!("{} already has pending changes in this transaction", kind),
						"duplicate modification in transaction",
					),
				};
				let kind_str = match kind {
					CatalogObjectKind::Namespace => "namespace",
					CatalogObjectKind::Table => "table",
					CatalogObjectKind::View => "view",
					_ => "object",
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					fragment,
					label: Some(label_str.to_string()),
					help: Some(format!(
						"a {} can only be created, updated, or deleted once per transaction",
						kind_str
					)),
					column: None,
					notes: vec![
						"This usually indicates a programming error in transaction management"
							.to_string(),
						"Consider reviewing the transaction logic for duplicate operations"
							.to_string(),
					],
					cause: None,
					operator_chain: None,
				}
			}

			CatalogError::CannotUpdateDeleted {
				kind,
				namespace,
				name,
				fragment,
			} => {
				let (code, message, kind_str) = match kind {
					CatalogObjectKind::Namespace => (
						"CA_014",
						format!(
							"cannot update namespace `{}` as it is marked for deletion in this transaction",
							namespace
						),
						"namespace",
					),
					CatalogObjectKind::Table => (
						"CA_015",
						format!(
							"cannot update table `{}::{}` as it is marked for deletion in this transaction",
							namespace,
							name.as_deref().unwrap_or("")
						),
						"table",
					),
					CatalogObjectKind::View => (
						"CA_016",
						format!(
							"cannot update view `{}::{}` as it is marked for deletion in this transaction",
							namespace,
							name.as_deref().unwrap_or("")
						),
						"view",
					),
					_ => (
						"CA_014",
						format!(
							"cannot update {} as it is marked for deletion in this transaction",
							kind
						),
						"object",
					),
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					fragment,
					label: Some(format!("attempted update on deleted {}", kind_str)),
					help: Some("remove the delete operation or skip the update".to_string()),
					column: None,
					notes: vec![format!(
						"A {} marked for deletion cannot be updated in the same transaction",
						kind_str
					)],
					cause: None,
					operator_chain: None,
				}
			}

			CatalogError::CannotDeleteAlreadyDeleted {
				kind,
				namespace,
				name,
				fragment,
			} => {
				let (code, message, kind_str) = match kind {
					CatalogObjectKind::Namespace => (
						"CA_017",
						format!(
							"namespace `{}` is already marked for deletion in this transaction",
							namespace
						),
						"namespace",
					),
					CatalogObjectKind::Table => (
						"CA_018",
						format!(
							"table `{}::{}` is already marked for deletion in this transaction",
							namespace,
							name.as_deref().unwrap_or("")
						),
						"table",
					),
					CatalogObjectKind::View => (
						"CA_019",
						format!(
							"view `{}::{}` is already marked for deletion in this transaction",
							namespace,
							name.as_deref().unwrap_or("")
						),
						"view",
					),
					_ => (
						"CA_017",
						format!("{} is already marked for deletion in this transaction", kind),
						"object",
					),
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					fragment,
					label: Some(format!("duplicate {} deletion", kind_str)),
					help: Some("remove the duplicate delete operation".to_string()),
					column: None,
					notes: vec![format!("A {} can only be deleted once per transaction", kind_str)],
					cause: None,
					operator_chain: None,
				}
			}

			CatalogError::PrimaryKeyEmpty {
				fragment,
			} => Diagnostic {
				code: "CA_020".to_string(),
				statement: None,
				message: "primary key must contain at least one column".to_string(),
				fragment,
				label: Some("empty primary key definition".to_string()),
				help: Some("specify at least one column for the primary key".to_string()),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::PrimaryKeyColumnNotFound {
				fragment,
				column_id,
			} => Diagnostic {
				code: "CA_021".to_string(),
				statement: None,
				message: format!("column with ID {} not found for primary key", column_id),
				fragment,
				label: Some("invalid column reference in primary key".to_string()),
				help: Some(
					"ensure all columns referenced in the primary key exist in the table or view"
						.to_string(),
				),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::SubscriptionAlreadyExists {
				fragment,
				name,
			} => Diagnostic {
				code: "CA_010".to_string(),
				statement: None,
				message: format!("subscription `{}` already exists", name),
				fragment,
				label: Some("duplicate subscription definition".to_string()),
				help: Some(
					"choose a different name or close the existing subscription first".to_string()
				),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::SubscriptionNotFound {
				fragment,
				name,
			} => Diagnostic {
				code: "CA_011".to_string(),
				statement: None,
				message: format!("subscription `{}` not found", name),
				fragment,
				label: Some("unknown subscription reference".to_string()),
				help: Some(
					"ensure the subscription exists or create it first using `CREATE SUBSCRIPTION`"
						.to_string(),
				),
				column: None,
				notes: vec![],
				cause: None,
				operator_chain: None,
			},

			CatalogError::ColumnNotFound {
				kind,
				namespace,
				name,
				column,
				fragment,
			} => {
				let kind_str = match kind {
					CatalogObjectKind::Table => "table",
					CatalogObjectKind::View => "view",
					_ => "object",
				};
				Diagnostic {
					code: "CA_039".to_string(),
					statement: None,
					message: format!(
						"column `{}` not found in {} `{}`.`{}`",
						column, kind_str, namespace, name
					),
					fragment,
					label: Some("unknown column reference".to_string()),
					help: Some("ensure the column exists in the table".to_string()),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}

			CatalogError::InUse {
				kind,
				namespace,
				name,
				dependents,
				fragment,
			} => {
				let (code, label, help) = match kind {
					CatalogObjectKind::Dictionary => (
						"CA_032",
						"dictionary is in use",
						"drop or alter the dependent columns first, or use CASCADE to automatically drop all dependents",
					),
					CatalogObjectKind::Enum => (
						"CA_033",
						"enum is in use",
						"drop or alter the dependent columns first, or use CASCADE to automatically drop all dependents",
					),
					CatalogObjectKind::Event => (
						"CA_033",
						"event is in use",
						"drop or alter the dependent handlers first, or use CASCADE to automatically drop all dependents",
					),
					CatalogObjectKind::Namespace => (
						"CA_034",
						"namespace contains referenced objects",
						"drop or alter the dependent columns in other namespaces first",
					),
					CatalogObjectKind::Table => (
						"CA_035",
						"table is in use",
						"drop or alter the dependent flows first, or use CASCADE to automatically drop all dependents",
					),
					CatalogObjectKind::View => (
						"CA_036",
						"view is in use",
						"drop or alter the dependent flows first, or use CASCADE to automatically drop all dependents",
					),
					CatalogObjectKind::Flow => (
						"CA_037",
						"flow is in use",
						"drop or alter the dependent flows first, or use CASCADE to automatically drop all dependents",
					),
					CatalogObjectKind::RingBuffer => (
						"CA_038",
						"ring buffer is in use",
						"drop or alter the dependent flows first, or use CASCADE to automatically drop all dependents",
					),
					_ => (
						"CA_032",
						"object is in use",
						"drop or alter the dependents first, or use CASCADE to automatically drop all dependents",
					),
				};
				let message = if matches!(kind, CatalogObjectKind::Namespace) {
					format!(
						"cannot drop namespace '{}' because it contains objects referenced from other namespaces: {}",
						namespace, dependents
					)
				} else {
					format!(
						"cannot drop {} '{}::{}' because it is referenced by: {}",
						kind,
						namespace,
						name.as_deref().unwrap_or(""),
						dependents
					)
				};
				Diagnostic {
					code: code.to_string(),
					statement: None,
					message,
					fragment,
					label: Some(label.to_string()),
					help: Some(help.to_string()),
					column: None,
					notes: vec![],
					cause: None,
					operator_chain: None,
				}
			}
		}
	}
}

impl From<CatalogError> for Error {
	fn from(err: CatalogError) -> Self {
		Error(err.into_diagnostic())
	}
}

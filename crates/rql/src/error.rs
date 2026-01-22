// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::fmt;

use reifydb_core::error::diagnostic::{catalog::table_not_found, internal::internal};
use reifydb_type::{error, error::Error};

/// Errors related to identifier resolution
#[derive(Debug, Clone)]
pub enum IdentifierError {
	SourceNotFound(PrimitiveNotFoundError),
	ColumnNotFound {
		column: String,
	},
	AmbiguousColumn(AmbiguousColumnError),
	UnknownAlias(UnknownAliasError),
	FunctionNotFound(FunctionNotFoundError),
	SequenceNotFound {
		namespace: String,
		name: String,
	},
	IndexNotFound {
		namespace: String,
		table: String,
		name: String,
	},
}

impl fmt::Display for IdentifierError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			IdentifierError::SourceNotFound(e) => {
				write!(f, "{}", e)
			}
			IdentifierError::ColumnNotFound {
				column,
			} => {
				write!(f, "Column '{}' not found", column)
			}
			IdentifierError::AmbiguousColumn(e) => {
				write!(f, "{}", e)
			}
			IdentifierError::UnknownAlias(e) => write!(f, "{}", e),
			IdentifierError::FunctionNotFound(e) => {
				write!(f, "{}", e)
			}
			IdentifierError::SequenceNotFound {
				namespace,
				name,
			} => {
				write!(f, "Sequence '{}.{}' not found", namespace, name)
			}
			IdentifierError::IndexNotFound {
				namespace,
				table,
				name,
			} => {
				write!(f, "Index '{}' on table '{}.{}' not found", name, namespace, table)
			}
		}
	}
}

impl std::error::Error for IdentifierError {}

impl From<IdentifierError> for Error {
	fn from(err: IdentifierError) -> Self {
		match err {
			IdentifierError::SourceNotFound(ref e) => {
				// Create a proper catalog error for source not
				// found
				error!(table_not_found(e.fragment.clone(), &e.namespace, &e.name))
			}
			_ => {
				// For other errors, use internal error
				error!(internal(err.to_string()))
			}
		}
	}
}

/// Namespace not found error
#[derive(Debug, Clone)]
pub struct SchemaNotFoundError {
	pub namespace: String,
}

impl fmt::Display for SchemaNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Namespace '{}' does not exist", self.namespace)
	}
}

/// Source (table/view) not found error
#[derive(Debug, Clone)]
pub struct PrimitiveNotFoundError {
	pub namespace: String,
	pub name: String,
	pub fragment: reifydb_type::fragment::Fragment,
}

impl fmt::Display for PrimitiveNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.namespace == "public" || self.namespace.is_empty() {
			write!(f, "Table or view '{}' does not exist", self.name)
		} else {
			write!(f, "Table or view '{}.{}' does not exist", self.namespace, self.name)
		}
	}
}

impl PrimitiveNotFoundError {
	/// Create error with additional context about what type was expected
	pub fn with_expected_type(
		namespace: String,
		name: String,
		_expected: &str,
		fragment: reifydb_type::fragment::Fragment,
	) -> Self {
		// Could extend this to include expected type in the error
		Self {
			namespace,
			name,
			fragment,
		}
	}
}

/// Ambiguous column reference error
#[derive(Debug, Clone)]
pub struct AmbiguousColumnError {
	pub column: String,
	pub sources: Vec<String>,
}

impl fmt::Display for AmbiguousColumnError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Column '{}' is ambiguous, found in sources: {}", self.column, self.sources.join(", "))
	}
}

/// Unknown alias error
#[derive(Debug, Clone)]
pub struct UnknownAliasError {
	pub alias: String,
}

impl fmt::Display for UnknownAliasError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Alias '{}' is not defined in the current scope", self.alias)
	}
}

/// Function not found error
#[derive(Debug, Clone)]
pub struct FunctionNotFoundError {
	pub namespaces: Vec<String>,
	pub name: String,
}

impl fmt::Display for FunctionNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.namespaces.is_empty() {
			write!(f, "Function '{}' does not exist", self.name)
		} else {
			let qualified = format!("{}::{}", self.namespaces.join("::"), self.name);
			write!(f, "Function '{}' does not exist", qualified)
		}
	}
}

/// Check if a fragment represents an injected default namespace
pub fn is_default_namespace(fragment: &reifydb_type::fragment::Fragment) -> bool {
	matches!(fragment, reifydb_type::fragment::Fragment::Internal { .. })
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_schema_not_found_display() {
		let err = SchemaNotFoundError {
			namespace: "myschema".to_string(),
		};
		assert_eq!(err.to_string(), "Namespace 'myschema' does not exist");
	}

	#[test]
	fn test_primitive_not_found_display() {
		let err = PrimitiveNotFoundError {
			namespace: "public".to_string(),
			name: "users".to_string(),
			fragment: reifydb_type::fragment::Fragment::None,
		};
		assert_eq!(err.to_string(), "Table or view 'users' does not exist");

		let err = PrimitiveNotFoundError {
			namespace: "myschema".to_string(),
			name: "users".to_string(),
			fragment: reifydb_type::fragment::Fragment::None,
		};
		assert_eq!(err.to_string(), "Table or view 'myschema.users' does not exist");
	}

	#[test]
	fn test_ambiguous_column_display() {
		let err = AmbiguousColumnError {
			column: "id".to_string(),
			sources: vec!["users".to_string(), "profiles".to_string()],
		};
		assert_eq!(err.to_string(), "Column 'id' is ambiguous, found in sources: users, profiles");
	}

	#[test]
	fn test_unknown_alias_display() {
		let err = UnknownAliasError {
			alias: "u".to_string(),
		};
		assert_eq!(err.to_string(), "Alias 'u' is not defined in the current scope");
	}

	#[test]
	fn test_function_not_found_display() {
		let err = FunctionNotFoundError {
			namespaces: vec![],
			name: "my_func".to_string(),
		};
		assert_eq!(err.to_string(), "Function 'my_func' does not exist");

		let err = FunctionNotFoundError {
			namespaces: vec!["pg_catalog".to_string(), "string".to_string()],
			name: "substr".to_string(),
		};
		assert_eq!(err.to_string(), "Function 'pg_catalog::string::substr' does not exist");
	}
}

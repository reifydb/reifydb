// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::fmt;

use reifydb_core::interface::identifier::SourceKind;

/// Errors related to identifier resolution
#[derive(Debug, Clone)]
pub enum IdentifierError {
	SchemaNotFound(SchemaNotFoundError),
	SourceNotFound(SourceNotFoundError),
	ColumnNotFound {
		column: String,
	},
	AmbiguousColumn(AmbiguousColumnError),
	UnknownAlias(UnknownAliasError),
	FunctionNotFound(FunctionNotFoundError),
	SequenceNotFound {
		schema: String,
		name: String,
	},
	IndexNotFound {
		schema: String,
		table: String,
		name: String,
	},
}

impl fmt::Display for IdentifierError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			IdentifierError::SchemaNotFound(e) => {
				write!(f, "{}", e)
			}
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
				schema,
				name,
			} => {
				write!(
					f,
					"Sequence '{}.{}' not found",
					schema, name
				)
			}
			IdentifierError::IndexNotFound {
				schema,
				table,
				name,
			} => {
				write!(
					f,
					"Index '{}' on table '{}.{}' not found",
					name, schema, table
				)
			}
		}
	}
}

impl std::error::Error for IdentifierError {}

impl From<IdentifierError> for reifydb_core::Error {
	fn from(err: IdentifierError) -> Self {
		reifydb_core::error!(
			reifydb_core::diagnostic::internal::internal(
				err.to_string()
			)
		)
	}
}

/// Schema not found error
#[derive(Debug, Clone)]
pub struct SchemaNotFoundError {
	pub schema: String,
}

impl fmt::Display for SchemaNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "Schema '{}' does not exist", self.schema)
	}
}

/// Source (table/view) not found error
#[derive(Debug, Clone)]
pub struct SourceNotFoundError {
	pub schema: String,
	pub name: String,
}

impl fmt::Display for SourceNotFoundError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		if self.schema == "public" || self.schema.is_empty() {
			write!(
				f,
				"Table or view '{}' does not exist",
				self.name
			)
		} else {
			write!(
				f,
				"Table or view '{}.{}' does not exist",
				self.schema, self.name
			)
		}
	}
}

impl SourceNotFoundError {
	/// Create error with additional context about what type was expected
	pub fn with_expected_type(
		schema: String,
		name: String,
		_expected: SourceKind,
	) -> Self {
		// Could extend this to include expected type in the error
		Self {
			schema,
			name,
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
		write!(
			f,
			"Column '{}' is ambiguous, found in sources: {}",
			self.column,
			self.sources.join(", ")
		)
	}
}

/// Unknown alias error
#[derive(Debug, Clone)]
pub struct UnknownAliasError {
	pub alias: String,
}

impl fmt::Display for UnknownAliasError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"Alias '{}' is not defined in the current scope",
			self.alias
		)
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
			let qualified = format!(
				"{}::{}",
				self.namespaces.join("::"),
				self.name
			);
			write!(f, "Function '{}' does not exist", qualified)
		}
	}
}

/// Helper function to create a source not found error with context
pub fn source_not_found_with_hint(
	schema: String,
	name: String,
	was_default: bool,
) -> IdentifierError {
	if was_default {
		// If schema was injected as default, mention it in the error
		IdentifierError::SourceNotFound(SourceNotFoundError {
			schema: schema.clone(),
			name: name.clone(),
		})
		// Could extend to add: "(using default schema 'public')" to the
		// message
	} else {
		IdentifierError::SourceNotFound(SourceNotFoundError {
			schema,
			name,
		})
	}
}

/// Check if a fragment represents an injected default schema
pub fn is_default_schema(fragment: &reifydb_type::Fragment) -> bool {
	matches!(
		fragment,
		reifydb_type::Fragment::Owned(
			reifydb_type::OwnedFragment::Internal { .. }
		)
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_schema_not_found_display() {
		let err = SchemaNotFoundError {
			schema: "myschema".to_string(),
		};
		assert_eq!(err.to_string(), "Schema 'myschema' does not exist");
	}

	#[test]
	fn test_source_not_found_display() {
		let err = SourceNotFoundError {
			schema: "public".to_string(),
			name: "users".to_string(),
		};
		assert_eq!(
			err.to_string(),
			"Table or view 'users' does not exist"
		);

		let err = SourceNotFoundError {
			schema: "myschema".to_string(),
			name: "users".to_string(),
		};
		assert_eq!(
			err.to_string(),
			"Table or view 'myschema.users' does not exist"
		);
	}

	#[test]
	fn test_ambiguous_column_display() {
		let err = AmbiguousColumnError {
			column: "id".to_string(),
			sources: vec![
				"users".to_string(),
				"profiles".to_string(),
			],
		};
		assert_eq!(
			err.to_string(),
			"Column 'id' is ambiguous, found in sources: users, profiles"
		);
	}

	#[test]
	fn test_unknown_alias_display() {
		let err = UnknownAliasError {
			alias: "u".to_string(),
		};
		assert_eq!(
			err.to_string(),
			"Alias 'u' is not defined in the current scope"
		);
	}

	#[test]
	fn test_function_not_found_display() {
		let err = FunctionNotFoundError {
			namespaces: vec![],
			name: "my_func".to_string(),
		};
		assert_eq!(
			err.to_string(),
			"Function 'my_func' does not exist"
		);

		let err = FunctionNotFoundError {
			namespaces: vec![
				"pg_catalog".to_string(),
				"string".to_string(),
			],
			name: "substr".to_string(),
		};
		assert_eq!(
			err.to_string(),
			"Function 'pg_catalog::string::substr' does not exist"
		);
	}
}

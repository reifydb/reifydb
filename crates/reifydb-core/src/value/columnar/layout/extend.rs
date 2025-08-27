// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::ColumnsLayout;
use crate::result::error::diagnostic::query;

impl ColumnsLayout {
	pub fn extend(&self, other: &ColumnsLayout) -> crate::Result<Self> {
		let mut columns = Vec::with_capacity(
			self.columns.len() + other.columns.len(),
		);

		// Add all columns from self (existing columns)
		columns.extend(self.columns.iter().cloned());

		// Check for duplicate columns and return error if found
		for column in &other.columns {
			let column_exists =
				self.columns.iter().any(|existing| {
					existing.name == column.name
						&& existing.schema
							== column.schema && existing.source
						== column.source
				});

			if column_exists {
				return crate::err!(
					query::extend_duplicate_column(
						&column.name
					)
				);
			}

			columns.push(column.clone());
		}

		Ok(Self {
			columns,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::value::columnar::layout::ColumnLayout;

	fn create_column_layout(
		name: &str,
		schema: Option<&str>,
		source: Option<&str>,
	) -> ColumnLayout {
		ColumnLayout {
			name: name.to_string(),
			schema: schema.map(|s| s.to_string()),
			source: source.map(|s| s.to_string()),
		}
	}

	fn create_layout(columns: Vec<ColumnLayout>) -> ColumnsLayout {
		ColumnsLayout {
			columns,
		}
	}

	#[test]
	fn test_extend_empty_layouts() {
		let base = create_layout(vec![]);
		let other = create_layout(vec![]);

		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 0);
	}

	#[test]
	fn test_extend_empty_base() {
		let base = create_layout(vec![]);
		let other = create_layout(vec![
			create_column_layout("col1", None, None),
			create_column_layout("col2", None, None),
		]);

		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 2);
		assert_eq!(result.columns[0].name, "col1");
		assert_eq!(result.columns[1].name, "col2");
	}

	#[test]
	fn test_extend_empty_other() {
		let base = create_layout(vec![
			create_column_layout("col1", None, None),
			create_column_layout("col2", None, None),
		]);
		let other = create_layout(vec![]);

		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 2);
		assert_eq!(result.columns[0].name, "col1");
		assert_eq!(result.columns[1].name, "col2");
	}

	#[test]
	fn test_extend_no_duplicates() {
		let base = create_layout(vec![
			create_column_layout("col1", None, None),
			create_column_layout("col2", None, None),
		]);
		let other = create_layout(vec![
			create_column_layout("col3", None, None),
			create_column_layout("col4", None, None),
		]);

		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 4);
		assert_eq!(result.columns[0].name, "col1");
		assert_eq!(result.columns[1].name, "col2");
		assert_eq!(result.columns[2].name, "col3");
		assert_eq!(result.columns[3].name, "col4");
	}

	#[test]
	fn test_extend_duplicate_column_name() {
		let base = create_layout(vec![
			create_column_layout("col1", None, None),
			create_column_layout("col2", None, None),
		]);
		let other = create_layout(vec![
			create_column_layout("col2", None, None), /* Duplicate name */
			create_column_layout("col3", None, None),
		]);

		let result = base.extend(&other);
		assert!(result.is_err());

		let error = result.unwrap_err();
		let diagnostic = error.diagnostic();
		assert_eq!(diagnostic.code, "EXTEND_002");
		assert!(diagnostic.message.contains("col2"));
	}

	#[test]
	fn test_extend_duplicate_with_different_schema() {
		let base = create_layout(vec![create_column_layout(
			"col1",
			Some("schema1"),
			None,
		)]);
		let other = create_layout(vec![
			create_column_layout("col1", Some("schema2"), None), /* Same name, different schema */
		]);

		// Should succeed because schema is different
		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 2);
		assert_eq!(
			result.columns[0].schema,
			Some("schema1".to_string())
		);
		assert_eq!(
			result.columns[1].schema,
			Some("schema2".to_string())
		);
	}

	#[test]
	fn test_extend_duplicate_with_different_source() {
		let base = create_layout(vec![create_column_layout(
			"col1",
			None,
			Some("table1"),
		)]);
		let other = create_layout(vec![
			create_column_layout("col1", None, Some("table2")), /* Same name, different source */
		]);

		// Should succeed because source is different
		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 2);
		assert_eq!(
			result.columns[0].source,
			Some("table1".to_string())
		);
		assert_eq!(
			result.columns[1].source,
			Some("table2".to_string())
		);
	}

	#[test]
	fn test_extend_exact_duplicate() {
		let base = create_layout(vec![create_column_layout(
			"col1",
			Some("schema1"),
			Some("table1"),
		)]);
		let other = create_layout(vec![
			create_column_layout(
				"col1",
				Some("schema1"),
				Some("table1"),
			), // Exact duplicate
		]);

		let result = base.extend(&other);
		assert!(result.is_err());

		let error = result.unwrap_err();
		let diagnostic = error.diagnostic();
		assert_eq!(diagnostic.code, "EXTEND_002");
	}

	#[test]
	fn test_extend_mixed_qualifications() {
		let base = create_layout(vec![
			create_column_layout("col1", None, None),
			create_column_layout("col2", Some("schema1"), None),
			create_column_layout("col3", None, Some("table1")),
		]);
		let other = create_layout(vec![
			create_column_layout("col4", None, None),
			create_column_layout(
				"col5",
				Some("schema2"),
				Some("table2"),
			),
		]);

		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 5);

		// Verify all columns are present in order
		assert_eq!(result.columns[0].name, "col1");
		assert_eq!(result.columns[1].name, "col2");
		assert_eq!(result.columns[2].name, "col3");
		assert_eq!(result.columns[3].name, "col4");
		assert_eq!(result.columns[4].name, "col5");
	}

	#[test]
	fn test_extend_multiple_duplicates() {
		let base = create_layout(vec![
			create_column_layout("col1", None, None),
			create_column_layout("col2", None, None),
		]);
		let other = create_layout(vec![
			create_column_layout("col1", None, None), /* First duplicate */
			create_column_layout("col3", None, None),
			create_column_layout("col2", None, None), /* Second duplicate */
		]);

		// Should fail on the first duplicate encountered
		let result = base.extend(&other);
		assert!(result.is_err());

		let error = result.unwrap_err();
		let diagnostic = error.diagnostic();
		assert_eq!(diagnostic.code, "EXTEND_002");
		assert!(diagnostic.message.contains("col1")); // First duplicate
	}

	#[test]
	fn test_extend_preserves_original_order() {
		let base = create_layout(vec![
			create_column_layout("base1", None, None),
			create_column_layout("base2", Some("schema1"), None),
		]);
		let other = create_layout(vec![
			create_column_layout("other1", None, Some("table1")),
			create_column_layout(
				"other2",
				Some("schema2"),
				Some("table2"),
			),
		]);

		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 4);

		// Base columns should come first, in original order
		assert_eq!(result.columns[0].name, "base1");
		assert_eq!(result.columns[1].name, "base2");

		// Other columns should follow, in original order
		assert_eq!(result.columns[2].name, "other1");
		assert_eq!(result.columns[3].name, "other2");
	}

	#[test]
	fn test_extend_capacity_optimization() {
		let base = create_layout(vec![create_column_layout(
			"col1", None, None,
		)]);
		let other = create_layout(vec![
			create_column_layout("col2", None, None),
			create_column_layout("col3", None, None),
		]);

		let result = base.extend(&other).unwrap();

		// Should have exactly the expected number of columns
		assert_eq!(result.columns.len(), 3);
		assert_eq!(result.columns.capacity(), 3); // Should be optimally sized
	}

	#[test]
	fn test_extend_case_sensitive_names() {
		let base = create_layout(vec![create_column_layout(
			"Column", None, None,
		)]);
		let other = create_layout(vec![
			create_column_layout("column", None, None), /* Different case */
			create_column_layout("COLUMN", None, None), /* Different case */
		]);

		// Should succeed because column names are case-sensitive
		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 3);
		assert_eq!(result.columns[0].name, "Column");
		assert_eq!(result.columns[1].name, "column");
		assert_eq!(result.columns[2].name, "COLUMN");
	}

	#[test]
	fn test_extend_none_vs_empty_string_schema() {
		let base = create_layout(vec![create_column_layout(
			"col1", None, None,
		)]);
		let other = create_layout(vec![
			create_column_layout("col1", Some(""), None), /* Empty string vs None */
		]);

		// Should succeed because None != Some("")
		let result = base.extend(&other).unwrap();
		assert_eq!(result.columns.len(), 2);
		assert_eq!(result.columns[0].schema, None);
		assert_eq!(result.columns[1].schema, Some("".to_string()));
	}
}

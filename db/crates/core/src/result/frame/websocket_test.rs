// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

#[cfg(test)]
mod tests {
	use crate::{Frame, value::columnar::ColumnData};

	#[test]
	fn test_frame_serialization_preserves_column_order() {
		use crate::value::columnar::{
			Column, ColumnQualified, Columns,
		};

		// Create columns using the same method as the versions table
		let mut names_to_insert = ColumnData::utf8_with_capacity(1);
		names_to_insert.push("reifydb");

		let mut versions_to_insert = ColumnData::utf8_with_capacity(1);
		versions_to_insert.push("0.0.1");

		let mut descriptions_to_insert =
			ColumnData::utf8_with_capacity(1);
		descriptions_to_insert.push("ReifyDB Database System");

		let mut types_to_insert = ColumnData::utf8_with_capacity(1);
		types_to_insert.push("package");

		let columns = vec![
			Column::ColumnQualified(ColumnQualified {
				name: "name".to_string(),
				data: names_to_insert,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "version".to_string(),
				data: versions_to_insert,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "description".to_string(),
				data: descriptions_to_insert,
			}),
			Column::ColumnQualified(ColumnQualified {
				name: "type".to_string(),
				data: types_to_insert,
			}),
		];

		// Convert to Frame like in the actual code path
		let columns_struct = Columns::new(columns);
		let frame: Frame = columns_struct.into();

		// Serialize to JSON
		let json = serde_json::to_string(&frame).unwrap();

		// Deserialize back
		let deserialized: Frame = serde_json::from_str(&json).unwrap();

		// Verify column order is preserved
		assert_eq!(deserialized.len(), 4);
		assert_eq!(deserialized[0].name, "name");
		assert_eq!(deserialized[1].name, "version");
		assert_eq!(deserialized[2].name, "description");
		assert_eq!(deserialized[3].name, "type");
	}
}

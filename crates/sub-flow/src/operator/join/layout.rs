use reifydb_core::{Row, value::encoded::EncodedValuesNamedLayout};
use reifydb_type::{RowNumber, Type, Value};

/// Builder for creating combined layouts when joining left and right rows.
/// Encapsulates the logic for merging field names (with conflict resolution)
/// and types from both sides of a join.
pub(crate) struct JoinedLayoutBuilder {
	layout: EncodedValuesNamedLayout,
	left_field_count: usize,
	total_fields: usize,
}

impl JoinedLayoutBuilder {
	/// Create a new layout builder from left and right row templates.
	/// Handles name conflicts by applying the alias or double-underscore prefix.
	pub(crate) fn new(left: &Row, right: &Row, alias: &Option<String>) -> Self {
		let left_field_count = left.layout.fields().fields.len();
		let right_field_count = right.layout.fields().fields.len();
		let total_fields = left_field_count + right_field_count;

		let left_names = left.layout.names();
		let mut combined_names = Vec::with_capacity(total_fields);
		let mut combined_types = Vec::with_capacity(total_fields);

		// Add left side columns
		for i in 0..left_field_count {
			if i < left_names.len() {
				combined_names.push(left_names[i].clone());
			}
			combined_types.push(left.layout.fields().fields[i].r#type);
		}

		// Add right side columns with ALWAYS-prefix behavior
		let right_names = right.layout.names();
		for i in 0..right_field_count {
			if i < right_names.len() {
				let col_name = &right_names[i];

				// ALWAYS prefix right columns with alias (should always be Some now)
				let alias_str = alias.as_deref().unwrap_or("other");
				let prefixed_name = format!("{}_{}", alias_str, col_name);

				// Check for secondary conflict (prefixed name already exists in combined_names)
				let mut final_name = prefixed_name.clone();
				if combined_names.contains(&final_name) {
					let mut counter = 2;
					loop {
						let candidate = format!("{}_{}", prefixed_name, counter);
						if !combined_names.contains(&candidate) {
							final_name = candidate;
							break;
						}
						counter += 1;
					}
				}

				combined_names.push(final_name);
			}
			combined_types.push(right.layout.fields().fields[i].r#type);
		}

		let fields: Vec<(String, Type)> = combined_names.into_iter().zip(combined_types.into_iter()).collect();
		let layout = EncodedValuesNamedLayout::new(fields);

		Self {
			layout,
			left_field_count,
			total_fields,
		}
	}

	/// Combine values from left and right rows into a single value vector.
	fn combine_values(&self, left: &Row, right: &Row) -> Vec<Value> {
		let mut combined = Vec::with_capacity(self.total_fields);

		for i in 0..self.left_field_count {
			combined.push(left.layout.get_value_by_idx(&left.encoded, i));
		}

		for i in 0..right.layout.fields().fields.len() {
			combined.push(right.layout.get_value_by_idx(&right.encoded, i));
		}

		combined
	}

	/// Build a joined row with the given row number.
	pub(crate) fn build_row(&self, row_number: RowNumber, left: &Row, right: &Row) -> Row {
		let combined_values = self.combine_values(left, right);
		let mut encoded_row = self.layout.allocate();
		self.layout.set_values(&mut encoded_row, &combined_values);

		Row {
			number: row_number,
			encoded: encoded_row,
			layout: self.layout.clone(),
		}
	}
}

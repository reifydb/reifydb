use reifydb_core::value::row::Row;
use reifydb_type::Type;
use serde::{Deserialize, Serialize};

/// Schema information for both sides of the join
/// Tracks column names and types (previously called JoinLayout)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct Schema {
	pub(crate) left_names: Vec<String>,
	pub(crate) left_types: Vec<Type>,
	pub(crate) right_names: Vec<String>,
	pub(crate) right_types: Vec<Type>,
}

impl Schema {
	pub(crate) fn new() -> Self {
		Default::default()
	}

	pub(crate) fn update_left_from_row(&mut self, row: &Row) {
		let names = row.layout.names();
		let types: Vec<Type> = row.layout.fields.iter().map(|f| f.r#type).collect();

		if self.left_names.is_empty() {
			self.left_names = names.to_vec();
			self.left_types = types;
			return;
		}

		// Update types to keep the most specific/defined type
		for (i, new_type) in types.iter().enumerate() {
			if i < self.left_types.len() {
				if self.left_types[i] == Type::Undefined && *new_type != Type::Undefined {
					self.left_types[i] = *new_type;
				}
			} else {
				self.left_types.push(*new_type);
				if i < names.len() {
					self.left_names.push(names[i].clone());
				}
			}
		}
	}

	pub(crate) fn update_right_from_row(&mut self, row: &Row) {
		let names = row.layout.names();
		let types: Vec<Type> = row.layout.fields.iter().map(|f| f.r#type).collect();

		if self.right_names.is_empty() {
			self.right_names = names.to_vec();
			self.right_types = types;
			return;
		}

		// Update types to keep the most specific/defined type
		for (i, new_type) in types.iter().enumerate() {
			if i < self.right_types.len() {
				if self.right_types[i] == Type::Undefined && *new_type != Type::Undefined {
					self.right_types[i] = *new_type;
				}
			} else {
				self.right_types.push(*new_type);
				if i < names.len() {
					self.right_names.push(names[i].clone());
				}
			}
		}
	}
}

//! Builder patterns for constructing flow changes and rows

use std::collections::HashMap;

use reifydb_core::{
	CommitVersion, Row,
	interface::{FlowNodeId, SourceId},
	value::encoded::EncodedValuesNamedLayout,
};
use reifydb_type::{RowNumber, Type, Value};

use crate::{FlowChange, FlowChangeOrigin, FlowDiff};

/// Builder for constructing FlowChange instances
pub struct FlowChangeBuilder {
	diffs: Vec<FlowDiff>,
	origin: Option<FlowChangeOrigin>,
	version: Option<CommitVersion>,
}

impl FlowChangeBuilder {
	/// Create a new FlowChangeBuilder
	pub fn new() -> Self {
		Self {
			diffs: Vec::new(),
			origin: None,
			version: None,
		}
	}

	/// Create a builder for an external flow change
	pub fn external(source: SourceId, version: CommitVersion) -> Self {
		Self {
			diffs: Vec::new(),
			origin: Some(FlowChangeOrigin::External(source)),
			version: Some(version),
		}
	}

	/// Create a builder for an internal flow change
	pub fn internal(from: FlowNodeId, version: CommitVersion) -> Self {
		Self {
			diffs: Vec::new(),
			origin: Some(FlowChangeOrigin::Internal(from)),
			version: Some(version),
		}
	}

	/// Add an insert diff
	pub fn insert(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Insert {
			post: row,
		});
		self
	}

	/// Add an update diff
	pub fn update(mut self, pre: Row, post: Row) -> Self {
		self.diffs.push(FlowDiff::Update {
			pre,
			post,
		});
		self
	}

	/// Add a remove diff
	pub fn remove(mut self, row: Row) -> Self {
		self.diffs.push(FlowDiff::Remove {
			pre: row,
		});
		self
	}

	/// Add a diff directly
	pub fn diff(mut self, diff: FlowDiff) -> Self {
		self.diffs.push(diff);
		self
	}

	/// Add multiple diffs
	pub fn diffs(mut self, diffs: impl IntoIterator<Item = FlowDiff>) -> Self {
		self.diffs.extend(diffs);
		self
	}

	/// Set the origin
	pub fn with_origin(mut self, origin: FlowChangeOrigin) -> Self {
		self.origin = Some(origin);
		self
	}

	/// Set the version
	pub fn with_version(mut self, version: CommitVersion) -> Self {
		self.version = Some(version);
		self
	}

	/// Build the FlowChange
	pub fn build(self) -> FlowChange {
		FlowChange {
			origin: self
				.origin
				.expect("FlowChange requires an origin - use with_origin(), external(), or internal()"),
			diffs: self.diffs,
			version: self.version.unwrap_or(CommitVersion(0)),
		}
	}
}

/// Builder for constructing Row instances
pub struct RowBuilder {
	number: RowNumber,
	fields: Vec<(String, Value)>,
}

impl RowBuilder {
	/// Create a new RowBuilder
	pub fn new(number: impl Into<RowNumber>) -> Self {
		Self {
			number: number.into(),
			fields: Vec::new(),
		}
	}

	/// Add a field with a value
	pub fn field(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
		self.fields.push((name.into(), value.into()));
		self
	}

	/// Add an undefined field
	pub fn undefined_field(mut self, name: impl Into<String>) -> Self {
		self.fields.push((name.into(), Value::Undefined));
		self
	}

	/// Add fields from a map
	pub fn fields_from_map(mut self, map: HashMap<String, Value>) -> Self {
		self.fields.extend(map);
		self
	}

	/// Build the Row
	pub fn build(self) -> Row {
		// Create layout from fields
		let layout_fields: Vec<(String, Type)> =
			self.fields.iter().map(|(name, value)| (name.clone(), value.get_type())).collect();

		let layout = EncodedValuesNamedLayout::new(layout_fields.into_iter());

		// Encode values
		// let mut encoded_bytes = Vec::new();
		// for (_, value) in &self.fields {
		//     // Simple encoding: serialize the value as JSON then to bytes
		//     // In production, this would use the proper encoding format
		//     let json_str = serde_json::to_string(value).unwrap_or_default();
		//     let bytes = json_str.as_bytes();
		//     encoded_bytes.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
		//     encoded_bytes.extend_from_slice(bytes);
		// }

		todo!();

		// Row {
		//     number: self.number,
		//     encoded: EncodedValues(CowVec::new(encoded_bytes)),
		//     layout,
		// }
	}
}

use std::collections::BTreeMap;

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	Error, Row,
	interface::{FlowNodeId, Transaction},
	value::encoded::EncodedValuesLayout,
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator};
use reifydb_type::{Blob, RowNumber, Type, internal_error};
use serde::{Deserialize, Serialize};

use crate::{
	flow::{FlowChange, FlowDiff},
	operator::{
		Operator,
		stateful::{RawStatefulOperator, SingleStateful},
		transform::TransformOperator,
	},
};

/// Serializable version of Row data
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializedRow {
	number: RowNumber,
	/// The raw encoded bytes of the encoded
	#[serde(with = "serde_bytes")]
	encoded_bytes: Vec<u8>,
	/// The field names from the layout (for recreating EncodedRowNamedLayout)
	field_names: Vec<String>,
	/// The field types from the layout
	field_types: Vec<Type>,
}

impl SerializedRow {
	fn from_row(row: &Row) -> Self {
		Self {
			number: row.number,
			encoded_bytes: row.encoded.as_slice().to_vec(),
			field_names: row.layout.names().to_vec(),
			field_types: row.layout.fields.iter().map(|f| f.r#type).collect(),
		}
	}

	fn to_row(self) -> Row {
		use reifydb_core::{
			util::CowVec,
			value::encoded::{EncodedValues, EncodedValuesNamedLayout},
		};

		let fields: Vec<(String, Type)> =
			self.field_names.into_iter().zip(self.field_types.into_iter()).collect();

		let layout = EncodedValuesNamedLayout::new(fields);
		let encoded = EncodedValues(CowVec::new(self.encoded_bytes));

		Row {
			number: self.number,
			encoded,
			layout,
		}
	}
}

/// State for tracking the top N rows
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TakeState {
	/// Map of encoded numbers to their serialized encoded data
	/// Using BTreeMap to keep rows sorted by RowNumber
	rows: BTreeMap<RowNumber, SerializedRow>,
}

impl Default for TakeState {
	fn default() -> Self {
		Self {
			rows: BTreeMap::new(),
		}
	}
}

pub struct TakeOperator {
	node: FlowNodeId,
	limit: usize,
	layout: EncodedValuesLayout,
}

impl TakeOperator {
	pub fn new(node: FlowNodeId, limit: usize) -> Self {
		Self {
			node,
			limit,
			layout: EncodedValuesLayout::new(&[Type::Blob]),
		}
	}

	fn load_take_state<T: Transaction>(&self, txn: &mut StandardCommandTransaction<T>) -> crate::Result<TakeState> {
		let state_row = self.load_state(txn)?;

		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(TakeState::default());
		}

		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(TakeState::default());
		}

		let config = standard();
		decode_from_slice(blob.as_ref(), config)
			.map(|(state, _)| state)
			.map_err(|e| Error(internal_error!("Failed to deserialize TakeState: {}", e)))
	}

	fn save_take_state<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		state: &TakeState,
	) -> crate::Result<()> {
		let config = standard();
		let serialized = encode_to_vec(state, config)
			.map_err(|e| Error(internal_error!("Failed to serialize TakeState: {}", e)))?;

		let mut state_row = self.layout.allocate_row();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);

		self.save_state(txn, state_row)
	}
}

impl<T: Transaction> TransformOperator<T> for TakeOperator {}

impl<T: Transaction> RawStatefulOperator<T> for TakeOperator {}

impl<T: Transaction> SingleStateful<T> for TakeOperator {
	fn layout(&self) -> EncodedValuesLayout {
		self.layout.clone()
	}
}

impl<T: Transaction> Operator<T> for TakeOperator {
	fn id(&self) -> FlowNodeId {
		self.node
	}

	fn apply(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		change: FlowChange,
		_evaluator: &StandardRowEvaluator,
	) -> crate::Result<FlowChange> {
		// Load current state
		let mut state = self.load_take_state(txn)?;
		let mut output_diffs = Vec::new();

		for diff in change.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let row_number = post.number;

					// Add to our tracking
					state.rows.insert(row_number, SerializedRow::from_row(&post));

					// Keep only the top N rows (highest encoded numbers)
					// BTreeMap keeps keys sorted, so we can efficiently get the top N
					while state.rows.len() > self.limit {
						// Remove the smallest (oldest) encoded number
						if let Some((&removed_row_num, removed_serialized)) =
							state.rows.iter().next()
						{
							let removed_serialized = removed_serialized.clone();
							state.rows.remove(&removed_row_num);

							// Emit a remove for the encoded that fell out of the window
							output_diffs.push(FlowDiff::Remove {
								pre: removed_serialized.to_row(),
							});
						}
					}

					// If this encoded is within the limit, emit the insert
					if state.rows.contains_key(&row_number) {
						output_diffs.push(FlowDiff::Insert {
							post,
						});
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					let row_number = post.number;

					// Update our tracking if this encoded is in the window
					if state.rows.contains_key(&row_number) {
						state.rows.insert(row_number, SerializedRow::from_row(&post));
						output_diffs.push(FlowDiff::Update {
							pre,
							post,
						});
					}
					// If not in window, ignore the update
				}
				FlowDiff::Remove {
					pre,
				} => {
					let row_number = pre.number;

					// If this encoded was in our window, remove it
					if state.rows.remove(&row_number).is_some() {
						output_diffs.push(FlowDiff::Remove {
							pre,
						});

						// Note: When a encoded is removed from the window, we might want to
						// pull in the next encoded that was previously outside the window.
						// However, we don't have access to those rows here.
						// This is a limitation of the current approach.
					}
				}
			}
		}

		// Save the updated state
		self.save_take_state(txn, &state)?;

		Ok(FlowChange::internal(self.node, change.version, output_diffs))
	}
}

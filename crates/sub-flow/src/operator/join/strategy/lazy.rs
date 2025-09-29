use reifydb_core::{
	FrameColumnData,
	interface::{Command, ExecuteCommand, Identity, Transaction},
	value::row::{EncodedRowNamedLayout, Row},
};
use reifydb_engine::{StandardCommandTransaction, StandardRowEvaluator, execute::Executor};
use reifydb_hash::Hash128;
use reifydb_rql::query::QueryString;
use reifydb_type::{Params, Type};

use crate::operator::join::{JoinState, operator::JoinOperator};

/// Query the right side and return rows that match the join condition
pub(crate) fn query_right_side<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	query_string: &QueryString,
	executor: &Executor,
	key_hash: Hash128,
	state: &mut JoinState,
	operator: &JoinOperator,
) -> crate::Result<Vec<Row>> {
	// Execute the query without parameters
	// The query may have its own filter (e.g., "from table | filter condition")
	// but we don't inject parameters from the left row
	let query = Command {
		rql: query_string.as_str(),
		params: Params::None,
		identity: &Identity::root(), // TODO: Should use proper identity from context
	};

	// Execute the query to get all right-side rows
	let results = executor.execute_command(txn, query)?;

	let mut right_rows = Vec::new();

	// Process query results - each frame contains rows to join with
	for frame in results {
		let frame_rows = process_query_frame(&frame, state, key_hash, operator)?;
		right_rows.extend(frame_rows);
	}

	Ok(right_rows)
}

/// Process a single query frame and return matching rows
pub(crate) fn process_query_frame(
	frame: &reifydb_core::Frame,
	state: &mut JoinState,
	key_hash: Hash128,
	operator: &JoinOperator,
) -> crate::Result<Vec<Row>> {
	let mut matching_rows = Vec::new();

	// Get row count from columns
	let row_count = if let Some(first_column) = frame.columns.first() {
		first_column.data.len()
	} else {
		return Ok(matching_rows);
	};

	// Extract schema from frame columns
	let (frame_names, frame_types) = extract_frame_schema(&frame.columns);

	// Update state schema if empty (for backward compatibility)
	if state.schema.right_types.is_empty() && !frame.columns.is_empty() {
		state.schema.right_names = frame_names.clone();
		state.schema.right_types = frame_types.clone();
	}

	// Process rows in order to ensure consistency
	for row_idx in 0..row_count {
		// Get the actual row number from frame.row_numbers
		let row_number = frame.row_numbers.get(row_idx).copied().unwrap();

		// Create a Row from the frame data
		let right_row = create_row_from_frame(&frame.columns, &frame_names, &frame_types, row_idx, row_number)?;

		// Compute the join key hash for this right row
		let evaluator = StandardRowEvaluator::new();
		let right_key_hash = operator.compute_join_key(&right_row, &operator.right_exprs, &evaluator)?;

		// Only include this row if it matches the left row's key
		if let Some(hash) = right_key_hash {
			if hash == key_hash {
				matching_rows.push(right_row);
			}
		}
	}

	Ok(matching_rows)
}

/// Extract schema (names and types) from frame columns
pub(crate) fn extract_frame_schema(columns: &[reifydb_core::FrameColumn]) -> (Vec<String>, Vec<Type>) {
	let mut frame_names = Vec::new();
	let mut frame_types = Vec::new();

	for column in columns.iter() {
		frame_names.push(column.name.clone());
		frame_types.push(infer_column_type(&column.data));
	}

	(frame_names, frame_types)
}

/// Infer the Type from FrameColumnData
pub(crate) fn infer_column_type(data: &FrameColumnData) -> Type {
	match data {
		FrameColumnData::Bool(_) => Type::Boolean,
		FrameColumnData::Int1(_) => Type::Int1,
		FrameColumnData::Int2(_) => Type::Int2,
		FrameColumnData::Int4(_) => Type::Int4,
		FrameColumnData::Int8(_) => Type::Int8,
		FrameColumnData::Uint1(_) => Type::Uint1,
		FrameColumnData::Uint2(_) => Type::Uint2,
		FrameColumnData::Uint4(_) => Type::Uint4,
		FrameColumnData::Uint8(_) => Type::Uint8,
		FrameColumnData::Uint16(_) => Type::Uint16,
		FrameColumnData::Float4(_) => Type::Float4,
		FrameColumnData::Float8(_) => Type::Float8,
		FrameColumnData::Utf8(_) => Type::Utf8,
		FrameColumnData::RowNumber(_) => Type::RowNumber,
		_ => Type::Undefined,
	}
}

/// Create a Row object from frame data at a specific index
pub(crate) fn create_row_from_frame(
	columns: &[reifydb_core::FrameColumn],
	frame_names: &[String],
	frame_types: &[Type],
	row_idx: usize,
	row_number: reifydb_type::RowNumber,
) -> crate::Result<Row> {
	// Extract values for this row from all columns
	let mut values = Vec::new();
	for column in columns.iter() {
		values.push(column.data.get_value(row_idx));
	}

	// Create a Row with the proper structure using the frame's schema
	let fields: Vec<(String, Type)> =
		frame_names.iter().zip(frame_types.iter()).map(|(name, typ)| (name.clone(), typ.clone())).collect();

	debug_assert!(!fields.is_empty());

	let layout = EncodedRowNamedLayout::new(fields);
	let mut encoded_row = layout.allocate_row();
	layout.set_values(&mut encoded_row, &values);

	Ok(Row {
		number: row_number,
		encoded: encoded_row,
		layout,
	})
}

/// Check if there are any right rows matching the key
/// This executes the query and checks if any rows match
pub(crate) fn has_matching_right_rows<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	query_string: &QueryString,
	executor: &Executor,
	key_hash: Hash128,
	state: &mut JoinState,
	operator: &JoinOperator,
) -> crate::Result<bool> {
	let rows = query_right_side(txn, query_string, executor, key_hash, state, operator)?;
	Ok(!rows.is_empty())
}

/// Check if a specific right row is the only one matching
pub(crate) fn is_only_matching_right_row<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	query_string: &QueryString,
	executor: &Executor,
	key_hash: Hash128,
	state: &mut JoinState,
	operator: &JoinOperator,
	target_row: &Row,
) -> crate::Result<bool> {
	let rows = query_right_side(txn, query_string, executor, key_hash, state, operator)?;
	Ok(rows.len() == 1 && rows[0].number == target_row.number)
}

/// Check if there are any other right rows besides the one being removed
pub(crate) fn has_other_right_rows<T: Transaction>(
	txn: &mut StandardCommandTransaction<T>,
	query_string: &QueryString,
	executor: &Executor,
	key_hash: Hash128,
	state: &mut JoinState,
	operator: &JoinOperator,
	excluding_row: &Row,
) -> crate::Result<bool> {
	let rows = query_right_side(txn, query_string, executor, key_hash, state, operator)?;
	Ok(rows.iter().any(|r| r.number != excluding_row.number))
}

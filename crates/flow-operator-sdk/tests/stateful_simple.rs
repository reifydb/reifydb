//! Simple integration tests for FFI stateful traits
//!
//! These tests demonstrate the usage patterns for the three stateful trait types.

use std::collections::HashMap;

use reifydb_core::{Row, interface::FlowNodeId, value::encoded::EncodedValuesLayout};
use reifydb_flow_operator_sdk::{
	FFIOperator, FFIOperatorMetadata, FlowChange,
	context::OperatorContext,
	error::Result,
	stateful::{FFIKeyedStateful, FFIRawStatefulOperator, FFISingleStateful, FFIWindowStateful},
};
use reifydb_type::{RowNumber, Type, Value};

// ============================================================================
// 1. Counter Operator (FFISingleStateful)
// ============================================================================

/// Counter operator that tracks the number of operations
struct CounterOperator {
	operator_id: FlowNodeId,
}

impl FFIOperatorMetadata for CounterOperator {
	const NAME: &'static str = "counter";
	const VERSION: u32 = 1;
}

impl FFIOperator for CounterOperator {
	fn new(operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
		Ok(Self {
			operator_id,
		})
	}

	fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
		// In a real implementation, we would use:
		// self.update_state(ctx, |layout, row| {
		//     let count = layout.get_i32(row, 0);
		//     layout.set_i32(row, 0, count + 1);
		//     Ok(())
		// })?;
		Ok(input)
	}

	fn get_rows(&mut self, _ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<Vec<Option<Row>>> {
		Ok(vec![None; row_numbers.len()])
	}
}

impl FFIRawStatefulOperator for CounterOperator {}

impl FFISingleStateful for CounterOperator {
	fn layout(&self) -> EncodedValuesLayout {
		// Single Int32 counter field
		EncodedValuesLayout::new(&[Type::Int4])
	}
}

// ============================================================================
// 2. Group Sum Operator (FFIKeyedStateful)
// ============================================================================

/// Group-by sum operator
struct GroupSumOperator {
	operator_id: FlowNodeId,
}

impl FFIOperatorMetadata for GroupSumOperator {
	const NAME: &'static str = "group_sum";
	const VERSION: u32 = 1;
}

impl FFIOperator for GroupSumOperator {
	fn new(operator_id: FlowNodeId, _config: &HashMap<String, Value>) -> Result<Self> {
		Ok(Self {
			operator_id,
		})
	}

	fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
		// In a real implementation, we would use:
		// for diff in &input.diffs {
		//     if let FlowDiff::Insert { post } = diff {
		//         let group_key = post.get(0)?;
		//         let amount = /* extract from row */;
		//         self.update_state(ctx, &[group_key], |layout, row| {
		//             let sum = layout.get_f64(row, 0);
		//             layout.set_f64(row, 0, sum + amount);
		//             Ok(())
		//         })?;
		//     }
		// }
		Ok(input)
	}

	fn get_rows(&mut self, _ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<Vec<Option<Row>>> {
		Ok(vec![None; row_numbers.len()])
	}
}

impl FFIRawStatefulOperator for GroupSumOperator {}

impl FFIKeyedStateful for GroupSumOperator {
	fn layout(&self) -> EncodedValuesLayout {
		// State: (sum: Float8, count: Int32)
		EncodedValuesLayout::new(&[Type::Float8, Type::Int4])
	}

	fn key_types(&self) -> &[Type] {
		// Group by single Int32 key
		&[Type::Int4]
	}
}

// ============================================================================
// 3. Sliding Window Operator (FFIWindowStateful)
// ============================================================================

/// Sliding window aggregation operator
struct SlidingWindowOperator {
	operator_id: FlowNodeId,
	window_size_ms: i64,
}

impl FFIOperatorMetadata for SlidingWindowOperator {
	const NAME: &'static str = "sliding_window";
	const VERSION: u32 = 1;
}

impl FFIOperator for SlidingWindowOperator {
	fn new(operator_id: FlowNodeId, config: &HashMap<String, Value>) -> Result<Self> {
		let window_size_ms = config
			.get("window_size_ms")
			.and_then(|v| {
				if let Value::Int8(val) = v {
					Some(*val)
				} else {
					None
				}
			})
			.unwrap_or(60_000); // Default 1 minute

		Ok(Self {
			operator_id,
			window_size_ms,
		})
	}

	fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
		// In a real implementation, we would:
		// 1. Extract timestamp from input row
		// 2. Calculate window_start = (timestamp / window_size_ms) * window_size_ms
		// 3. Create window_key from window_start
		// 4. Update window state using update_window()
		// 5. Periodically expire old windows using remove_window()
		Ok(input)
	}

	fn get_rows(&mut self, _ctx: &mut OperatorContext, row_numbers: &[RowNumber]) -> Result<Vec<Option<Row>>> {
		Ok(vec![None; row_numbers.len()])
	}
}

impl FFIRawStatefulOperator for SlidingWindowOperator {}

impl FFIWindowStateful for SlidingWindowOperator {
	fn layout(&self) -> EncodedValuesLayout {
		// State: (sum: Float8, count: Int32, min: Float8, max: Float8)
		EncodedValuesLayout::new(&[Type::Float8, Type::Int4, Type::Float8, Type::Float8])
	}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_counter_operator_compiles() {
		let operator_id = FlowNodeId(1);
		let config = HashMap::new();
		let operator = CounterOperator::new(operator_id, &config).unwrap();

		// Verify layout
		let layout = operator.layout();
		assert_eq!(layout.fields.len(), 1);
		assert_eq!(layout.fields[0].r#type, Type::Int4);
	}

	#[test]
	fn test_counter_default_key() {
		let operator_id = FlowNodeId(1);
		let operator = CounterOperator::new(operator_id, &HashMap::new()).unwrap();

		// Verify default key is empty
		let key = operator.key();
		assert_eq!(key.as_bytes().len(), 0);
	}

	#[test]
	fn test_counter_create_state() {
		let operator_id = FlowNodeId(1);
		let operator = CounterOperator::new(operator_id, &HashMap::new()).unwrap();

		let state = operator.create_state();
		assert!(state.as_ref().len() > 0);

		let layout = operator.layout();
		let count = layout.get_i32(&state, 0);
		assert_eq!(count, 0);
	}

	#[test]
	fn test_group_sum_operator_compiles() {
		let operator_id = FlowNodeId(1);
		let config = HashMap::new();
		let operator = GroupSumOperator::new(operator_id, &config).unwrap();

		let layout = operator.layout();
		assert_eq!(layout.fields.len(), 2);
		assert_eq!(layout.fields[0].r#type, Type::Float8);
		assert_eq!(layout.fields[1].r#type, Type::Int4);
	}

	#[test]
	fn test_group_sum_key_types() {
		let operator_id = FlowNodeId(1);
		let operator = GroupSumOperator::new(operator_id, &HashMap::new()).unwrap();

		let key_types = operator.key_types();
		assert_eq!(key_types.len(), 1);
		assert_eq!(key_types[0], Type::Int4);
	}

	#[test]
	fn test_group_sum_encode_key() {
		let operator_id = FlowNodeId(1);
		let operator = GroupSumOperator::new(operator_id, &HashMap::new()).unwrap();

		let key1 = vec![Value::Int4(100)];
		let key2 = vec![Value::Int4(200)];

		let encoded1 = operator.encode_key(&key1);
		let encoded2 = operator.encode_key(&key2);

		// Different keys should produce different encodings
		assert_ne!(encoded1.as_bytes(), encoded2.as_bytes());

		// Same key should produce same encoding
		let encoded1_again = operator.encode_key(&key1);
		assert_eq!(encoded1.as_bytes(), encoded1_again.as_bytes());
	}

	#[test]
	fn test_group_sum_create_state() {
		let operator_id = FlowNodeId(1);
		let operator = GroupSumOperator::new(operator_id, &HashMap::new()).unwrap();

		let state = operator.create_state();
		let layout = operator.layout();

		// Verify defaults are 0
		let sum = layout.get_f64(&state, 0);
		let count = layout.get_i32(&state, 1);

		assert_eq!(sum, 0.0);
		assert_eq!(count, 0);
	}

	#[test]
	fn test_sliding_window_operator_compiles() {
		let mut config = HashMap::new();
		config.insert("window_size_ms".to_string(), Value::Int8(30_000));

		let operator_id = FlowNodeId(1);
		let operator = SlidingWindowOperator::new(operator_id, &config).unwrap();
		assert_eq!(operator.window_size_ms, 30_000);
	}

	#[test]
	fn test_sliding_window_layout() {
		let operator_id = FlowNodeId(1);
		let operator = SlidingWindowOperator::new(operator_id, &HashMap::new()).unwrap();

		let layout = operator.layout();
		assert_eq!(layout.fields.len(), 4);
		assert_eq!(layout.fields[0].r#type, Type::Float8);
		assert_eq!(layout.fields[1].r#type, Type::Int4);
		assert_eq!(layout.fields[2].r#type, Type::Float8);
		assert_eq!(layout.fields[3].r#type, Type::Float8);
	}

	#[test]
	fn test_sliding_window_create_state() {
		let operator_id = FlowNodeId(1);
		let operator = SlidingWindowOperator::new(operator_id, &HashMap::new()).unwrap();

		let state = operator.create_state();
		let layout = operator.layout();

		// Verify defaults
		let sum = layout.get_f64(&state, 0);
		let count = layout.get_i32(&state, 1);
		let min = layout.get_f64(&state, 2);
		let max = layout.get_f64(&state, 3);

		assert_eq!(sum, 0.0);
		assert_eq!(count, 0);
		assert_eq!(min, 0.0);
		assert_eq!(max, 0.0);
	}
}

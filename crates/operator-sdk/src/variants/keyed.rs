//! Keyed operator pattern for operators that partition state by key

use crate::builders::FlowChangeBuilder;
use crate::context::OperatorContext;
use crate::error::Result;
use crate::operator::{FlowChange, FlowDiff, FFIOperator};
use reifydb_core::Row;
use reifydb_type::Value;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashMap;

/// Trait for operators that maintain state partitioned by key
pub trait KeyedOperator: Send + Sync + 'static {
	/// The type used for keyed state
	type State: Serialize + DeserializeOwned + Default + Clone + Send + Sync;

	/// Extract the key from a row
	fn get_key(&self, row: &Row) -> Vec<Value>;

	/// Process a row with its keyed state
	fn process_keyed(&mut self, state: &mut Self::State, row: &Row) -> Result<Option<Row>>;
}

/// Adapter that converts a KeyedOperator to a regular Operator
pub struct KeyedAdapter<T: KeyedOperator> {
	inner: T,
	states: HashMap<String, T::State>,
}

impl<T: KeyedOperator> KeyedAdapter<T> {
	/// Create a new adapter
	pub fn new(inner: T) -> Self {
		Self {
			inner,
			states: HashMap::new(),
		}
	}

	/// Convert key values to string for storage
	fn key_to_string(key: &[Value]) -> String {
		// Simple serialization for keys
		key.iter().map(|v| format!("{:?}", v)).collect::<Vec<_>>().join(":")
	}
}

impl<T: KeyedOperator> FFIOperator for KeyedAdapter<T> {
	fn new() -> Self {
		// TODO: This needs redesign for the new operator model
		panic!("KeyedAdapter needs redesign for new operator model")
	}

	fn initialize(&mut self, _config: &HashMap<String, Value>) -> Result<()> {
		// Load saved states from context if available
		Ok(())
	}

	fn apply(&mut self, ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
		// Load states from context
		if let Some(states) = ctx.state().get::<HashMap<String, T::State>>("keyed_states")? {
			self.states = states;
		}

		let mut builder = FlowChangeBuilder::new().with_version(input.version);

		for diff in input.diffs {
			match diff {
				FlowDiff::Insert {
					post,
				} => {
					let key = self.inner.get_key(&post);
					let key_str = Self::key_to_string(&key);

					let state = self.states.entry(key_str).or_default();
					if let Some(transformed) = self.inner.process_keyed(state, &post)? {
						builder = builder.insert(transformed);
					}
				}
				FlowDiff::Update {
					pre,
					post,
				} => {
					let key = self.inner.get_key(&post);
					let key_str = Self::key_to_string(&key);

					let state = self.states.entry(key_str).or_default();
					if let Some(transformed) = self.inner.process_keyed(state, &post)? {
						builder = builder.update(pre, transformed);
					} else {
						builder = builder.remove(pre);
					}
				}
				FlowDiff::Remove {
					pre,
				} => {
					builder = builder.remove(pre);
				}
			}
		}

		// Save states back to context
		ctx.state().set("keyed_states", &self.states)?;

		Ok(builder.build())
	}

	fn get_rows(&mut self, _ctx: &mut OperatorContext, _row_numbers: &[reifydb_type::RowNumber]) -> Result<Vec<Option<Row>>> {
		// TODO: Implement for keyed pattern
		Ok(vec![])
	}
}

/// Helper function to create a keyed operator adapter
pub fn keyed<T: KeyedOperator>(operator: T) -> KeyedAdapter<T> {
	KeyedAdapter::new(operator)
}

/// Example: Group-by aggregation
#[cfg(test)]
mod examples {
    use super::*;

    #[derive(Default, Clone, Serialize, serde::Deserialize)]
	struct GroupState {
		count: u64,
		sum: f64,
	}

	struct GroupByAggregator {
		key_field: String,
		value_field: String,
	}

	impl KeyedOperator for GroupByAggregator {
		type State = GroupState;

		fn get_key(&self, _row: &Row) -> Vec<Value> {
			// Extract key field from row
			// For now, return a dummy key
			vec![Value::Utf8("key".to_string())]
		}

		fn process_keyed(&mut self, state: &mut Self::State, _row: &Row) -> Result<Option<Row>> {
			// Update state with new value
			state.count += 1;
			// state.sum += extract_value(row, &self.value_field);

			// Return aggregated row
			Ok(Some(_row.clone()))
		}
	}
}

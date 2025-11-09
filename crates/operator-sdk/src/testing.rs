//! Testing utilities for operators

use crate::builders::{FlowChangeBuilder, RowBuilder};
use crate::context::MockContext;
use crate::error::Result;
use crate::operator::{FlowChange, FlowDiff, Operator};
use reifydb_core::Row;

/// Test harness for operators
pub struct TestHarness {
	inputs: Vec<FlowChange>,
	expected_outputs: Vec<FlowChange>,
	context: MockContext,
}

impl TestHarness {
	/// Create a new test harness
	pub fn new() -> Self {
		Self {
			inputs: Vec::new(),
			expected_outputs: Vec::new(),
			context: MockContext::new(),
		}
	}

	/// Add an input flow change
	pub fn with_input(mut self, input: FlowChange) -> Self {
		self.inputs.push(input);
		self
	}

	/// Add an expected output
	pub fn expect_output(mut self, output: FlowChange) -> Self {
		self.expected_outputs.push(output);
		self
	}

	/// Set initial state
	pub fn with_state<T: serde::Serialize>(mut self, key: &str, value: &T) -> Result<Self> {
		self.context = self.context.with_state(key, value)?;
		Ok(self)
	}

	/// Run the test
	pub fn run<O: Operator>(mut self, mut operator: O) -> Result<()> {
		assert_eq!(
			self.inputs.len(),
			self.expected_outputs.len(),
			"Number of inputs must match number of expected outputs"
		);

		for (input, expected) in self.inputs.into_iter().zip(self.expected_outputs) {
			let actual = operator.apply(self.context.as_mut(), input)?;
			assert_flow_changes_equal(&actual, &expected)?;
		}

		Ok(())
	}

	/// Run and return outputs (for inspection)
	pub fn run_and_get_outputs<O: Operator>(mut self, mut operator: O) -> Result<Vec<FlowChange>> {
		let mut outputs = Vec::new();

		for input in self.inputs {
			let output = operator.apply(self.context.as_mut(), input)?;
			outputs.push(output);
		}

		Ok(outputs)
	}
}

/// Builder for creating test scenarios
pub struct TestBuilder<O: Operator> {
	operator: O,
	scenarios: Vec<TestScenario>,
}

#[derive(Clone)]
struct TestScenario {
	input: FlowChange,
	expected: Option<FlowChange>,
}

impl<O: Operator> TestBuilder<O> {
	/// Create a new test builder
	pub fn new(operator: O) -> Self {
		Self {
			operator,
			scenarios: Vec::new(),
		}
	}

	/// Add a test scenario
	pub fn scenario(mut self, input: FlowChange, expected: FlowChange) -> Self {
		self.scenarios.push(TestScenario {
			input,
			expected: Some(expected),
		});
		self
	}

	/// Add an input without checking output
	pub fn input(mut self, input: FlowChange) -> Self {
		self.scenarios.push(TestScenario {
			input,
			expected: None,
		});
		self
	}

	/// Run all scenarios
	pub fn run(mut self) -> Result<()> {
		let mut ctx = MockContext::new();

		for scenario in self.scenarios {
			let actual = self.operator.apply(ctx.as_mut(), scenario.input.clone())?;

			if let Some(expected) = scenario.expected {
				assert_flow_changes_equal(&actual, &expected)?;
			}
		}

		Ok(())
	}
}

/// Helper to test an operator
pub fn test_operator<O: Operator>(operator: O) -> TestBuilder<O> {
	TestBuilder::new(operator)
}

/// Assert two flow changes are equal
fn assert_flow_changes_equal(actual: &FlowChange, expected: &FlowChange) -> Result<()> {
	if actual.version != expected.version {
		return Err(crate::error::Error::Other(format!(
			"Version mismatch: expected {}, got {}",
			expected.version, actual.version
		)));
	}

	if actual.diffs.len() != expected.diffs.len() {
		return Err(crate::error::Error::Other(format!(
			"Diff count mismatch: expected {}, got {}",
			expected.diffs.len(),
			actual.diffs.len()
		)));
	}

	for (i, (actual_diff, expected_diff)) in actual.diffs.iter().zip(&expected.diffs).enumerate() {
		if !diffs_equal(actual_diff, expected_diff) {
			return Err(crate::error::Error::Other(format!("Diff {} mismatch", i)));
		}
	}

	Ok(())
}

/// Compare two diffs
fn diffs_equal(a: &FlowDiff, b: &FlowDiff) -> bool {
	match (a, b) {
		(
			FlowDiff::Insert {
				post: a,
			},
			FlowDiff::Insert {
				post: b,
			},
		) => rows_equal(a, b),
		(
			FlowDiff::Update {
				pre: pre_a,
				post: post_a,
			},
			FlowDiff::Update {
				pre: pre_b,
				post: post_b,
			},
		) => rows_equal(pre_a, pre_b) && rows_equal(post_a, post_b),
		(
			FlowDiff::Remove {
				pre: a,
			},
			FlowDiff::Remove {
				pre: b,
			},
		) => rows_equal(a, b),
		_ => false,
	}
}

/// Compare two rows
fn rows_equal(a: &Row, b: &Row) -> bool {
	// Simple comparison - in practice we'd need more sophisticated comparison
	a.number == b.number
}

/// Test data generators
pub mod generators {
    use super::*;
    use serde_json::json;

    /// Generate a sequence of insert changes
	pub fn insert_sequence(start: u64, count: usize) -> Vec<FlowChange> {
		(0..count)
			.map(|i| {
				FlowChangeBuilder::new()
					.insert(RowBuilder::from_json(
						start + i as u64,
						json!({
						    "id": i,
						    "value": i * 2,
						}),
					))
					.with_version(i as u64)
					.build()
			})
			.collect()
	}

	/// Generate update changes
	pub fn update_sequence(start: u64, count: usize) -> Vec<FlowChange> {
		(0..count)
			.map(|i| {
				let pre = RowBuilder::from_json(
					start + i as u64,
					json!({
					    "id": i,
					    "value": i * 2,
					}),
				);
				let post = RowBuilder::from_json(
					start + i as u64,
					json!({
					    "id": i,
					    "value": i * 3,
					}),
				);
				FlowChangeBuilder::new().update(pre, post).with_version(i as u64).build()
			})
			.collect()
	}

	/// Generate a mixed sequence of changes
	pub fn mixed_sequence(count: usize) -> Vec<FlowChange> {
		(0..count)
			.map(|i| {
				let row = RowBuilder::from_json(
					i as u64,
					json!({
					    "id": i,
					    "type": i % 3,
					}),
				);

				let mut builder = FlowChangeBuilder::new();
				match i % 3 {
					0 => builder = builder.insert(row),
					1 => {
						let pre = row.clone();
						builder = builder.update(pre, row)
					}
					_ => builder = builder.remove(row),
				}
				builder.with_version(i as u64).build()
			})
			.collect()
	}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OperatorContext;

    #[derive(Default)]
	struct PassthroughOperator;

	impl Operator for PassthroughOperator {
		fn apply(&mut self, _ctx: &mut OperatorContext, input: FlowChange) -> Result<FlowChange> {
			Ok(input)
		}
	}

	#[test]
	fn test_harness_basic() {
		let input = FlowChangeBuilder::new().insert(RowBuilder::new(0u64).build()).build();

		let expected = input.clone();

		TestHarness::new().with_input(input).expect_output(expected).run(PassthroughOperator).unwrap();
	}

	#[test]
	fn test_builder_pattern() {
		let input = FlowChangeBuilder::new().insert(RowBuilder::new(0u64).build()).build();

		test_operator(PassthroughOperator).scenario(input.clone(), input).run().unwrap();
	}
}

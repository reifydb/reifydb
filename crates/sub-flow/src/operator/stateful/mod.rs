// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{
		CommandTransaction, FlowNodeId,
		expression::Expression,
		key::{EncodableKey, FlowNodeStateKey},
	},
	row::EncodedRow,
	util::CowVec,
};

use crate::operator::Operator;

pub mod builtin;
pub mod registry;

pub use builtin::*;

pub trait StatefulOperator<T: CommandTransaction>: Operator<T> {
	fn id(&self) -> FlowNodeId;

	fn read_state(&self, txn: &mut T) -> crate::Result<Vec<u8>> {
		let key = FlowNodeStateKey::new(self.id());
		let encoded_key = key.encode();
		match txn.get(&encoded_key)? {
			Some(versioned) => Ok(versioned.row.as_ref().to_vec()),
			None => Ok(Vec::new()),
		}
	}

	fn write_state(
		&self,
		txn: &mut T,
		state: Vec<u8>,
	) -> crate::Result<()> {
		let key = FlowNodeStateKey::new(self.id());
		let encoded_key = key.encode();
		let encoded_row = EncodedRow(CowVec::new(state));
		txn.set(&encoded_key, encoded_row)?;
		Ok(())
	}

	fn clear_state(&self, txn: &mut T) -> crate::Result<()> {
		let key = FlowNodeStateKey::new(self.id());
		let encoded_key = key.encode();
		txn.remove(&encoded_key)?;
		Ok(())
	}
}

pub trait StatefulOperatorFactory<T: CommandTransaction>: Send + Sync {
	fn create_from_expressions(
		node: FlowNodeId,
		expressions: &[Expression<'static>],
	) -> crate::Result<Box<dyn Operator<T>>>;
}

pub mod extract {
	use reifydb_core::interface::expression::ConstantExpression;

	use super::*;

	pub fn int(expr: &Expression) -> crate::Result<i64> {
		match expr {
			Expression::Constant(ConstantExpression::Number {
				fragment,
			}) => {
				// Parse the number from the fragment text
				let text = fragment.text();
				text.parse::<i64>().map_err(|_| {
					panic!(
						"Failed to parse integer from: {}",
						text
					)
				})
			}
			_ => panic!("Expected integer value"),
		}
	}

	/// Extract float from expression
	pub fn float(expr: &Expression) -> crate::Result<f64> {
		match expr {
			Expression::Constant(ConstantExpression::Number {
				fragment,
			}) => {
				// Parse the number from the fragment text
				let text = fragment.text();
				text.parse::<f64>().map_err(|_| {
					panic!(
						"Failed to parse float from: {}",
						text
					)
				})
			}
			_ => panic!("Expected numeric value"),
		}
	}

	/// Extract string from expression
	pub fn string(expr: &Expression) -> crate::Result<String> {
		match expr {
			Expression::Constant(ConstantExpression::Text {
				fragment,
			}) => Ok(fragment.text().to_string()),
			Expression::Column(col) => {
				// Convert Fragment to string
				match &col.0 {
					reifydb_type::Fragment::Owned(
						owned,
					) => Ok(owned.to_string()),
					reifydb_type::Fragment::Borrowed(
						borrowed,
					) => Ok(borrowed.text().to_string()),
					_ => unimplemented!(),
				}
			}
			_ => panic!("Expected string value"),
		}
	}
}

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, EncodedKeyRange,
	interface::{
		FlowNodeId,
		expression::Expression,
		key::{EncodableKey, FlowNodeStateKey},
	},
	row::EncodedRow,
	util::CowVec,
};

use crate::operator::Operator;

// Iterator wrapper for state entries
pub struct StateEntryIterator<'a> {
	inner: BoxedVersionedIter<'a>,
}

impl<'a> Iterator for StateEntryIterator<'a> {
	type Item = (EncodedKey, EncodedRow);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|versioned| {
			if let Some(state_key) = FlowNodeStateKey::decode(&versioned.key) {
				(EncodedKey::new(state_key.key), versioned.row)
			} else {
				(versioned.key, versioned.row)
			}
		})
	}
}

pub mod builtin;
pub mod registry;

pub use builtin::*;
use reifydb_core::interface::{
	BoxedVersionedIter, Transaction, VersionedCommandTransaction, VersionedQueryTransaction,
};
use reifydb_engine::StandardCommandTransaction;

pub trait TransformOperator<T: Transaction>: Operator<T> {
	fn id(&self) -> FlowNodeId;

	fn get(&self, txn: &mut StandardCommandTransaction<T>, key: &EncodedKey) -> crate::Result<EncodedRow> {
		let state_key = FlowNodeStateKey::new(self.id(), key.as_ref().to_vec());

		let encoded_key = state_key.encode();

		match txn.get(&encoded_key)? {
			Some(versioned) => Ok(versioned.row),
			None => Ok(EncodedRow(CowVec::new(Vec::new()))),
		}
	}

	fn set(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		key: &EncodedKey,
		value: EncodedRow,
	) -> crate::Result<()> {
		let state_key = FlowNodeStateKey::new(self.id(), key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		txn.set(&encoded_key, value)?;
		Ok(())
	}

	fn remove(&self, txn: &mut StandardCommandTransaction<T>, key: &EncodedKey) -> crate::Result<()> {
		let state_key = FlowNodeStateKey::new(self.id(), key.as_ref().to_vec());
		let encoded_key = state_key.encode();
		txn.remove(&encoded_key)?;
		Ok(())
	}

	fn scan<'a>(&self, txn: &'a mut StandardCommandTransaction<T>) -> crate::Result<StateEntryIterator<'a>> {
		let range = FlowNodeStateKey::node_range(self.id());
		Ok(StateEntryIterator {
			inner: txn.range(range)?,
		})
	}

	fn range<'a>(
		&self,
		txn: &'a mut StandardCommandTransaction<T>,
		start_key: Option<&EncodedKey>,
		end_key: Option<&EncodedKey>,
	) -> crate::Result<StateEntryIterator<'a>> {
		let start = start_key
			.map(|key| FlowNodeStateKey::new(self.id(), key.as_ref().to_vec()).encode())
			.or_else(|| Some(FlowNodeStateKey::new_empty(self.id()).encode()));

		let end = end_key
			.map(|key| FlowNodeStateKey::new(self.id(), key.as_ref().to_vec()).encode())
			.or_else(|| Some(FlowNodeStateKey::new_empty(FlowNodeId(self.id().0 + 1)).encode()));

		let range = EncodedKeyRange::start_end(start, end);
		Ok(StateEntryIterator {
			inner: txn.range(range)?,
		})
	}

	fn clear(&self, txn: &mut StandardCommandTransaction<T>) -> crate::Result<()> {
		let range = FlowNodeStateKey::node_range(self.id());
		let keys_to_remove: Vec<_> = txn.range(range)?.map(|versioned| versioned.key).collect();

		for key in keys_to_remove {
			txn.remove(&key)?;
		}
		Ok(())
	}
}

pub trait TransformOperatorFactory<T: Transaction>: Send + Sync {
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
				text.parse::<i64>().map_err(|_| panic!("Failed to parse integer from: {}", text))
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
				text.parse::<f64>().map_err(|_| panic!("Failed to parse float from: {}", text))
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
				match &col.0.name {
					reifydb_type::Fragment::Owned(owned) => Ok(owned.to_string()),
					reifydb_type::Fragment::Borrowed(borrowed) => Ok(borrowed.text().to_string()),
					_ => unimplemented!(),
				}
			}
			_ => panic!("Expected string value"),
		}
	}
}

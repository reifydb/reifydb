// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	Result,
	value::{Value, value_type::ValueType},
};

use crate::{
	internal_err,
	value::column::buffer::{ColumnBuffer, with_container},
};

impl ColumnBuffer {
	pub fn push_typed(&mut self, value: Value, declared: &ValueType) -> Result<()> {
		match declared {
			ValueType::Option(inner_type) => self.push_typed_option(value, inner_type),
			_ => self.push_typed_required(value, declared),
		}
	}

	fn push_typed_option(&mut self, value: Value, inner_type: &ValueType) -> Result<()> {
		let buffer_type = self.get_type();
		let ColumnBuffer::Option {
			inner,
			bitvec,
		} = self
		else {
			return internal_err!(
				"column declares Option({:?}) but its buffer is {:?}; a buffer that stops being an Option publishes a column type the schema never declared",
				inner_type,
				buffer_type
			);
		};
		if inner.get_type() != *inner_type {
			return internal_err!(
				"column declares Option({:?}) but its buffer holds Option({:?})",
				inner_type,
				inner.get_type()
			);
		}
		match value {
			Value::None {
				inner: none_type,
			} => {
				if none_type != *inner_type {
					return internal_err!(
						"column declares Option({:?}) but received a none of type {:?}",
						inner_type,
						none_type
					);
				}
				with_container!(inner.as_mut(), |c| c.push_default());
				bitvec.push(false);
			}
			value => {
				let value_type = value.get_type();
				if value_type != *inner_type {
					return internal_err!(
						"column declares Option({:?}) but received a value of type {:?}",
						inner_type,
						value_type
					);
				}
				inner.push_value(value);
				bitvec.push(true);
			}
		}
		Ok(())
	}

	fn push_typed_required(&mut self, value: Value, declared: &ValueType) -> Result<()> {
		let buffer_type = self.get_type();
		if buffer_type != *declared {
			return internal_err!("column declares {:?} but its buffer is {:?}", declared, buffer_type);
		}
		let value_type = value.get_type();
		if value_type != *declared {
			return internal_err!(
				"column declares {:?} but received a value of type {:?}; a required column can hold neither a none nor another type",
				declared,
				value_type
			);
		}
		self.push_value(value);
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, value_type::ValueType};

	use crate::value::column::ColumnBuffer;

	fn optional_utf8() -> ValueType {
		ValueType::Option(Box::new(ValueType::Utf8))
	}

	#[test]
	fn an_optional_column_stays_an_option_when_the_first_value_is_present() {
		// push_value drops the Option wrapper when the first present value lands on an empty buffer, so the
		// published column type would otherwise depend on row order.
		let mut typed = ColumnBuffer::with_capacity(optional_utf8(), 0);
		typed.push_typed(Value::Utf8("a".to_string()), &optional_utf8()).unwrap();
		assert_eq!(
			typed.get_type(),
			optional_utf8(),
			"declared Option(Utf8) must survive a present first value"
		);

		let mut inferred = ColumnBuffer::with_capacity(optional_utf8(), 0);
		inferred.push_value(Value::Utf8("a".to_string()));
		assert_eq!(
			inferred.get_type(),
			ValueType::Utf8,
			"push_value is expected to demote here; if it stops, this test is guarding nothing"
		);
	}

	#[test]
	fn an_optional_column_keeps_its_type_across_every_arrival_order() {
		// A column that alternates present and none must publish exactly one type; a shifting type breaks any
		// consumer reading the registered schema.
		for order in [[true, false, true], [false, true, false]] {
			let mut buffer = ColumnBuffer::with_capacity(optional_utf8(), 0);
			for present in order {
				let value = if present {
					Value::Utf8("x".to_string())
				} else {
					Value::none_of(ValueType::Utf8)
				};
				buffer.push_typed(value, &optional_utf8()).unwrap();
			}
			assert_eq!(buffer.get_type(), optional_utf8(), "order {order:?} changed the column type");
			assert_eq!(buffer.len(), 3);
		}
	}

	#[test]
	fn a_value_of_the_wrong_type_is_named_rather_than_reaching_an_unimplemented_arm() {
		// Without both types in the message this is no better than the anonymous unimplemented!() it replaces.
		let mut buffer = ColumnBuffer::with_capacity(optional_utf8(), 0);
		let err = buffer.push_typed(Value::Uint8(7), &optional_utf8()).unwrap_err();
		let message = err.to_string();
		assert!(message.contains("Utf8"), "error must name the declared type, got: {message}");
		assert!(message.contains("Uint8"), "error must name the received type, got: {message}");
	}

	#[test]
	fn a_required_column_refuses_a_none() {
		// push_value promotes a required buffer to Option on a none, publishing a nullable column the schema
		// never declared.
		let mut buffer = ColumnBuffer::with_capacity(ValueType::Utf8, 0);
		assert!(buffer.push_typed(Value::none_of(ValueType::Utf8), &ValueType::Utf8).is_err());
		assert_eq!(buffer.get_type(), ValueType::Utf8, "a refused push must leave the buffer untouched");
		assert_eq!(buffer.len(), 0);
	}

	#[test]
	fn a_none_of_the_wrong_inner_type_is_refused() {
		// A none carries its own type; accepting a mismatched one lets a column claim nullability for a type it
		// never held.
		let mut buffer = ColumnBuffer::with_capacity(optional_utf8(), 0);
		assert!(buffer.push_typed(Value::none_of(ValueType::Uint8), &optional_utf8()).is_err());
		assert_eq!(buffer.len(), 0);
	}

	#[test]
	fn a_required_column_accepts_its_own_type() {
		let mut buffer = ColumnBuffer::with_capacity(ValueType::Uint2, 0);
		buffer.push_typed(Value::Uint2(3), &ValueType::Uint2).unwrap();
		assert_eq!(buffer.get_type(), ValueType::Uint2);
		assert_eq!(buffer.len(), 1);
	}
}

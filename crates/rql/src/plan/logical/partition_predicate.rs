// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::column::Column;
use reifydb_value::{
	fragment::Fragment,
	value::{
		Value,
		boolean::parse::parse_bool,
		number::parse::{parse_primitive_int, parse_primitive_uint},
		partition::Partition,
		temporal::parse::{
			date::parse_date, datetime::parse_datetime, duration::parse_duration, time::parse_time,
		},
		uuid::parse::{parse_identity_id, parse_uuid4, parse_uuid7},
		value_type::ValueType,
	},
};

use crate::expression::{ColumnExpression, ConstantExpression, Expression};

pub fn extract_partition(condition: &Expression, columns: &[Column], partition_by: &[String]) -> Option<Partition> {
	if partition_by.is_empty() {
		return None;
	}

	let mut conjuncts = Vec::new();
	flatten_and(condition, &mut conjuncts);

	let mut values: Vec<Value> = Vec::with_capacity(partition_by.len());
	for col_name in partition_by {
		let col = columns.iter().find(|c| c.name == *col_name)?;
		if col.dictionary_id.is_some() {
			return None;
		}
		let value_type = col.constraint.get_type();
		let fragment = conjuncts.iter().find_map(|e| eq_literal_for(e, col_name, &value_type))?;
		values.push(partition_value(&value_type, fragment)?);
	}

	Some(Partition::of(&values))
}

fn flatten_and<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
	match expr {
		Expression::And(and) => {
			flatten_and(&and.left, out);
			flatten_and(&and.right, out);
		}
		other => out.push(other),
	}
}

fn eq_literal_for(expr: &Expression, col_name: &str, value_type: &ValueType) -> Option<Fragment> {
	let Expression::Equal(eq) = expr else {
		return None;
	};
	if is_column(&eq.left, col_name) {
		return literal_fragment(&eq.right, value_type);
	}
	if is_column(&eq.right, col_name) {
		return literal_fragment(&eq.left, value_type);
	}
	None
}

fn is_column(expr: &Expression, col_name: &str) -> bool {
	match expr {
		Expression::Column(ColumnExpression(col)) => col.name.text() == col_name,
		Expression::AccessSource(access) => access.column.name.text() == col_name,
		_ => false,
	}
}

fn literal_fragment(expr: &Expression, value_type: &ValueType) -> Option<Fragment> {
	match expr {
		Expression::Constant(constant) => constant_fragment(constant, value_type),
		Expression::Cast(cast) => {
			if cast.to.ty != *value_type {
				return None;
			}
			match cast.expression.as_ref() {
				Expression::Constant(ConstantExpression::Text {
					fragment,
				}) => Some(fragment.clone()),
				_ => None,
			}
		}
		_ => None,
	}
}

fn constant_fragment(constant: &ConstantExpression, value_type: &ValueType) -> Option<Fragment> {
	match constant {
		ConstantExpression::Text {
			fragment,
		} if *value_type == ValueType::Utf8 => Some(fragment.clone()),
		ConstantExpression::Bool {
			fragment,
		} if *value_type == ValueType::Boolean => Some(fragment.clone()),
		ConstantExpression::Number {
			fragment,
		} if value_type.is_number() => Some(fragment.clone()),
		_ => None,
	}
}

fn partition_value(value_type: &ValueType, fragment: Fragment) -> Option<Value> {
	match value_type {
		ValueType::Boolean => parse_bool(fragment).ok().map(Value::Boolean),
		ValueType::Int1 => parse_primitive_int::<i8>(fragment).ok().map(Value::Int1),
		ValueType::Int2 => parse_primitive_int::<i16>(fragment).ok().map(Value::Int2),
		ValueType::Int4 => parse_primitive_int::<i32>(fragment).ok().map(Value::Int4),
		ValueType::Int8 => parse_primitive_int::<i64>(fragment).ok().map(Value::Int8),
		ValueType::Int16 => parse_primitive_int::<i128>(fragment).ok().map(Value::Int16),
		ValueType::Uint1 => parse_primitive_uint::<u8>(fragment).ok().map(Value::Uint1),
		ValueType::Uint2 => parse_primitive_uint::<u16>(fragment).ok().map(Value::Uint2),
		ValueType::Uint4 => parse_primitive_uint::<u32>(fragment).ok().map(Value::Uint4),
		ValueType::Uint8 => parse_primitive_uint::<u64>(fragment).ok().map(Value::Uint8),
		ValueType::Uint16 => parse_primitive_uint::<u128>(fragment).ok().map(Value::Uint16),
		ValueType::Utf8 => Some(Value::Utf8(fragment.text().to_string())),
		ValueType::Date => parse_date(fragment).ok().map(Value::Date),
		ValueType::DateTime => parse_datetime(fragment).ok().map(Value::DateTime),
		ValueType::Time => parse_time(fragment).ok().map(Value::Time),
		ValueType::Duration => parse_duration(fragment).ok().map(Value::Duration),
		ValueType::Uuid4 => parse_uuid4(fragment).ok().map(Value::Uuid4),
		ValueType::Uuid7 => parse_uuid7(fragment).ok().map(Value::Uuid7),
		ValueType::IdentityId => parse_identity_id(fragment).ok().map(Value::IdentityId),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{
		catalog::{
			column::{Column, ColumnIndex},
			id::ColumnId,
		},
		identifier::{ColumnIdentifier, ColumnShape},
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{
			constraint::TypeConstraint,
			dictionary::DictionaryId,
			temporal::parse::{date::parse_date, datetime::parse_datetime},
			uuid::parse::parse_uuid4,
		},
	};

	use super::*;
	use crate::expression::{
		AndExpression, CastExpression, EqExpression, GreaterThanExpression, OrExpression, TypeExpression,
	};

	// The writer hashes the column's declared-type Value (crates/engine/src/partition.rs and
	// crates/sub-flow/src/operator/sink/partition.rs). Partition::of postcards each Value, so
	// Value::Int4(1) and Value::Int8(1) hash differently. Every prune must therefore reproduce the
	// writer's exact Value variant; a mismatch silently drops rows from a pruned scan. Each
	// `prunes_*` test below is that round-trip invariant, and each `declines_*` test pins a case
	// where equality and postcard encoding disagree (or no literal exists), where pruning would be
	// unsound rather than merely absent.

	fn col(name: &str, ty: ValueType) -> Column {
		Column {
			id: ColumnId(1),
			name: name.to_string(),
			constraint: TypeConstraint::unconstrained(ty),
			properties: Vec::new(),
			index: ColumnIndex(0),
			auto_increment: false,
			dictionary_id: None,
		}
	}

	fn column_ref(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			shape: ColumnShape::Alias(Fragment::internal("t")),
			name: Fragment::internal(name),
		}))
	}

	fn text(value: &str) -> Expression {
		Expression::Constant(ConstantExpression::Text {
			fragment: Fragment::internal(value),
		})
	}

	fn number(value: &str) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::internal(value),
		})
	}

	fn boolean(value: &str) -> Expression {
		Expression::Constant(ConstantExpression::Bool {
			fragment: Fragment::internal(value),
		})
	}

	fn cast(inner: Expression, ty: ValueType) -> Expression {
		Expression::Cast(CastExpression {
			fragment: Fragment::internal("cast"),
			expression: Box::new(inner),
			to: TypeExpression {
				fragment: Fragment::internal("ty"),
				ty,
			},
		})
	}

	fn eq(left: Expression, right: Expression) -> Expression {
		Expression::Equal(EqExpression {
			left: Box::new(left),
			right: Box::new(right),
			fragment: Fragment::internal("=="),
		})
	}

	fn and(left: Expression, right: Expression) -> Expression {
		Expression::And(AndExpression {
			left: Box::new(left),
			right: Box::new(right),
			fragment: Fragment::internal("and"),
		})
	}

	fn or(left: Expression, right: Expression) -> Expression {
		Expression::Or(OrExpression {
			left: Box::new(left),
			right: Box::new(right),
			fragment: Fragment::internal("or"),
		})
	}

	fn by(names: &[&str]) -> Vec<String> {
		names.iter().map(|n| n.to_string()).collect()
	}

	#[test]
	fn prunes_utf8() {
		let columns = vec![col("region", ValueType::Utf8)];
		let condition = eq(column_ref("region"), text("east"));
		assert_eq!(
			extract_partition(&condition, &columns, &by(&["region"])),
			Some(Partition::of(&[Value::Utf8("east".to_string())]))
		);
	}

	#[test]
	fn prunes_utf8_with_reversed_operands() {
		let columns = vec![col("region", ValueType::Utf8)];
		let condition = eq(text("east"), column_ref("region"));
		assert_eq!(
			extract_partition(&condition, &columns, &by(&["region"])),
			Some(Partition::of(&[Value::Utf8("east".to_string())]))
		);
	}

	#[test]
	fn prunes_int4_as_declared_type_not_smallest_fitting() {
		// The general constant evaluator narrows a bare `1` to Int1. Pruning must instead parse it
		// into the column's declared Int4, because that is what the writer stored.
		let columns = vec![col("tenant", ValueType::Int4)];
		let condition = eq(column_ref("tenant"), number("1"));
		assert_eq!(
			extract_partition(&condition, &columns, &by(&["tenant"])),
			Some(Partition::of(&[Value::Int4(1)]))
		);
		assert_ne!(
			extract_partition(&condition, &columns, &by(&["tenant"])),
			Some(Partition::of(&[Value::Int1(1)])),
			"an Int1 hash would point at a partition the writer never wrote"
		);
	}

	#[test]
	fn prunes_int8() {
		let columns = vec![col("tenant", ValueType::Int8)];
		let condition = eq(column_ref("tenant"), number("9000000000"));
		assert_eq!(
			extract_partition(&condition, &columns, &by(&["tenant"])),
			Some(Partition::of(&[Value::Int8(9_000_000_000)]))
		);
	}

	#[test]
	fn prunes_uint8() {
		let columns = vec![col("tenant", ValueType::Uint8)];
		let condition = eq(column_ref("tenant"), number("7"));
		assert_eq!(
			extract_partition(&condition, &columns, &by(&["tenant"])),
			Some(Partition::of(&[Value::Uint8(7)]))
		);
	}

	#[test]
	fn prunes_boolean() {
		let columns = vec![col("active", ValueType::Boolean)];
		let condition = eq(column_ref("active"), boolean("true"));
		assert_eq!(
			extract_partition(&condition, &columns, &by(&["active"])),
			Some(Partition::of(&[Value::Boolean(true)]))
		);
	}

	#[test]
	fn prunes_date_via_cast() {
		// The engine's cast(Utf8 -> Date) calls the same parse_date, so the pruned partition and the
		// writer's stored partition are the same bytes by construction.
		let columns = vec![col("d", ValueType::Date)];
		let condition = eq(column_ref("d"), cast(text("2024-06-20"), ValueType::Date));
		let expected = Value::Date(parse_date(Fragment::internal("2024-06-20")).unwrap());
		assert_eq!(extract_partition(&condition, &columns, &by(&["d"])), Some(Partition::of(&[expected])));
	}

	#[test]
	fn prunes_datetime_via_cast() {
		let columns = vec![col("ts", ValueType::DateTime)];
		let literal = "2024-06-20T10:00:00Z";
		let condition = eq(column_ref("ts"), cast(text(literal), ValueType::DateTime));
		let expected = Value::DateTime(parse_datetime(Fragment::internal(literal)).unwrap());
		assert_eq!(extract_partition(&condition, &columns, &by(&["ts"])), Some(Partition::of(&[expected])));
	}

	#[test]
	fn prunes_uuid4_via_cast() {
		let columns = vec![col("u", ValueType::Uuid4)];
		let literal = "6ba7b810-9dad-41d1-80b4-00c04fd430c8";
		let condition = eq(column_ref("u"), cast(text(literal), ValueType::Uuid4));
		let expected = Value::Uuid4(parse_uuid4(Fragment::internal(literal)).unwrap());
		assert_eq!(extract_partition(&condition, &columns, &by(&["u"])), Some(Partition::of(&[expected])));
	}

	#[test]
	fn prunes_multi_column_in_partition_by_order() {
		// partition_col_indices maps names in partition_by order, so the reader must hash in the same
		// order. Swapping the conjuncts must not change the partition.
		let columns = vec![col("region", ValueType::Utf8), col("tenant", ValueType::Int4)];
		let expected = Partition::of(&[Value::Utf8("east".to_string()), Value::Int4(1)]);

		let forward = and(eq(column_ref("region"), text("east")), eq(column_ref("tenant"), number("1")));
		assert_eq!(extract_partition(&forward, &columns, &by(&["region", "tenant"])), Some(expected));

		let reversed = and(eq(column_ref("tenant"), number("1")), eq(column_ref("region"), text("east")));
		assert_eq!(extract_partition(&reversed, &columns, &by(&["region", "tenant"])), Some(expected));
	}

	#[test]
	fn multi_column_partition_is_order_sensitive() {
		let columns = vec![col("region", ValueType::Utf8), col("tier", ValueType::Utf8)];
		let condition = and(eq(column_ref("region"), text("east")), eq(column_ref("tier"), text("gold")));
		let a = extract_partition(&condition, &columns, &by(&["region", "tier"]));
		let b = extract_partition(&condition, &columns, &by(&["tier", "region"]));
		assert_ne!(a, b, "partition_by order must feed the hash");
	}

	#[test]
	fn declines_partial_predicate_on_multi_column_partition() {
		let columns = vec![col("region", ValueType::Utf8), col("tier", ValueType::Utf8)];
		let condition = eq(column_ref("region"), text("east"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["region", "tier"])), None);
	}

	#[test]
	fn declines_float8() {
		// -0.0 == 0.0 is Ordering::Equal but their postcard bytes differ, so a row the filter accepts
		// can live in a different partition than the one pruning would pick.
		let columns = vec![col("f", ValueType::Float8)];
		let condition = eq(column_ref("f"), number("0.0"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["f"])), None);
	}

	#[test]
	fn declines_decimal() {
		// 1.0 == 1.00 compares equal but BigDecimal encodes scale.
		let columns = vec![col("d", ValueType::Decimal)];
		let condition = eq(column_ref("d"), number("1.0"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["d"])), None);
	}

	#[test]
	fn declines_bignum_int() {
		let columns = vec![col("n", ValueType::Int)];
		let condition = eq(column_ref("n"), number("1"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["n"])), None);
	}

	#[test]
	fn declines_blob() {
		// A blob literal is blob::hex(..), an Expression::Call, never a foldable constant.
		let columns = vec![col("b", ValueType::Blob)];
		let condition = eq(column_ref("b"), text("576f726c64"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["b"])), None);
	}

	#[test]
	fn declines_dictionary_encoded_column() {
		let mut column = col("region", ValueType::DictionaryId);
		column.dictionary_id = Some(DictionaryId(1));
		let condition = eq(column_ref("region"), text("east"));
		assert_eq!(extract_partition(&condition, &[column], &by(&["region"])), None);
	}

	#[test]
	fn declines_cast_to_mismatched_type() {
		// cast("1", int8) against an int4 column would hash Int8(1) while the writer stored Int4(1).
		let columns = vec![col("tenant", ValueType::Int4)];
		let condition = eq(column_ref("tenant"), cast(text("1"), ValueType::Int8));
		assert_eq!(extract_partition(&condition, &columns, &by(&["tenant"])), None);
	}

	#[test]
	fn declines_out_of_range_numeric_literal() {
		let columns = vec![col("tenant", ValueType::Int1)];
		let condition = eq(column_ref("tenant"), number("300"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["tenant"])), None);
	}

	#[test]
	fn declines_kind_mismatched_literal() {
		let columns = vec![col("region", ValueType::Utf8)];
		let condition = eq(column_ref("region"), number("5"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["region"])), None);
	}

	#[test]
	fn declines_undefined_literal() {
		let columns = vec![col("region", ValueType::Utf8)];
		let condition = eq(
			column_ref("region"),
			Expression::Constant(ConstantExpression::None {
				fragment: Fragment::internal("none"),
			}),
		);
		assert_eq!(extract_partition(&condition, &columns, &by(&["region"])), None);
	}

	#[test]
	fn declines_disjunction() {
		let columns = vec![col("region", ValueType::Utf8)];
		let condition = or(eq(column_ref("region"), text("east")), eq(column_ref("region"), text("west")));
		assert_eq!(extract_partition(&condition, &columns, &by(&["region"])), None);
	}

	#[test]
	fn declines_non_equality_predicate() {
		let columns = vec![col("tenant", ValueType::Int4)];
		let condition = Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(column_ref("tenant")),
			right: Box::new(number("1")),
			fragment: Fragment::internal(">"),
		});
		assert_eq!(extract_partition(&condition, &columns, &by(&["tenant"])), None);
	}

	#[test]
	fn declines_unknown_partition_column() {
		let columns = vec![col("region", ValueType::Utf8)];
		let condition = eq(column_ref("region"), text("east"));
		assert_eq!(extract_partition(&condition, &columns, &by(&["nope"])), None);
	}

	#[test]
	fn declines_empty_partition_by() {
		let columns = vec![col("region", ValueType::Utf8)];
		let condition = eq(column_ref("region"), text("east"));
		assert_eq!(extract_partition(&condition, &columns, &[]), None);
	}
}

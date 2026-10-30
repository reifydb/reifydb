// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	error::diagnostic::catalog::{enum_value_expected, sumtype_variant_not_found},
	expression::{
		ConstantExpression, EqExpression, Expression, IsVariantExpression, NotEqExpression, PrefixExpression,
		PrefixOperator,
	},
	interface::{catalog::sumtype::SumType, resolved::ResolvedObject},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	return_error,
	value::{constraint::Constraint, sumtype::SumTypeId, value_type::ValueType},
};

use crate::Result;

pub enum EnumOperand {
	Name(Fragment),
	None(Fragment),
	Typed(Fragment, ValueType),
	Other,
}

pub fn text_literal(expression: &Expression) -> Result<EnumOperand> {
	match expression {
		Expression::Constant(ConstantExpression::Text {
			fragment,
		}) => Ok(EnumOperand::Name(fragment.clone())),
		_ => Ok(EnumOperand::Other),
	}
}

pub fn declared_sumtype_id(source: &ResolvedObject, col_name: &str) -> Option<SumTypeId> {
	if matches!(source, ResolvedObject::Dictionary(_)) {
		return None;
	}
	let tag_col_name = format!("{}_tag", col_name);
	let tag_col = source.columns().iter().find(|c| c.name == tag_col_name)?;
	match tag_col.constraint.constraint() {
		Some(Constraint::SumType(id)) => Some(*id),
		_ => None,
	}
}

pub fn variant_by_name(sumtype: &SumType, name: &Fragment) -> Result<usize> {
	match sumtype.variants.iter().position(|v| v.name.eq_ignore_ascii_case(name.text())) {
		Some(index) => Ok(index),
		None => return_error!(sumtype_variant_not_found(name.clone(), &sumtype.name)),
	}
}

pub fn resolve_enum_equality(
	expr: &mut Expression,
	source: &ResolvedObject,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
	operand: &mut dyn FnMut(&Expression) -> Result<EnumOperand>,
) -> Result<()> {
	for child in children_mut(expr) {
		resolve_enum_equality(child, source, catalog, rx, operand)?;
	}
	let (left, right, fragment, negated) = match expr {
		Expression::Equal(e) => (&e.left, &e.right, &e.fragment, false),
		Expression::NotEqual(e) => (&e.left, &e.right, &e.fragment, true),
		_ => return Ok(()),
	};
	let Expression::Column(column) = left.as_ref() else {
		return Ok(());
	};
	let col_name = column.0.name.text();
	let Some(declared) = declared_sumtype_id(source, col_name) else {
		return Ok(());
	};
	let (left, fragment) = (left.clone(), fragment.clone());
	*expr = match operand(right.as_ref())? {
		EnumOperand::Other => return Ok(()),
		EnumOperand::Name(name) => {
			let sumtype = catalog.get_sumtype(rx, declared)?;
			let variant = &sumtype.variants[variant_by_name(&sumtype, &name)?];
			let is_variant = Expression::IsVariant(IsVariantExpression {
				expression: left,
				sumtype_name: Fragment::internal(&sumtype.name),
				variant_name: Fragment::internal(&variant.name),
				tag: variant.tag,
				fragment: fragment.clone(),
			});
			match negated {
				false => is_variant,
				true => Expression::Prefix(PrefixExpression {
					operator: PrefixOperator::Not(fragment.clone()),
					expression: Box::new(is_variant),
					fragment,
				}),
			}
		}
		EnumOperand::None(none) => {
			let right = Box::new(Expression::Constant(ConstantExpression::None {
				fragment: none,
			}));
			match negated {
				false => Expression::Equal(EqExpression {
					left,
					right,
					fragment,
				}),
				true => Expression::NotEqual(NotEqExpression {
					left,
					right,
					fragment,
				}),
			}
		}
		EnumOperand::Typed(param, actual) => {
			let sumtype = catalog.get_sumtype(rx, declared)?;
			return_error!(enum_value_expected(param, col_name, &sumtype.name, actual));
		}
	};
	Ok(())
}

fn children_mut(expr: &mut Expression) -> Vec<&mut Expression> {
	match expr {
		Expression::Alias(e) => vec![&mut e.expression],
		Expression::Cast(e) => vec![&mut e.expression],
		Expression::Prefix(e) => vec![&mut e.expression],
		Expression::IsVariant(e) => vec![&mut e.expression],
		Expression::FieldAccess(e) => vec![&mut e.object],
		Expression::Add(e) => vec![&mut e.left, &mut e.right],
		Expression::Sub(e) => vec![&mut e.left, &mut e.right],
		Expression::Mul(e) => vec![&mut e.left, &mut e.right],
		Expression::Div(e) => vec![&mut e.left, &mut e.right],
		Expression::Rem(e) => vec![&mut e.left, &mut e.right],
		Expression::Equal(e) => vec![&mut e.left, &mut e.right],
		Expression::NotEqual(e) => vec![&mut e.left, &mut e.right],
		Expression::LessThan(e) => vec![&mut e.left, &mut e.right],
		Expression::LessThanEqual(e) => vec![&mut e.left, &mut e.right],
		Expression::GreaterThan(e) => vec![&mut e.left, &mut e.right],
		Expression::GreaterThanEqual(e) => vec![&mut e.left, &mut e.right],
		Expression::And(e) => vec![&mut e.left, &mut e.right],
		Expression::Or(e) => vec![&mut e.left, &mut e.right],
		Expression::Xor(e) => vec![&mut e.left, &mut e.right],
		Expression::Between(e) => vec![&mut e.value, &mut e.lower, &mut e.upper],
		Expression::In(e) => vec![&mut e.value, &mut e.list],
		Expression::Contains(e) => vec![&mut e.value, &mut e.list],
		Expression::Call(e) => e.args.iter_mut().collect(),
		Expression::Tuple(e) => e.expressions.iter_mut().collect(),
		Expression::List(e) => e.expressions.iter_mut().collect(),
		Expression::Map(e) => e.expressions.iter_mut().collect(),
		Expression::Extend(e) => e.expressions.iter_mut().collect(),
		Expression::Record(e) => e.fields.iter_mut().map(|(_, expression)| expression).collect(),
		Expression::If(e) => {
			let mut all: Vec<&mut Expression> = vec![&mut e.condition, &mut e.then_expr];
			for else_if in &mut e.else_ifs {
				all.push(&mut else_if.condition);
				all.push(&mut else_if.then_expr);
			}
			all.extend(e.else_expr.as_deref_mut());
			all
		}
		Expression::AccessSource(_)
		| Expression::Column(_)
		| Expression::Constant(_)
		| Expression::Type(_)
		| Expression::Parameter(_)
		| Expression::Variable(_) => vec![],
	}
}

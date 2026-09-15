// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::interface::resolved::ResolvedObject;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::constraint::Constraint;

use crate::{
	Result,
	expression::{
		AddExpression, AliasExpression, AndExpression, CallExpression, CastExpression, ContainsExpression,
		DivExpression, EqExpression, Expression, ExtendExpression, GreaterThanEqExpression,
		GreaterThanExpression, InExpression, IsVariantExpression, LessThanEqExpression, LessThanExpression,
		ListExpression, MapExpression, MulExpression, NotEqExpression, OrExpression, PrefixExpression,
		RemExpression, SubExpression, TupleExpression, XorExpression, name::display_label,
	},
};

pub fn resolve_is_variant_tags(
	expr: &mut Expression,
	source: &ResolvedObject,
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
) -> Result<()> {
	for_each_is_variant(expr, &mut |e| {
		let col_name = match e.expression.as_ref() {
			Expression::Column(c) => c.0.name.text().to_string(),
			other => display_label(other).text().to_string(),
		};

		let tag_col_name = format!("{}_tag", col_name);
		let columns = source.columns();
		if let Some(tag_col) = columns.iter().find(|c| c.name == tag_col_name)
			&& let Some(Constraint::SumType(id)) = tag_col.constraint.constraint()
		{
			let def = catalog.get_sumtype(rx, *id)?;
			let variant_name = e.variant_name.text().to_lowercase();
			if let Some(variant) = def.variants.iter().find(|v| v.name.to_lowercase() == variant_name) {
				e.tag = Some(variant.tag);
			}
		}
		Ok(())
	})
}

pub fn for_each_is_variant(
	expr: &mut Expression,
	f: &mut dyn FnMut(&mut IsVariantExpression) -> Result<()>,
) -> Result<()> {
	match expr {
		Expression::IsVariant(e) => {
			f(e)?;
			for_each_is_variant(&mut e.expression, f)?;
		}
		Expression::Add(AddExpression {
			left,
			right,
			..
		})
		| Expression::Div(DivExpression {
			left,
			right,
			..
		})
		| Expression::Rem(RemExpression {
			left,
			right,
			..
		})
		| Expression::Mul(MulExpression {
			left,
			right,
			..
		})
		| Expression::Sub(SubExpression {
			left,
			right,
			..
		})
		| Expression::GreaterThan(GreaterThanExpression {
			left,
			right,
			..
		})
		| Expression::GreaterThanEqual(GreaterThanEqExpression {
			left,
			right,
			..
		})
		| Expression::LessThan(LessThanExpression {
			left,
			right,
			..
		})
		| Expression::LessThanEqual(LessThanEqExpression {
			left,
			right,
			..
		})
		| Expression::Equal(EqExpression {
			left,
			right,
			..
		})
		| Expression::NotEqual(NotEqExpression {
			left,
			right,
			..
		})
		| Expression::And(AndExpression {
			left,
			right,
			..
		})
		| Expression::Or(OrExpression {
			left,
			right,
			..
		})
		| Expression::Xor(XorExpression {
			left,
			right,
			..
		}) => {
			for_each_is_variant(left, f)?;
			for_each_is_variant(right, f)?;
		}
		Expression::In(InExpression {
			value,
			list,
			..
		})
		| Expression::Contains(ContainsExpression {
			value,
			list,
			..
		}) => {
			for_each_is_variant(value, f)?;
			for_each_is_variant(list, f)?;
		}
		Expression::Between(e) => {
			for_each_is_variant(&mut e.value, f)?;
			for_each_is_variant(&mut e.lower, f)?;
			for_each_is_variant(&mut e.upper, f)?;
		}
		Expression::Alias(AliasExpression {
			expression,
			..
		})
		| Expression::Cast(CastExpression {
			expression,
			..
		})
		| Expression::Prefix(PrefixExpression {
			expression,
			..
		}) => {
			for_each_is_variant(expression, f)?;
		}
		Expression::FieldAccess(e) => {
			for_each_is_variant(&mut e.object, f)?;
		}
		Expression::Call(CallExpression {
			args: expressions,
			..
		})
		| Expression::Tuple(TupleExpression {
			expressions,
			..
		})
		| Expression::List(ListExpression {
			expressions,
			..
		})
		| Expression::Map(MapExpression {
			expressions,
			..
		})
		| Expression::Extend(ExtendExpression {
			expressions,
			..
		}) => {
			for expression in expressions {
				for_each_is_variant(expression, f)?;
			}
		}
		Expression::SumTypeConstructor(e) => {
			for (_, expression) in &mut e.columns {
				for_each_is_variant(expression, f)?;
			}
		}
		Expression::If(e) => {
			for_each_is_variant(&mut e.condition, f)?;
			for_each_is_variant(&mut e.then_expr, f)?;
			for else_if in &mut e.else_ifs {
				for_each_is_variant(&mut else_if.condition, f)?;
				for_each_is_variant(&mut else_if.then_expr, f)?;
			}
			if let Some(else_expr) = &mut e.else_expr {
				for_each_is_variant(else_expr, f)?;
			}
		}
		Expression::AccessSource(_)
		| Expression::Constant(_)
		| Expression::Column(_)
		| Expression::Type(_)
		| Expression::Parameter(_)
		| Expression::Variable(_) => {}
	}
	Ok(())
}

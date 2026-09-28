// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod fragment;
pub mod name;

use std::{
	fmt,
	fmt::{Display, Formatter},
	sync::Arc,
};

use reifydb_value::{fragment::Fragment, value::value_type::ValueType};
use serde::{Deserialize, Serialize};

use crate::interface::identifier::{ColumnIdentifier, ColumnObject};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AliasExpression {
	pub alias: IdentExpression,
	pub expression: Box<Expression>,
	pub fragment: Fragment,
}

impl Display for AliasExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		Display::fmt(&self.alias, f)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
	AccessSource(AccessObjectExpression),

	Alias(AliasExpression),

	Cast(CastExpression),

	Constant(ConstantExpression),

	Column(ColumnExpression),

	Add(AddExpression),

	Div(DivExpression),

	Call(CallExpression),

	Rem(RemExpression),

	Mul(MulExpression),

	Sub(SubExpression),

	Tuple(TupleExpression),

	List(ListExpression),

	Prefix(PrefixExpression),

	GreaterThan(GreaterThanExpression),

	GreaterThanEqual(GreaterThanEqExpression),

	LessThan(LessThanExpression),

	LessThanEqual(LessThanEqExpression),

	Equal(EqExpression),

	NotEqual(NotEqExpression),

	Between(BetweenExpression),

	And(AndExpression),

	Or(OrExpression),

	Xor(XorExpression),

	In(InExpression),

	Contains(ContainsExpression),

	Type(TypeExpression),

	Parameter(ParameterExpression),
	Variable(VariableExpression),

	If(IfExpression),
	Map(MapExpression),
	Extend(ExtendExpression),
	SumTypeConstructor(SumTypeConstructorExpression),
	IsVariant(IsVariantExpression),
	FieldAccess(FieldAccessExpression),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessObjectExpression {
	pub column: ColumnIdentifier,
}

impl AccessObjectExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		match &self.column.object {
			ColumnObject::Qualified {
				name,
				..
			} => Fragment::merge_all([name.clone(), self.column.name.clone()]),
			ColumnObject::Alias(alias) => Fragment::merge_all([alias.clone(), self.column.name.clone()]),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstantExpression {
	None {
		fragment: Fragment,
	},
	Bool {
		fragment: Fragment,
	},

	Number {
		fragment: Fragment,
	},

	Text {
		fragment: Fragment,
	},

	Temporal {
		fragment: Fragment,
	},

	Duration {
		fragment: Fragment,
	},
}

impl Display for ConstantExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			ConstantExpression::None {
				..
			} => write!(f, "none"),
			ConstantExpression::Bool {
				fragment,
			} => write!(f, "{}", fragment.text()),
			ConstantExpression::Number {
				fragment,
			} => write!(f, "{}", fragment.text()),
			ConstantExpression::Text {
				fragment,
			} => write!(f, "\"{}\"", fragment.text()),
			ConstantExpression::Temporal {
				fragment,
			} => write!(f, "{}", fragment.text()),
			ConstantExpression::Duration {
				fragment,
			} => write!(f, "{}", fragment.text()),
		}
	}
}

impl ConstantExpression {
	pub fn infer_type(&self) -> ValueType {
		match self {
			ConstantExpression::None {
				..
			} => ValueType::Any,
			ConstantExpression::Bool {
				..
			} => ValueType::Boolean,
			ConstantExpression::Number {
				..
			} => ValueType::Int4,
			ConstantExpression::Text {
				..
			} => ValueType::Utf8,
			ConstantExpression::Temporal {
				..
			} => ValueType::DateTime,
			ConstantExpression::Duration {
				..
			} => ValueType::Duration,
		}
	}
}

impl Expression {
	pub fn infer_type(&self) -> Option<ValueType> {
		match self {
			Expression::Constant(c) => Some(c.infer_type()),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CastExpression {
	pub fragment: Fragment,
	pub expression: Box<Expression>,
	pub to: TypeExpression,
}

impl CastExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.fragment.clone(),
			self.expression.full_fragment_owned(),
			self.to.full_fragment_owned(),
		])
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment {
		move || self.full_fragment_owned()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypeExpression {
	pub fragment: Fragment,
	pub ty: ValueType,
}

impl TypeExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		self.fragment.clone()
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment {
		move || self.full_fragment_owned()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AddExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DivExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MulExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GreaterThanExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl GreaterThanExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GreaterThanEqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl GreaterThanEqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LessThanExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl LessThanExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LessThanEqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl LessThanEqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl EqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotEqExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl NotEqExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BetweenExpression {
	pub value: Box<Expression>,
	pub lower: Box<Expression>,
	pub upper: Box<Expression>,
	pub fragment: Fragment,
}

impl BetweenExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.value.full_fragment_owned(),
			self.fragment.clone(),
			self.lower.full_fragment_owned(),
			self.upper.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AndExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl AndExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl OrExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct XorExpression {
	pub left: Box<Expression>,
	pub right: Box<Expression>,
	pub fragment: Fragment,
}

impl XorExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.left.full_fragment_owned(),
			self.fragment.clone(),
			self.right.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InExpression {
	pub value: Box<Expression>,
	pub list: Box<Expression>,
	pub negated: bool,
	pub fragment: Fragment,
}

impl InExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.value.full_fragment_owned(),
			self.fragment.clone(),
			self.list.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContainsExpression {
	pub value: Box<Expression>,
	pub list: Box<Expression>,
	pub fragment: Fragment,
}

impl ContainsExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([
			self.value.full_fragment_owned(),
			self.fragment.clone(),
			self.list.full_fragment_owned(),
		])
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnExpression(pub ColumnIdentifier);

impl ColumnExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		self.0.name.clone()
	}

	pub fn column(&self) -> &ColumnIdentifier {
		&self.0
	}
}

impl Display for Expression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Expression::AccessSource(AccessObjectExpression {
				column,
			}) => match &column.object {
				ColumnObject::Qualified {
					name,
					..
				} => {
					write!(f, "{}.{}", name.text(), column.name.text())
				}
				ColumnObject::Alias(alias) => {
					write!(f, "{}.{}", alias.text(), column.name.text())
				}
			},
			Expression::Alias(AliasExpression {
				alias,
				expression,
				..
			}) => {
				write!(f, "{} as {}", expression, alias)
			}
			Expression::Cast(CastExpression {
				expression: expr,
				..
			}) => write!(f, "{}", expr),
			Expression::Constant(fragment) => {
				write!(f, "Constant({})", fragment)
			}
			Expression::Column(ColumnExpression(column)) => {
				write!(f, "{}", column.name.text())
			}
			Expression::Add(AddExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} + {})", left, right)
			}
			Expression::Div(DivExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} / {})", left, right)
			}
			Expression::Call(call) => write!(f, "{}", call),
			Expression::Rem(RemExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} % {})", left, right)
			}
			Expression::Mul(MulExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} * {})", left, right)
			}
			Expression::Sub(SubExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} - {})", left, right)
			}
			Expression::Tuple(tuple) => write!(f, "({})", tuple),
			Expression::List(list) => write!(f, "{}", list),
			Expression::Prefix(prefix) => write!(f, "{}", prefix),
			Expression::GreaterThan(GreaterThanExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} > {})", left, right)
			}
			Expression::GreaterThanEqual(GreaterThanEqExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} >= {})", left, right)
			}
			Expression::LessThan(LessThanExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} < {})", left, right)
			}
			Expression::LessThanEqual(LessThanEqExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} <= {})", left, right)
			}
			Expression::Equal(EqExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} == {})", left, right)
			}
			Expression::NotEqual(NotEqExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} != {})", left, right)
			}
			Expression::Between(BetweenExpression {
				value,
				lower,
				upper,
				..
			}) => {
				write!(f, "({} BETWEEN {} AND {})", value, lower, upper)
			}
			Expression::And(AndExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} and {})", left, right)
			}
			Expression::Or(OrExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} or {})", left, right)
			}
			Expression::Xor(XorExpression {
				left,
				right,
				..
			}) => {
				write!(f, "({} xor {})", left, right)
			}
			Expression::In(InExpression {
				value,
				list,
				negated,
				..
			}) => {
				if *negated {
					write!(f, "({} NOT IN {})", value, list)
				} else {
					write!(f, "({} IN {})", value, list)
				}
			}
			Expression::Contains(ContainsExpression {
				value,
				list,
				..
			}) => {
				write!(f, "({} CONTAINS {})", value, list)
			}
			Expression::Type(TypeExpression {
				fragment,
				..
			}) => write!(f, "{}", fragment.text()),
			Expression::Parameter(param) => match param {
				ParameterExpression::Positional {
					fragment,
					..
				} => write!(f, "{}", fragment.text()),
				ParameterExpression::Named {
					fragment,
				} => write!(f, "{}", fragment.text()),
			},
			Expression::Variable(var) => write!(f, "{}", var.fragment.text()),
			Expression::If(if_expr) => write!(f, "{}", if_expr),
			Expression::Map(map_expr) => write!(
				f,
				"MAP{{ {} }}",
				map_expr.expressions
					.iter()
					.map(|expr| format!("{}", expr))
					.collect::<Vec<_>>()
					.join(", ")
			),
			Expression::Extend(extend_expr) => write!(
				f,
				"EXTEND{{ {} }}",
				extend_expr
					.expressions
					.iter()
					.map(|expr| format!("{}", expr))
					.collect::<Vec<_>>()
					.join(", ")
			),
			Expression::SumTypeConstructor(ctor) => write!(
				f,
				"{}::{}{{ {} }}",
				ctor.sumtype_name.text(),
				ctor.variant_name.text(),
				ctor.columns
					.iter()
					.map(|(name, expr)| format!("{}: {}", name.text(), expr))
					.collect::<Vec<_>>()
					.join(", ")
			),
			Expression::IsVariant(e) => write!(f, "({} IS {})", e.expression, e.variant_name.text()),
			Expression::FieldAccess(field_access) => write!(f, "{}", field_access),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CallExpression {
	pub func: IdentExpression,
	pub args: Vec<Expression>,
	pub fragment: Fragment,
}

impl CallExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::Statement {
			column: self.func.0.column(),
			line: self.func.0.line(),
			text: Arc::from(format!(
				"{}({})",
				self.func.0.text(),
				self.args
					.iter()
					.map(|arg| arg.full_fragment_owned().text().to_string())
					.collect::<Vec<_>>()
					.join(",")
			)),
		}
	}
}

impl Display for CallExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let args = self.args.iter().map(|arg| format!("{}", arg)).collect::<Vec<_>>().join(", ");
		write!(f, "{}({})", self.func, args)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdentExpression(pub Fragment);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParameterExpression {
	Positional {
		fragment: Fragment,
	},
	Named {
		fragment: Fragment,
	},
}

impl ParameterExpression {
	pub fn position(&self) -> Option<u32> {
		match self {
			ParameterExpression::Positional {
				fragment,
			} => fragment.text()[1..].parse().ok(),
			ParameterExpression::Named {
				..
			} => None,
		}
	}

	pub fn name(&self) -> Option<&str> {
		match self {
			ParameterExpression::Named {
				fragment,
			} => Some(&fragment.text()[1..]),
			ParameterExpression::Positional {
				..
			} => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VariableExpression {
	pub fragment: Fragment,
}

impl VariableExpression {
	pub fn name(&self) -> &str {
		let text = self.fragment.text();
		text.strip_prefix('$').unwrap_or(text)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IfExpression {
	pub condition: Box<Expression>,
	pub then_expr: Box<Expression>,
	pub else_ifs: Vec<ElseIfExpression>,
	pub else_expr: Option<Box<Expression>>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ElseIfExpression {
	pub condition: Box<Expression>,
	pub then_expr: Box<Expression>,
	pub fragment: Fragment,
}

impl IfExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		self.fragment.clone()
	}

	pub fn lazy_fragment(&self) -> impl Fn() -> Fragment {
		move || self.full_fragment_owned()
	}
}

impl Display for IfExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "if {} {{ {} }}", self.condition, self.then_expr)?;

		for else_if in &self.else_ifs {
			write!(f, " else if {} {{ {} }}", else_if.condition, else_if.then_expr)?;
		}

		if let Some(else_expr) = &self.else_expr {
			write!(f, " else {{ {} }}", else_expr)?;
		}

		Ok(())
	}
}

impl IdentExpression {
	pub fn name(&self) -> &str {
		self.0.text()
	}
}

impl Display for IdentExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0.text())
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PrefixOperator {
	Minus(Fragment),
	Plus(Fragment),
	Not(Fragment),
}

impl PrefixOperator {
	pub fn full_fragment_owned(&self) -> Fragment {
		match self {
			PrefixOperator::Minus(fragment) => fragment.clone(),
			PrefixOperator::Plus(fragment) => fragment.clone(),
			PrefixOperator::Not(fragment) => fragment.clone(),
		}
	}
}

impl Display for PrefixOperator {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			PrefixOperator::Minus(_) => write!(f, "-"),
			PrefixOperator::Plus(_) => write!(f, "+"),
			PrefixOperator::Not(_) => write!(f, "not"),
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrefixExpression {
	pub operator: PrefixOperator,
	pub expression: Box<Expression>,
	pub fragment: Fragment,
}

impl PrefixExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([self.operator.full_fragment_owned(), self.expression.full_fragment_owned()])
	}
}

impl Display for PrefixExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "({}{})", self.operator, self.expression)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TupleExpression {
	pub expressions: Vec<Expression>,
	pub fragment: Fragment,
}

impl Display for TupleExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let items = self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
		write!(f, "({})", items)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ListExpression {
	pub expressions: Vec<Expression>,
	pub fragment: Fragment,
}

impl Display for ListExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		let items = self.expressions.iter().map(|e| format!("{}", e)).collect::<Vec<_>>().join(", ");
		write!(f, "[{}]", items)
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MapExpression {
	pub expressions: Vec<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExtendExpression {
	pub expressions: Vec<Expression>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SumTypeConstructorExpression {
	pub namespace: Fragment,
	pub sumtype_name: Fragment,
	pub variant_name: Fragment,
	pub columns: Vec<(Fragment, Expression)>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IsVariantExpression {
	pub expression: Box<Expression>,
	pub namespace: Option<Fragment>,
	pub sumtype_name: Fragment,
	pub variant_name: Fragment,
	pub tag: Option<u8>,
	pub fragment: Fragment,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldAccessExpression {
	pub object: Box<Expression>,
	pub field: Fragment,
	pub fragment: Fragment,
}

impl FieldAccessExpression {
	pub fn full_fragment_owned(&self) -> Fragment {
		Fragment::merge_all([self.object.full_fragment_owned(), self.fragment.clone(), self.field.clone()])
	}
}

impl Display for FieldAccessExpression {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}.{}", self.object, self.field.text())
	}
}

pub fn extract_variable_names(expr: &Expression) -> Vec<String> {
	let mut names = Vec::new();
	collect_variable_names(expr, &mut names);
	names
}

fn collect_variable_names(expr: &Expression, names: &mut Vec<String>) {
	match expr {
		Expression::Variable(v) => {
			let name = v.name().to_string();
			if !names.contains(&name) {
				names.push(name);
			}
		}
		Expression::Add(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Sub(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Mul(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Div(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Rem(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::GreaterThan(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::GreaterThanEqual(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::LessThan(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::LessThanEqual(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Equal(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::NotEqual(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::And(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Or(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Xor(e) => {
			collect_variable_names(&e.left, names);
			collect_variable_names(&e.right, names);
		}
		Expression::Between(e) => {
			collect_variable_names(&e.value, names);
			collect_variable_names(&e.lower, names);
			collect_variable_names(&e.upper, names);
		}
		Expression::In(e) => {
			collect_variable_names(&e.value, names);
			collect_variable_names(&e.list, names);
		}
		Expression::Contains(e) => {
			collect_variable_names(&e.value, names);
			collect_variable_names(&e.list, names);
		}
		Expression::Prefix(e) => collect_variable_names(&e.expression, names),
		Expression::Cast(e) => collect_variable_names(&e.expression, names),
		Expression::Alias(e) => collect_variable_names(&e.expression, names),
		Expression::Call(e) => {
			for arg in &e.args {
				collect_variable_names(arg, names);
			}
		}
		Expression::Tuple(e) => {
			for expr in &e.expressions {
				collect_variable_names(expr, names);
			}
		}
		Expression::List(e) => {
			for expr in &e.expressions {
				collect_variable_names(expr, names);
			}
		}
		Expression::If(e) => {
			collect_variable_names(&e.condition, names);
			collect_variable_names(&e.then_expr, names);
			for else_if in &e.else_ifs {
				collect_variable_names(&else_if.condition, names);
				collect_variable_names(&else_if.then_expr, names);
			}
			if let Some(else_expr) = &e.else_expr {
				collect_variable_names(else_expr, names);
			}
		}
		Expression::Map(e) => {
			for expr in &e.expressions {
				collect_variable_names(expr, names);
			}
		}
		Expression::Extend(e) => {
			for expr in &e.expressions {
				collect_variable_names(expr, names);
			}
		}
		Expression::SumTypeConstructor(e) => {
			for (_, expr) in &e.columns {
				collect_variable_names(expr, names);
			}
		}
		Expression::IsVariant(e) => collect_variable_names(&e.expression, names),
		Expression::FieldAccess(e) => collect_variable_names(&e.object, names),
		Expression::Constant(_)
		| Expression::Column(_)
		| Expression::AccessSource(_)
		| Expression::Parameter(_)
		| Expression::Type(_) => {}
	}
}

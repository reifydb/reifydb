// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashSet;

use crate::{Error, ast::*};

pub fn emit(stmt: &Statement) -> Result<String, Error> {
	match stmt {
		Statement::Select(sel) => emit_select_top(sel),
		Statement::Insert(ins) => emit_insert(ins),
		Statement::Update(upd) => emit_update(upd),
		Statement::Delete(del) => emit_delete(del),
		Statement::CreateTable(ct) => emit_create_table(ct),
	}
}

// ── SELECT → RQL pipeline ───────────────────────────────────────────────

fn emit_select_top(sel: &SelectStatement) -> Result<String, Error> {
	let mut cte_names = HashSet::new();
	let mut parts = Vec::new();

	// Emit each CTE as a LET $name = <pipeline>
	for cte in &sel.ctes {
		let name_lower = cte.name.to_ascii_lowercase();
		let pipeline = emit_select_inner(&cte.query, &cte_names)?;
		parts.push(format!("LET ${name_lower} = {pipeline}"));
		cte_names.insert(name_lower);
	}

	// Emit the main SELECT
	parts.push(emit_select_inner(sel, &cte_names)?);

	Ok(parts.join("; "))
}

fn emit_select(sel: &SelectStatement) -> Result<String, Error> {
	emit_select_inner(sel, &HashSet::new())
}

fn emit_select_inner(sel: &SelectStatement, cte_names: &HashSet<String>) -> Result<String, Error> {
	// If there is no FROM clause, this is a computed-only SELECT
	if sel.from.is_none() {
		return emit_select_no_from(sel);
	}

	let mut parts = Vec::new();

	// FROM
	parts.push(emit_from_clause(sel.from.as_ref().unwrap(), cte_names)?);

	// JOINs
	for join in &sel.joins {
		parts.push(emit_join(join, cte_names)?);
	}

	// FILTER (WHERE)
	if let Some(ref where_clause) = sel.where_clause {
		parts.push(format!("FILTER {{{}}}", emit_expr(where_clause)?));
	}

	// AGGREGATE (GROUP BY with aggregate functions)
	let has_aggregates = has_aggregate_functions(&sel.columns);
	if has_aggregates || !sel.group_by.is_empty() {
		let agg_exprs = collect_aggregate_columns(&sel.columns)?;
		let by_exprs = emit_expr_comma_list(&sel.group_by)?;

		if !agg_exprs.is_empty() {
			let mut agg_str = format!("AGGREGATE {{{agg_exprs}}}");
			if !sel.group_by.is_empty() {
				agg_str.push_str(&format!(" BY {{{by_exprs}}}"));
			}
			parts.push(agg_str);
		}

		// HAVING
		if let Some(ref having) = sel.having {
			parts.push(format!("FILTER {{{}}}", emit_expr(having)?));
		}

		// MAP only non-aggregate columns that aren't already in GROUP BY
		let map_exprs = collect_non_aggregate_map_columns(sel)?;
		if !map_exprs.is_empty() {
			parts.push(format!("MAP {{{map_exprs}}}"));
		}
	} else {
		// DISTINCT
		if sel.distinct {
			let cols = emit_select_columns_plain(&sel.columns)?;
			parts.push(format!("DISTINCT {{{cols}}}"));
		}
		// MAP (column projection) — only if not SELECT *
		else if !is_all_columns(&sel.columns) {
			let cols = emit_select_columns(&sel.columns)?;
			parts.push(format!("MAP {{{cols}}}"));
		}
	}

	// SORT (ORDER BY)
	if !sel.order_by.is_empty() {
		let sort = emit_order_by(&sel.order_by)?;
		parts.push(format!("SORT {{{sort}}}"));
	}

	// TAKE (LIMIT)
	if let Some(limit) = sel.limit {
		parts.push(format!("TAKE {limit}"));
	}

	// OFFSET
	if let Some(offset) = sel.offset {
		parts.push(format!("OFFSET {offset}"));
	}

	Ok(parts.join(" "))
}

fn emit_select_no_from(sel: &SelectStatement) -> Result<String, Error> {
	let cols = emit_select_columns(&sel.columns)?;
	Ok(format!("MAP {{{cols}}}"))
}

fn emit_from_clause(from: &FromClause, cte_names: &HashSet<String>) -> Result<String, Error> {
	match from {
		FromClause::Table {
			name,
			schema,
		} => {
			if schema.is_none() && cte_names.contains(&name.to_ascii_lowercase()) {
				return Ok(format!("FROM ${}", name.to_ascii_lowercase()));
			}
			if let Some(schema) = schema {
				Ok(format!("FROM {schema}.{name}"))
			} else {
				Ok(format!("FROM {name}"))
			}
		}
		FromClause::Subquery(sel) => {
			let inner = emit_select(sel)?;
			Ok(format!("FROM {{{inner}}}"))
		}
	}
}

fn emit_join(join: &JoinClause, cte_names: &HashSet<String>) -> Result<String, Error> {
	let join_kw = match join.join_type {
		JoinType::Inner => "JOIN",
		JoinType::Left => "LEFT JOIN",
	};

	let table_name = match &join.table {
		FromClause::Table {
			name,
			schema,
		} => {
			if schema.is_none() && cte_names.contains(&name.to_ascii_lowercase()) {
				format!("${}", name.to_ascii_lowercase())
			} else if let Some(schema) = schema {
				format!("{schema}.{name}")
			} else {
				name.clone()
			}
		}
		FromClause::Subquery(sel) => emit_select(sel)?,
	};

	let alias = join
		.table_alias
		.as_deref()
		.or(match &join.table {
			FromClause::Table {
				name,
				..
			} => Some(name.as_str()),
			_ => None,
		})
		.unwrap_or("_");

	// Extract USING columns from the ON condition
	let using = emit_join_using(&join.on, alias)?;

	Ok(format!("{join_kw} {{FROM {table_name}}} AS {alias} USING ({using})"))
}

/// Convert a JOIN ON condition like `t1.a = t2.b` to USING `(a, t2.b)` format.
fn emit_join_using(on_expr: &Expr, right_alias: &str) -> Result<String, Error> {
	match on_expr {
		Expr::BinaryOp {
			left,
			op: BinaryOp::Eq,
			right,
		} => {
			let (left_col, right_col) = extract_join_columns(left, right, right_alias)?;
			Ok(format!("{left_col}, {right_col}"))
		}
		Expr::BinaryOp {
			left,
			op: BinaryOp::And,
			right,
		} => {
			let l = emit_join_using(left, right_alias)?;
			let r = emit_join_using(right, right_alias)?;
			Ok(format!("{l}, {r}"))
		}
		_ => {
			// Fallback: emit as a filter-like expression
			emit_expr(on_expr)
		}
	}
}

fn extract_join_columns(left: &Expr, right: &Expr, right_alias: &str) -> Result<(String, String), Error> {
	let left_col = match left {
		Expr::QualifiedIdentifier(_table, col) => col.clone(),
		Expr::Identifier(col) => col.clone(),
		_ => emit_expr(left)?,
	};
	let right_col = match right {
		Expr::QualifiedIdentifier(table, col) => format!("{table}.{col}"),
		Expr::Identifier(col) => format!("{right_alias}.{col}"),
		_ => emit_expr(right)?,
	};
	Ok((left_col, right_col))
}

// ── INSERT → RQL ────────────────────────────────────────────────────────

fn emit_insert(ins: &InsertStatement) -> Result<String, Error> {
	let table = if let Some(ref schema) = ins.schema {
		format!("{schema}.{}", ins.table)
	} else {
		ins.table.clone()
	};

	let mut rows = Vec::new();
	for row_values in &ins.values {
		if ins.columns.is_empty() {
			// No column names — emit positional tuple
			let vals: Result<Vec<_>, _> = row_values.iter().map(emit_expr).collect();
			rows.push(format!("({})", vals?.join(", ")));
		} else {
			// Named columns — emit record
			let mut fields = Vec::new();
			for (i, val) in row_values.iter().enumerate() {
				let col_name = if i < ins.columns.len() {
					&ins.columns[i]
				} else {
					return Err(Error("more values than columns in INSERT".into()));
				};
				fields.push(format!("{}: {}", col_name, emit_expr(val)?));
			}
			rows.push(format!("{{{}}}", fields.join(", ")));
		}
	}

	Ok(format!("INSERT {} [{}]", table, rows.join(", ")))
}

// ── UPDATE → RQL ────────────────────────────────────────────────────────

fn emit_update(upd: &UpdateStatement) -> Result<String, Error> {
	let table = if let Some(ref schema) = upd.schema {
		format!("{schema}.{}", upd.table)
	} else {
		upd.table.clone()
	};

	let mut assignments = Vec::new();
	for (col, val) in &upd.assignments {
		assignments.push(format!("{}: {}", col, emit_expr(val)?));
	}

	let mut result = format!("UPDATE {} {{{}}}", table, assignments.join(", "));

	if let Some(ref where_clause) = upd.where_clause {
		result.push_str(&format!(" FILTER {{{}}}", emit_expr(where_clause)?));
	}

	Ok(result)
}

// ── DELETE → RQL ────────────────────────────────────────────────────────

fn emit_delete(del: &DeleteStatement) -> Result<String, Error> {
	let table = if let Some(ref schema) = del.schema {
		format!("{schema}.{}", del.table)
	} else {
		del.table.clone()
	};

	let mut result = format!("DELETE {table}");

	if let Some(ref where_clause) = del.where_clause {
		result.push_str(&format!(" FILTER {{{}}}", emit_expr(where_clause)?));
	}

	Ok(result)
}

// ── CREATE TABLE → RQL ──────────────────────────────────────────────────

fn emit_create_table(ct: &CreateTableStatement) -> Result<String, Error> {
	let table = if let Some(ref schema) = ct.schema {
		format!("{schema}.{}", ct.table)
	} else {
		ct.table.clone()
	};

	let mut cols = Vec::new();
	for col in &ct.columns {
		cols.push(format!("{}: {}", col.name, emit_rql_type(&col.data_type)));
	}

	Ok(format!("CREATE TABLE {} {{{}}}", table, cols.join(", ")))
}

fn emit_rql_type(ty: &SqlType) -> &'static str {
	match ty {
		SqlType::Int | SqlType::Int4 | SqlType::Integer => "int4",
		SqlType::Int2 | SqlType::Smallint => "int2",
		SqlType::Int8 | SqlType::Bigint => "int8",
		SqlType::Float4 | SqlType::Real => "float4",
		SqlType::Float8 | SqlType::Double => "float8",
		SqlType::Boolean | SqlType::Bool => "bool",
		SqlType::Varchar(_) | SqlType::Char(_) | SqlType::Text | SqlType::Utf8 => "utf8",
		SqlType::Blob => "blob",
	}
}

// ── Expression emitter ──────────────────────────────────────────────────

fn emit_expr(expr: &Expr) -> Result<String, Error> {
	match expr {
		Expr::Identifier(name) => Ok(name.clone()),
		Expr::QualifiedIdentifier(table, col) => Ok(format!("{table}.{col}")),
		Expr::IntegerLiteral(n) => Ok(n.to_string()),
		Expr::FloatLiteral(f) => Ok(format_float(*f)),
		Expr::StringLiteral(s) => Ok(format!("'{s}'")),
		Expr::BoolLiteral(b) => Ok(if *b {
			"true"
		} else {
			"false"
		}
		.into()),
		Expr::Null => Ok("none".into()),
		Expr::BinaryOp {
			left,
			op,
			right,
		} => {
			let l = emit_expr(left)?;
			let r = emit_expr(right)?;
			let op_str = match op {
				BinaryOp::Eq => "==",
				BinaryOp::NotEq => "!=",
				BinaryOp::Lt => "<",
				BinaryOp::Gt => ">",
				BinaryOp::LtEq => "<=",
				BinaryOp::GtEq => ">=",
				BinaryOp::And => "and",
				BinaryOp::Or => "or",
				BinaryOp::Add => "+",
				BinaryOp::Sub => "-",
				BinaryOp::Mul => "*",
				BinaryOp::Div => "/",
				BinaryOp::Mod => "%",
			};
			Ok(format!("{l} {op_str} {r}"))
		}
		Expr::UnaryOp {
			op,
			expr,
		} => {
			let e = emit_expr(expr)?;
			match op {
				UnaryOp::Not => Ok(format!("not {e}")),
				UnaryOp::Neg => Ok(format!("-{e}")),
			}
		}
		Expr::FunctionCall {
			name,
			args,
		} => {
			let func_name = sql_to_rql_function(name)?;
			let arg_strs: Result<Vec<_>, _> = args.iter().map(emit_expr).collect();
			let args_str = arg_strs?.join(", ");
			Ok(format!("{func_name}({args_str})"))
		}
		Expr::Between {
			expr,
			low,
			high,
			negated,
		} => {
			let e = emit_expr(expr)?;
			let l = emit_expr(low)?;
			let h = emit_expr(high)?;
			if *negated {
				Ok(format!("not ({e} between {l} and {h})"))
			} else {
				Ok(format!("{e} between {l} and {h}"))
			}
		}
		Expr::InList {
			expr,
			list,
			negated,
		} => {
			let e = emit_expr(expr)?;
			let items: Result<Vec<_>, _> = list.iter().map(emit_expr).collect();
			let items_str = items?.join(", ");
			if *negated {
				Ok(format!("not ({e} in ({items_str}))"))
			} else {
				Ok(format!("{e} in ({items_str})"))
			}
		}
		Expr::IsNull {
			expr,
			negated,
		} => {
			let e = emit_expr(expr)?;
			if *negated {
				Ok(format!("{e} != none"))
			} else {
				Ok(format!("{e} == none"))
			}
		}
		Expr::Cast {
			expr,
			data_type,
		} => {
			let e = emit_expr(expr)?;
			let ty = emit_rql_type(data_type);
			Ok(format!("cast({e}, {ty})"))
		}
		Expr::Nested(inner) => {
			let e = emit_expr(inner)?;
			Ok(format!("({e})"))
		}
	}
}

fn format_float(f: f64) -> String {
	let s = f.to_string();
	if s.contains('.') {
		s
	} else {
		format!("{s}.0")
	}
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn is_all_columns(cols: &[SelectColumn]) -> bool {
	cols.len() == 1 && matches!(cols[0], SelectColumn::AllColumns)
}

fn has_aggregate_functions(cols: &[SelectColumn]) -> bool {
	cols.iter().any(|c| match c {
		SelectColumn::Expr {
			expr,
			..
		} => expr_has_aggregate(expr),
		_ => false,
	})
}

fn expr_has_aggregate(expr: &Expr) -> bool {
	match expr {
		Expr::FunctionCall {
			name,
			..
		} => {
			matches!(
				sql_to_rql_function(name),
				Ok("math::count" | "math::sum" | "math::avg" | "math::min" | "math::max")
			)
		}
		Expr::BinaryOp {
			left,
			right,
			..
		} => expr_has_aggregate(left) || expr_has_aggregate(right),
		Expr::UnaryOp {
			expr,
			..
		} => expr_has_aggregate(expr),
		Expr::Nested(inner) => expr_has_aggregate(inner),
		_ => false,
	}
}

fn collect_aggregate_columns(cols: &[SelectColumn]) -> Result<String, Error> {
	let mut agg_exprs = Vec::new();
	for col in cols {
		if let SelectColumn::Expr {
			expr,
			alias,
		} = col
		{
			if expr_has_aggregate(expr) {
				let e = emit_expr(expr)?;
				if let Some(alias) = alias {
					agg_exprs.push(format!("{alias}: {e}"));
				} else {
					agg_exprs.push(e);
				}
			}
		}
	}
	Ok(agg_exprs.join(", "))
}

fn collect_non_aggregate_map_columns(sel: &SelectStatement) -> Result<String, Error> {
	let mut map_exprs = Vec::new();
	for col in &sel.columns {
		if let SelectColumn::Expr {
			expr,
			alias,
		} = col
		{
			if !expr_has_aggregate(expr) {
				// Check if this column is already in GROUP BY
				let is_in_group_by = sel.group_by.iter().any(|gb| expr_eq(gb, expr));
				if !is_in_group_by {
					let e = emit_expr(expr)?;
					if let Some(alias) = alias {
						map_exprs.push(format!("{alias}: {e}"));
					} else {
						map_exprs.push(e);
					}
				}
			}
		}
	}
	Ok(map_exprs.join(", "))
}

fn emit_select_columns(cols: &[SelectColumn]) -> Result<String, Error> {
	let mut parts = Vec::new();
	for col in cols {
		match col {
			SelectColumn::AllColumns => parts.push("*".into()),
			SelectColumn::Expr {
				expr,
				alias,
			} => {
				let e = emit_expr(expr)?;
				if let Some(alias) = alias {
					parts.push(format!("{alias}: {e}"));
				} else {
					parts.push(e);
				}
			}
		}
	}
	Ok(parts.join(", "))
}

fn emit_select_columns_plain(cols: &[SelectColumn]) -> Result<String, Error> {
	let mut parts = Vec::new();
	for col in cols {
		match col {
			SelectColumn::AllColumns => parts.push("*".into()),
			SelectColumn::Expr {
				expr,
				..
			} => {
				parts.push(emit_expr(expr)?);
			}
		}
	}
	Ok(parts.join(", "))
}

fn sql_to_rql_function(name: &str) -> Result<&'static str, Error> {
	match name.to_uppercase().as_str() {
		// Aggregates
		"COUNT" => Ok("math::count"),
		"SUM" => Ok("math::sum"),
		"AVG" => Ok("math::avg"),
		"MIN" => Ok("math::min"),
		"MAX" => Ok("math::max"),
		// Math scalar
		"ABS" => Ok("math::abs"),
		"ACOS" => Ok("math::acos"),
		"ASIN" => Ok("math::asin"),
		"ATAN" => Ok("math::atan"),
		"ATAN2" => Ok("math::atan2"),
		"CEIL" | "CEILING" => Ok("math::ceil"),
		"COS" => Ok("math::cos"),
		"EXP" => Ok("math::exp"),
		"FLOOR" => Ok("math::floor"),
		"GCD" => Ok("math::gcd"),
		"LCM" => Ok("math::lcm"),
		"LOG" => Ok("math::log"),
		"LOG10" => Ok("math::log10"),
		"LOG2" => Ok("math::log2"),
		"MOD" => Ok("math::mod"),
		"PI" => Ok("math::pi"),
		"POWER" | "POW" => Ok("math::power"),
		"ROUND" => Ok("math::round"),
		"SIGN" => Ok("math::sign"),
		"SIN" => Ok("math::sin"),
		"SQRT" => Ok("math::sqrt"),
		"TAN" => Ok("math::tan"),
		"TRUNCATE" | "TRUNC" => Ok("math::truncate"),
		// Text
		"ASCII" => Ok("text::ascii"),
		"CHAR" | "CHR" => Ok("text::char"),
		"CONCAT" => Ok("text::concat"),
		"LENGTH" | "LEN" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => Ok("text::length"),
		"LOWER" | "LCASE" => Ok("text::lower"),
		"LPAD" => Ok("text::pad_left"),
		"REPEAT" => Ok("text::repeat"),
		"REPLACE" => Ok("text::replace"),
		"REVERSE" => Ok("text::reverse"),
		"RPAD" => Ok("text::pad_right"),
		"SUBSTRING" | "SUBSTR" => Ok("text::substring"),
		"TRIM" => Ok("text::trim"),
		"LTRIM" => Ok("text::trim_start"),
		"RTRIM" => Ok("text::trim_end"),
		"UPPER" | "UCASE" => Ok("text::upper"),
		_ => Err(Error(format!("no SQL-to-RQL mapping for function: {name}"))),
	}
}

fn emit_order_by(items: &[OrderByItem]) -> Result<String, Error> {
	let mut parts = Vec::new();
	for item in items {
		let e = emit_expr(&item.expr)?;
		match item.direction {
			OrderDirection::Asc => parts.push(format!("{e}:asc")),
			OrderDirection::Desc => parts.push(e),
		};
	}
	Ok(parts.join(", "))
}

fn emit_expr_comma_list(exprs: &[Expr]) -> Result<String, Error> {
	let parts: Result<Vec<_>, _> = exprs.iter().map(emit_expr).collect();
	Ok(parts?.join(", "))
}

/// Simple structural equality check for expressions (for GROUP BY dedup).
fn expr_eq(a: &Expr, b: &Expr) -> bool {
	match (a, b) {
		(Expr::Identifier(a), Expr::Identifier(b)) => a == b,
		(Expr::QualifiedIdentifier(at, ac), Expr::QualifiedIdentifier(bt, bc)) => at == bt && ac == bc,
		_ => false,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{parser::Parser, token::tokenize};

	fn transpile(sql: &str) -> String {
		let tokens = tokenize(sql).unwrap();
		let stmt = Parser::new(tokens).parse().unwrap();
		emit(&stmt).unwrap()
	}

	#[test]
	fn test_select_star() {
		assert_eq!(transpile("SELECT * FROM users"), "FROM users");
	}

	#[test]
	fn test_select_columns() {
		assert_eq!(transpile("SELECT id, name FROM users"), "FROM users MAP {id, name}");
	}

	#[test]
	fn test_select_alias() {
		assert_eq!(transpile("SELECT id, name AS username FROM users"), "FROM users MAP {id, username: name}");
	}

	#[test]
	fn test_where() {
		assert_eq!(transpile("SELECT * FROM users WHERE age > 18"), "FROM users FILTER {age > 18}");
	}

	#[test]
	fn test_order_by() {
		assert_eq!(transpile("SELECT * FROM users ORDER BY name ASC"), "FROM users SORT {name:asc}");
	}

	#[test]
	fn test_limit() {
		assert_eq!(transpile("SELECT * FROM users LIMIT 10"), "FROM users TAKE 10");
	}

	#[test]
	fn test_insert() {
		assert_eq!(
			transpile("INSERT INTO users (id, name) VALUES (1, 'Alice')"),
			"INSERT users [{id: 1, name: 'Alice'}]"
		);
	}

	#[test]
	fn test_update() {
		assert_eq!(
			transpile("UPDATE users SET name = 'Bob' WHERE id = 1"),
			"UPDATE users {name: 'Bob'} FILTER {id == 1}"
		);
	}

	#[test]
	fn test_delete() {
		assert_eq!(transpile("DELETE FROM users WHERE id = 1"), "DELETE users FILTER {id == 1}");
	}

	#[test]
	fn test_create_table() {
		assert_eq!(
			transpile("CREATE TABLE users (id INT, name TEXT, active BOOLEAN)"),
			"CREATE TABLE users {id: int4, name: utf8, active: bool}"
		);
	}

	#[test]
	fn test_null_to_none() {
		assert_eq!(transpile("SELECT NULL"), "MAP {none}");
	}

	#[test]
	fn test_not_equal() {
		assert_eq!(transpile("SELECT * FROM t WHERE a <> b"), "FROM t FILTER {a != b}");
	}

	#[test]
	fn test_and_or() {
		assert_eq!(transpile("SELECT * FROM t WHERE a = 1 AND b = 2"), "FROM t FILTER {a == 1 and b == 2}");
	}

	#[test]
	fn test_distinct() {
		assert_eq!(transpile("SELECT DISTINCT name FROM users"), "FROM users DISTINCT {name}");
	}

	#[test]
	fn test_group_by_aggregate() {
		assert_eq!(
			transpile("SELECT COUNT(id) FROM users GROUP BY dept"),
			"FROM users AGGREGATE {math::count(id)} BY {dept}"
		);
	}

	#[test]
	fn test_cast() {
		assert_eq!(transpile("SELECT CAST(x AS INT)"), "MAP {cast(x, int4)}");
	}

	#[test]
	fn test_between() {
		assert_eq!(transpile("SELECT * FROM t WHERE x BETWEEN 1 AND 10"), "FROM t FILTER {x between 1 and 10}");
	}

	#[test]
	fn test_in_list() {
		assert_eq!(transpile("SELECT * FROM t WHERE x IN (1, 2, 3)"), "FROM t FILTER {x in (1, 2, 3)}");
	}

	#[test]
	fn test_is_null() {
		assert_eq!(transpile("SELECT * FROM t WHERE x IS NULL"), "FROM t FILTER {x == none}");
	}

	#[test]
	fn test_is_not_null() {
		assert_eq!(transpile("SELECT * FROM t WHERE x IS NOT NULL"), "FROM t FILTER {x != none}");
	}

	#[test]
	fn test_join() {
		assert_eq!(
			transpile("SELECT * FROM t1 INNER JOIN t2 ON t1.a = t2.b"),
			"FROM t1 JOIN {FROM t2} AS t2 USING (a, t2.b)"
		);
	}

	#[test]
	fn test_left_join() {
		assert_eq!(
			transpile("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.b"),
			"FROM t1 LEFT JOIN {FROM t2} AS t2 USING (a, t2.b)"
		);
	}

	#[test]
	fn test_select_no_from() {
		assert_eq!(transpile("SELECT 1 + 2"), "MAP {1 + 2}");
	}

	#[test]
	fn test_multiple_insert_rows() {
		assert_eq!(
			transpile("INSERT INTO t (a, b) VALUES (1, 2), (3, 4)"),
			"INSERT t [{a: 1, b: 2}, {a: 3, b: 4}]"
		);
	}

	#[test]
	fn test_schema_qualified_table() {
		assert_eq!(transpile("SELECT * FROM test.users"), "FROM test.users");
	}

	#[test]
	fn test_simple_cte() {
		assert_eq!(
			transpile("WITH t AS (SELECT * FROM users WHERE active = true) SELECT * FROM t"),
			"LET $t = FROM users FILTER {active == true}; FROM $t"
		);
	}

	#[test]
	fn test_insert_without_columns() {
		assert_eq!(transpile("INSERT INTO t1 VALUES (1, 'true')"), "INSERT t1 [(1, 'true')]");
	}

	#[test]
	fn test_multiple_ctes() {
		assert_eq!(
			transpile("WITH a AS (SELECT * FROM users), b AS (SELECT id FROM a) SELECT * FROM b"),
			"LET $a = FROM users; LET $b = FROM $a MAP {id}; FROM $b"
		);
	}
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{error::Diagnostic, fragment::Fragment, value::value_type::ValueType};

pub fn namespace_not_found(fragment: Fragment, namespace: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_002".to_string(),
		rql: None,
		message: format!("namespace `{}` not found", namespace),
		fragment,
		label: Some("unknown namespace reference".to_string()),
		help: Some("make sure the namespace exists before using it or create it first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn view_already_exists(fragment: Fragment, namespace: &str, view: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_003".to_string(),
		rql: None,
		message: format!("view `{}::{}` already exists", namespace, view),
		fragment,
		label: Some("duplicate view definition".to_string()),
		help: Some("choose a different name, drop the existing view or create view in a different namespace"
			.to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn table_not_found(fragment: Fragment, namespace: &str, table: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_004".to_string(),
		rql: None,
		message: format!("table `{}::{}` not found", namespace, table),
		fragment,
		label: Some("unknown table reference".to_string()),
		help: Some("ensure the table exists or create it first using `CREATE TABLE`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn ringbuffer_not_found(fragment: Fragment, namespace: &str, ringbuffer: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_006".to_string(),
		rql: None,
		message: format!("ring buffer `{}::{}` not found", namespace, ringbuffer),
		fragment,
		label: Some("unknown ring buffer reference".to_string()),
		help: Some("ensure the ring buffer exists or create it first using `CREATE RING BUFFER`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn queue_deduplication_key_not_utf8(fragment: Fragment, actual: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_018".to_string(),
		rql: None,
		message: format!("INSERT WITH deduplication_key must evaluate to utf8, got {}", actual),
		column: None,
		fragment,
		label: Some("wrong deduplication_key type".to_string()),
		help: Some("Produce the key as text, e.g. deduplication_key: \"invoice-\" + order_id".to_string()),
		notes: vec![
			"A none deduplication_key is accepted and is a key like any other, matching how a declared deduplicate treats none"
				.to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn queue_not_before_not_datetime(fragment: Fragment, actual: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_019".to_string(),
		rql: None,
		message: format!("INSERT WITH not_before must evaluate to a datetime, got {}", actual),
		column: None,
		fragment,
		label: Some("wrong not_before type".to_string()),
		help: Some("Produce an instant, e.g. not_before: datetime::add(clock::now(), duration::minutes(5))"
			.to_string()),
		notes: vec!["A none not_before means the item is immediately due".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn queue_reserved_column_collision(fragment: Fragment, queue: &str, column: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_017".to_string(),
		rql: None,
		message: format!(
			"queue `{}` declares a column named `{}`, which INSERT ... WITH reserves",
			queue, column
		),
		column: None,
		fragment,
		label: Some("reserved column name".to_string()),
		help: Some(format!("Rename the `{}` column, or drop the WITH block from the INSERT", column)),
		notes: vec![
			"INSERT ... WITH desugars deduplication_key and not_before into hidden columns of these names"
				.to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn queue_not_found(fragment: Fragment, namespace: &str, queue: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_096".to_string(),
		rql: None,
		message: format!("queue `{}::{}` not found", namespace, queue),
		fragment,
		label: Some("unknown queue reference".to_string()),
		help: Some("ensure the queue exists or create it first using `CREATE QUEUE`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn sumtype_not_found(fragment: Fragment, namespace: &str, name: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_002".to_string(),
		rql: None,
		message: format!("type `{}::{}` not found", namespace, name),
		fragment,
		label: Some("unknown type".to_string()),
		help: Some(format!("create the enum first with `CREATE ENUM {}::{} {{ ... }}`", namespace, name)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn sumtype_variant_not_found(fragment: Fragment, sumtype: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_100".to_string(),
		rql: None,
		message: format!("`{}` is not a variant of `{}`", fragment.text(), sumtype),
		fragment,
		label: Some("unknown variant".to_string()),
		help: Some(format!("use a variant name or tag declared by `{}`", sumtype)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn column_not_sumtype(fragment: Fragment, column: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_101".to_string(),
		rql: None,
		message: format!("`{}` is a variant, but column `{}` is not an enum", fragment.text(), column),
		fragment,
		label: Some("variant for a column that is not an enum".to_string()),
		help: Some(format!("declare `{}` with an enum type, or write a value of its type", column)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn variant_in_expression(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "CA_102".to_string(),
		rql: None,
		message: format!("`{}` is an enum variant, which cannot be used inside an expression", fragment.text()),
		fragment,
		label: Some("enum variant inside an expression".to_string()),
		help: Some(
			"give the variant as a whole column value, or compare a column with it as `column == variant` in a filter"
				.to_string(),
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn variant_enum_not_known(fragment: Fragment, column: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_103".to_string(),
		rql: None,
		message: format!(
			"`{}` cannot be matched, because the enum of `{}` is not known here",
			fragment.text(),
			column
		),
		fragment,
		label: Some("enum not known here".to_string()),
		help: Some(format!(
			"match `{}` right after reading its table, view or ringbuffer, before steps such as extend, join or aggregate",
			column
		)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn dictionary_not_found(fragment: Fragment, namespace: &str, dictionary: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_007".to_string(),
		rql: None,
		message: format!("dictionary `{}::{}` not found", namespace, dictionary),
		fragment,
		label: Some("unknown dictionary reference".to_string()),
		help: Some("ensure the dictionary exists or create it first using `CREATE DICTIONARY`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn view_not_found(fragment: Fragment, namespace: &str, view: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_004".to_string(),
		rql: None,
		message: format!("view `{}::{}` not found", namespace, view),
		fragment,
		label: Some("unknown view reference".to_string()),
		help: Some("ensure the view exists or create it first using `CREATE VIEW`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn procedure_not_found(fragment: Fragment, namespace: &str, procedure: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_082".to_string(),
		rql: None,
		message: format!("procedure `{}::{}` not found", namespace, procedure),
		fragment,
		label: Some("unknown procedure reference".to_string()),
		help: Some("ensure the procedure exists or create it first using `CREATE PROCEDURE`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn primary_key_invalid_type(fragment: Fragment, column: &str, ty: ValueType) -> Diagnostic {
	Diagnostic {
		code: "CA_104".to_string(),
		rql: None,
		message: format!("primary key column `{}` has type `{}`, which a primary key cannot hold", column, ty),
		fragment,
		label: Some("unsupported primary key column type".to_string()),
		help: Some("primary key columns must be non-optional fixed-width types: bool, integers, floats, date, time, datetime, duration, uuid4, uuid7 or identity_id".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn virtual_table_not_found(namespace: &str, name: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_023".to_string(),
		rql: None,
		message: format!("virtual table `{}::{}` not found", namespace, name),
		fragment: Fragment::None,
		label: Some("unknown virtual table reference".to_string()),
		help: Some("ensure the virtual table is registered before using it".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn series_not_found(fragment: Fragment, namespace: &str, series: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_024".to_string(),
		rql: None,
		message: format!("series `{}.{}` not found", namespace, series),
		fragment,
		label: Some("unknown series reference".to_string()),
		help: Some("ensure the series exists or create it first using `CREATE SERIES`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn handler_not_found(fragment: Fragment, namespace: &str, handler: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_083".to_string(),
		rql: None,
		message: format!("handler `{}::{}` not found", namespace, handler),
		fragment,
		label: Some("unknown handler reference".to_string()),
		help: Some("ensure the handler exists or create it first using `CREATE HANDLER`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn test_not_found(fragment: Fragment, namespace: &str, test: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_084".to_string(),
		rql: None,
		message: format!("test `{}::{}` not found", namespace, test),
		fragment,
		label: Some("unknown test reference".to_string()),
		help: Some("ensure the test exists or create it first using `CREATE TEST`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn procedure_has_live_bindings(fragment: Fragment, namespace: &str, binding_names: &[String]) -> Diagnostic {
	let list = binding_names.join(", ");
	let procedure = fragment.text().to_string();
	Diagnostic {
		code: "CA_085".to_string(),
		rql: None,
		message: format!(
			"cannot drop procedure `{}::{}`: {} live binding(s): {}",
			namespace,
			procedure,
			binding_names.len(),
			list
		),
		fragment,
		label: Some("procedure has live bindings".to_string()),
		help: Some("drop the bindings first using `DROP BINDING` before dropping the procedure".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn update_partition_column_immutable(
	fragment: Fragment,
	column: &str,
	target_namespace: &str,
	target_name: &str,
	via_view: Option<(&str, &str)>,
) -> Diagnostic {
	let message = match via_view {
		Some((view_namespace, view_name)) => format!(
			"cannot update column `{}` on `{}::{}`: it is the partition key of downstream view `{}::{}`",
			column, target_namespace, target_name, view_namespace, view_name
		),
		None => format!(
			"cannot update column `{}` on `{}::{}`: partition columns are immutable",
			column, target_namespace, target_name
		),
	};
	Diagnostic {
		code: "PART_004".to_string(),
		rql: None,
		message,
		fragment,
		label: Some("partition column change rejected".to_string()),
		help: Some(
			"partition columns determine a row's physical location - directly, or transitively through a downstream partitioned view - and cannot be updated; delete and re-insert the row instead"
				.to_string(),
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

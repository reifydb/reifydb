// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::{error::Diagnostic, fragment::Fragment, value::r#type::Type};

pub fn namespace_already_exists(fragment: Fragment, namespace: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_001".to_string(),
		statement: None,
		message: format!("namespace `{}` already exists", namespace),
		fragment,
		label: Some("duplicate namespace definition".to_string()),
		help: Some("choose a different name or drop the existing namespace first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn namespace_not_found(fragment: Fragment, namespace: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_002".to_string(),
		statement: None,
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

pub fn table_already_exists(fragment: Fragment, namespace: &str, table: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_003".to_string(),
		statement: None,
		message: format!("table `{}::{}` already exists", namespace, table),
		fragment,
		label: Some("duplicate table definition".to_string()),
		help: Some("choose a different name, drop the existing table or create table in a different namespace"
			.to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_already_exists(fragment: Fragment, namespace: &str, flow: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_030".to_string(),
		statement: None,
		message: format!("flow `{}::{}` already exists", namespace, flow),
		fragment,
		label: Some("duplicate flow definition".to_string()),
		help: Some("choose a different name, drop the existing flow or create flow in a different namespace"
			.to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn flow_not_found(fragment: Fragment, namespace: &str, flow: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_031".to_string(),
		statement: None,
		message: format!("flow `{}::{}` not found", namespace, flow),
		fragment,
		label: Some("unknown flow reference".to_string()),
		help: Some("ensure the flow exists or create it first using `CREATE FLOW`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn view_already_exists(fragment: Fragment, namespace: &str, view: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_003".to_string(),
		statement: None,
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
		statement: None,
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

pub fn ringbuffer_already_exists(fragment: Fragment, namespace: &str, ringbuffer: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_005".to_string(),
		statement: None,
		message: format!("ring buffer `{}::{}` already exists", namespace, ringbuffer),
		fragment,
		label: Some("duplicate ring buffer definition".to_string()),
		help: Some(
			"choose a different name, drop the existing ring buffer or create ring buffer in a different namespace"
				.to_string(),
		),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn ringbuffer_not_found(fragment: Fragment, namespace: &str, ringbuffer: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_006".to_string(),
		statement: None,
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

pub fn sumtype_already_exists(fragment: Fragment, namespace: &str, name: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_003".to_string(),
		statement: None,
		message: format!("enum `{}::{}` already exists", namespace, name),
		fragment,
		label: Some("duplicate enum definition".to_string()),
		help: Some("choose a different name or drop the existing enum first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn sumtype_not_found(fragment: Fragment, namespace: &str, name: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_002".to_string(),
		statement: None,
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

pub fn dictionary_already_exists(fragment: Fragment, namespace: &str, dictionary: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_006".to_string(),
		statement: None,
		message: format!("dictionary `{}::{}` already exists", namespace, dictionary),
		fragment,
		label: Some("duplicate dictionary definition".to_string()),
		help: Some("choose a different name, drop the existing dictionary or create dictionary in a different namespace".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn dictionary_not_found(fragment: Fragment, namespace: &str, dictionary: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_007".to_string(),
		statement: None,
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

pub fn dictionary_type_mismatch(
	fragment: Fragment,
	column: &str,
	column_type: Type,
	dictionary: &str,
	dictionary_value_type: Type,
) -> Diagnostic {
	Diagnostic {
		code: "CA_008".to_string(),
		statement: None,
		message: format!(
			"column `{}` type `{}` does not match dictionary `{}` value type `{}`",
			column, column_type, dictionary, dictionary_value_type
		),
		fragment,
		label: Some("type mismatch".to_string()),
		help: Some(format!(
			"change the column type to `{}` to match the dictionary value type",
			dictionary_value_type
		)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn table_column_already_exists(fragment: Fragment, namespace: &str, table: &str, column: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_005".to_string(),
		statement: None,
		message: format!("column `{}` already exists in table `{}::{}`", column, namespace, table),
		fragment,
		label: Some("duplicate column definition".to_string()),
		help: Some("choose a different column name or drop the existing one first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn view_not_found(fragment: Fragment, namespace: &str, view: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_004".to_string(),
		statement: None,
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

pub fn view_column_already_exists(fragment: Fragment, namespace: &str, view: &str, column: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_005".to_string(),
		statement: None,
		message: format!("column `{}` already exists in view `{}::{}`", column, namespace, view),
		fragment,
		label: Some("duplicate column definition".to_string()),
		help: Some("choose a different column name or drop the existing one first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn auto_increment_invalid_type(fragment: Fragment, column: &str, ty: Type) -> Diagnostic {
	Diagnostic {
		code: "CA_006".to_string(),
		statement: None,
		message: format!("auto increment is not supported for type `{}`", ty),
		fragment,
		label: Some("invalid auto increment usage".to_string()),
		help: Some(format!(
			"auto increment is only supported for integer types (int1-16, uint1-16), column `{}` has type `{}`",
			column, ty
		)),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn table_column_property_already_exists(policy: &str, column: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_008".to_string(),
		statement: None,
		message: format!("policy `{policy:?}` already exists for column `{}`", column),
		fragment: Fragment::None,
		label: Some("duplicate column policy".to_string()),
		help: Some("remove the existing policy first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn index_variable_length_not_supported() -> Diagnostic {
	Diagnostic {
		code: "CA_009".to_string(),
		statement: None,
		message: "variable-length types (UTF8, BLOB) are not supported in indexes".to_string(),
		fragment: Fragment::None,
		label: Some("unsupported type for indexing".to_string()),
		help: Some("only fixed-size types can be indexed currently".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn index_types_directions_mismatch(types_len: usize, directions_len: usize) -> Diagnostic {
	Diagnostic {
		code: "CA_010".to_string(),
		statement: None,
		message: format!(
			"mismatch between number of types ({}) and directions ({})",
			types_len, directions_len
		),
		fragment: Fragment::None,
		label: Some("length mismatch".to_string()),
		help: Some("each indexed field must have a corresponding sort direction".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn namespace_already_pending_in_transaction(namespace_name: Fragment) -> Diagnostic {
	let fragment = namespace_name;
	let name = fragment.text();
	Diagnostic {
		code: "CA_011".to_string(),
		statement: None,
		message: format!("namespace `{}` already has pending changes in this transaction", name),
		fragment,
		label: Some("duplicate namespace modification in transaction".to_string()),
		help: Some("a namespace can only be created, updated, or deleted once per transaction".to_string()),
		column: None,
		notes: vec![
			"This usually indicates a programming error in transaction management".to_string(),
			"Consider reviewing the transaction logic for duplicate operations".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn table_already_pending_in_transaction(namespace_name: Fragment, table_name: Fragment) -> Diagnostic {
	let namespace_fragment = namespace_name;
	let table_fragment = table_name;
	let namespace = namespace_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
		code: "CA_012".to_string(),
		statement: None,
		message: format!("table `{}::{}` already has pending changes in this transaction", namespace, table),
		fragment: table_fragment,
		label: Some("duplicate table modification in transaction".to_string()),
		help: Some("a table can only be created, updated, or deleted once per transaction".to_string()),
		column: None,
		notes: vec![
			"This usually indicates a programming error in transaction management".to_string(),
			"Consider reviewing the transaction logic for duplicate operations".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn view_already_pending_in_transaction(namespace_name: Fragment, view_name: Fragment) -> Diagnostic {
	let namespace_fragment = namespace_name;
	let view_fragment = view_name;
	let namespace = namespace_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
		code: "CA_013".to_string(),
		statement: None,
		message: format!("view `{}::{}` already has pending changes in this transaction", namespace, view),
		fragment: view_fragment,
		label: Some("duplicate view modification in transaction".to_string()),
		help: Some("a view can only be created, updated, or deleted once per transaction".to_string()),
		column: None,
		notes: vec![
			"This usually indicates a programming error in transaction management".to_string(),
			"Consider reviewing the transaction logic for duplicate operations".to_string(),
		],
		cause: None,
		operator_chain: None,
	}
}

pub fn cannot_update_deleted_namespace(namespace_name: Fragment) -> Diagnostic {
	let fragment = namespace_name;
	let name = fragment.text();
	Diagnostic {
		code: "CA_014".to_string(),
		statement: None,
		message: format!("cannot update namespace `{}` as it is marked for deletion in this transaction", name),
		fragment,
		label: Some("attempted update on deleted namespace".to_string()),
		help: Some("remove the delete operation or skip the update".to_string()),
		column: None,
		notes: vec!["A namespace marked for deletion cannot be updated in the same transaction".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn cannot_update_deleted_table(namespace_name: Fragment, table_name: Fragment) -> Diagnostic {
	let namespace_fragment = namespace_name;
	let table_fragment = table_name;
	let namespace = namespace_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
		code: "CA_015".to_string(),
		statement: None,
		message: format!(
			"cannot update table `{}::{}` as it is marked for deletion in this transaction",
			namespace, table
		),
		fragment: table_fragment,
		label: Some("attempted update on deleted table".to_string()),
		help: Some("remove the delete operation or skip the update".to_string()),
		column: None,
		notes: vec!["A table marked for deletion cannot be updated in the same transaction".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn cannot_update_deleted_view(namespace_name: Fragment, view_name: Fragment) -> Diagnostic {
	let namespace_fragment = namespace_name;
	let view_fragment = view_name;
	let namespace = namespace_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
		code: "CA_016".to_string(),
		statement: None,
		message: format!(
			"cannot update view `{}::{}` as it is marked for deletion in this transaction",
			namespace, view
		),
		fragment: view_fragment,
		label: Some("attempted update on deleted view".to_string()),
		help: Some("remove the delete operation or skip the update".to_string()),
		column: None,
		notes: vec!["A view marked for deletion cannot be updated in the same transaction".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn cannot_delete_already_deleted_namespace(namespace_name: Fragment) -> Diagnostic {
	let fragment = namespace_name;
	let name = fragment.text();
	Diagnostic {
		code: "CA_017".to_string(),
		statement: None,
		message: format!("namespace `{}` is already marked for deletion in this transaction", name),
		fragment,
		label: Some("duplicate namespace deletion".to_string()),
		help: Some("remove the duplicate delete operation".to_string()),
		column: None,
		notes: vec!["A namespace can only be deleted once per transaction".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn cannot_delete_already_deleted_table(namespace_name: Fragment, table_name: Fragment) -> Diagnostic {
	let namespace_fragment = namespace_name;
	let table_fragment = table_name;
	let namespace = namespace_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
		code: "CA_018".to_string(),
		statement: None,
		message: format!("table `{}::{}` is already marked for deletion in this transaction", namespace, table),
		fragment: table_fragment,
		label: Some("duplicate table deletion".to_string()),
		help: Some("remove the duplicate delete operation".to_string()),
		column: None,
		notes: vec!["A table can only be deleted once per transaction".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn cannot_delete_already_deleted_view(namespace_name: Fragment, view_name: Fragment) -> Diagnostic {
	let namespace_fragment = namespace_name;
	let view_fragment = view_name;
	let namespace = namespace_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
		code: "CA_019".to_string(),
		statement: None,
		message: format!("view `{}::{}` is already marked for deletion in this transaction", namespace, view),
		fragment: view_fragment,
		label: Some("duplicate view deletion".to_string()),
		help: Some("remove the duplicate delete operation".to_string()),
		column: None,
		notes: vec!["A view can only be deleted once per transaction".to_string()],
		cause: None,
		operator_chain: None,
	}
}

pub fn primary_key_empty(fragment: Fragment) -> Diagnostic {
	Diagnostic {
		code: "CA_020".to_string(),
		statement: None,
		message: "primary key must contain at least one column".to_string(),
		fragment,
		label: Some("empty primary key definition".to_string()),
		help: Some("specify at least one column for the primary key".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn primary_key_column_not_found(fragment: Fragment, column_id: u64) -> Diagnostic {
	Diagnostic {
		code: "CA_021".to_string(),
		statement: None,
		message: format!("column with ID {} not found for primary key", column_id),
		fragment,
		label: Some("invalid column reference in primary key".to_string()),
		help: Some("ensure all columns referenced in the primary key exist in the table or view".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn virtual_table_already_exists(namespace: &str, name: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_022".to_string(),
		statement: None,
		message: format!("virtual table `{}::{}` already exists", namespace, name),
		fragment: Fragment::None,
		label: Some("duplicate virtual table definition".to_string()),
		help: Some("choose a different name or unregister the existing virtual table first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn virtual_table_not_found(namespace: &str, name: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_023".to_string(),
		statement: None,
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

pub fn subscription_already_exists(fragment: Fragment, subscription: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_010".to_string(),
		statement: None,
		message: format!("subscription `{}` already exists", subscription),
		fragment,
		label: Some("duplicate subscription definition".to_string()),
		help: Some("choose a different name or close the existing subscription first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn subscription_not_found(fragment: Fragment, subscription: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_011".to_string(),
		statement: None,
		message: format!("subscription `{}` not found", subscription),
		fragment,
		label: Some("unknown subscription reference".to_string()),
		help: Some("ensure the subscription exists or create it first using `CREATE SUBSCRIPTION`".to_string()),
		column: None,
		notes: vec![],
		cause: None,
		operator_chain: None,
	}
}

pub fn series_not_found(fragment: Fragment, namespace: &str, series: &str) -> Diagnostic {
	Diagnostic {
		code: "CA_024".to_string(),
		statement: None,
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

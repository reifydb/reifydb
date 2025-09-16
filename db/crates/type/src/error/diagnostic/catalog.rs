// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use crate::{
	OwnedFragment, error::diagnostic::Diagnostic, fragment::IntoFragment,
};

pub fn namespace_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_001".to_string(),
        statement: None,
        message: format!("namespace `{}` already exists", namespace),
        fragment,
        label: Some("duplicate namespace definition".to_string()),
        help: Some("choose a different name or drop the existing namespace first".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn namespace_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_002".to_string(),
        statement: None,
        message: format!("namespace `{}` not found", namespace),
        fragment,
        label: Some("undefined namespace reference".to_string()),
        help: Some("make sure the namespace exists before using it or create it first".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn table_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	table: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_003".to_string(),
        statement: None,
        message: format!("table `{}.{}` already exists", namespace, table),
        fragment,
        label: Some("duplicate table definition".to_string()),
        help: Some("choose a different name, drop the existing table or create table in a different namespace".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn view_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	view: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_003".to_string(),
        statement: None,
        message: format!("view `{}.{}` already exists", namespace, view),
        fragment,
        label: Some("duplicate view definition".to_string()),
        help: Some("choose a different name, drop the existing view or create view in a different namespace".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn table_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	table: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_004".to_string(),
        statement: None,
        message: format!("table `{}.{}` not found", namespace, table),
        fragment,
        label: Some("unknown table reference".to_string()),
        help: Some("ensure the table exists or create it first using `CREATE TABLE`".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn ring_buffer_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	ring_buffer: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_005".to_string(),
        statement: None,
        message: format!("ring buffer `{}.{}` already exists", namespace, ring_buffer),
        fragment,
        label: Some("duplicate ring buffer definition".to_string()),
        help: Some("choose a different name, drop the existing ring buffer or create ring buffer in a different namespace".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn ring_buffer_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	ring_buffer: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_006".to_string(),
        statement: None,
        message: format!("ring buffer `{}.{}` not found", namespace, ring_buffer),
        fragment,
        label: Some("unknown ring buffer reference".to_string()),
        help: Some("ensure the ring buffer exists or create it first using `CREATE RING BUFFER`".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn table_column_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	table: &str,
	column: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_005".to_string(),
        statement: None,
        message: format!("column `{}` already exists in table `{}`.`{}`", column, namespace, table),
        fragment,
        label: Some("duplicate column definition".to_string()),
        help: Some("choose a different column name or drop the existing one first".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn view_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	view: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_004".to_string(),
        statement: None,
        message: format!("view `{}.{}` not found", namespace, view),
        fragment,
        label: Some("unknown view reference".to_string()),
        help: Some("ensure the view exists or create it first using `CREATE VIEW`".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn view_column_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	namespace: &str,
	view: &str,
	column: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_005".to_string(),
        statement: None,
        message: format!("column `{}` already exists in view `{}`.`{}`", column, namespace, view),
        fragment,
        label: Some("duplicate column definition".to_string()),
        help: Some("choose a different column name or drop the existing one first".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn auto_increment_invalid_type<'a>(
	fragment: impl IntoFragment<'a>,
	column: &str,
	ty: crate::Type,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
		code: "CA_006".to_string(),
		statement: None,
		message: format!(
			"auto increment is not supported for type `{}`",
			ty
		),
		fragment,
		label: Some("invalid auto increment usage".to_string()),
		help: Some(format!(
			"auto increment is only supported for integer types (int1-16, uint1-16), column `{}` has type `{}`",
			column, ty
		)),
		column: None,
		notes: vec![],
		cause: None,
	}
}

pub fn table_column_policy_already_exists(
	policy: &str,
	column: &str,
) -> Diagnostic {
	Diagnostic {
		code: "CA_008".to_string(),
		statement: None,
		message: format!(
			"policy `{policy:?}` already exists for column `{}`",
			column
		),
		fragment: OwnedFragment::None,
		label: Some("duplicate column policy".to_string()),
		help: Some("remove the existing policy first".to_string()),
		column: None,
		notes: vec![],
		cause: None,
	}
}

pub fn index_variable_length_not_supported() -> Diagnostic {
	Diagnostic {
        code: "CA_009".to_string(),
        statement: None,
        message: "variable-length types (UTF8, BLOB) are not supported in indexes".to_string(),
        fragment: OwnedFragment::None,
        label: Some("unsupported type for indexing".to_string()),
        help: Some("only fixed-size types can be indexed currently".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn index_types_directions_mismatch(
	types_len: usize,
	directions_len: usize,
) -> Diagnostic {
	Diagnostic {
        code: "CA_010".to_string(),
        statement: None,
        message: format!(
            "mismatch between number of types ({}) and directions ({})",
            types_len, directions_len
        ),
        fragment: OwnedFragment::None,
        label: Some("length mismatch".to_string()),
        help: Some("each indexed field must have a corresponding sort direction".to_string()),
        column: None,
        notes: vec![],
        cause: None}
}

pub fn namespace_already_pending_in_transaction<'a>(
	namespace_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = namespace_name.into_fragment().into_owned();
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
        cause: None}
}

pub fn table_already_pending_in_transaction<'a>(
	namespace_name: impl IntoFragment<'a>,
	table_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let namespace_fragment = namespace_name.into_fragment().into_owned();
	let table_fragment = table_name.into_fragment().into_owned();
	let namespace = namespace_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
        code: "CA_012".to_string(),
        statement: None,
        message: format!("table `{}.{}` already has pending changes in this transaction", namespace, table),
        fragment: table_fragment,
        label: Some("duplicate table modification in transaction".to_string()),
        help: Some("a table can only be created, updated, or deleted once per transaction".to_string()),
        column: None,
        notes: vec![
            "This usually indicates a programming error in transaction management".to_string(),
            "Consider reviewing the transaction logic for duplicate operations".to_string(),
        ],
        cause: None}
}

pub fn view_already_pending_in_transaction<'a>(
	namespace_name: impl IntoFragment<'a>,
	view_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let namespace_fragment = namespace_name.into_fragment().into_owned();
	let view_fragment = view_name.into_fragment().into_owned();
	let namespace = namespace_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
        code: "CA_013".to_string(),
        statement: None,
        message: format!("view `{}.{}` already has pending changes in this transaction", namespace, view),
        fragment: view_fragment,
        label: Some("duplicate view modification in transaction".to_string()),
        help: Some("a view can only be created, updated, or deleted once per transaction".to_string()),
        column: None,
        notes: vec![
            "This usually indicates a programming error in transaction management".to_string(),
            "Consider reviewing the transaction logic for duplicate operations".to_string(),
        ],
        cause: None}
}

pub fn cannot_update_deleted_namespace<'a>(
	namespace_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = namespace_name.into_fragment().into_owned();
	let name = fragment.text();
	Diagnostic {
        code: "CA_014".to_string(),
        statement: None,
        message: format!("cannot update namespace `{}` as it is marked for deletion in this transaction", name),
        fragment,
        label: Some("attempted update on deleted namespace".to_string()),
        help: Some("remove the delete operation or skip the update".to_string()),
        column: None,
        notes: vec![
            "A namespace marked for deletion cannot be updated in the same transaction".to_string(),
        ],
        cause: None}
}

pub fn cannot_update_deleted_table<'a>(
	namespace_name: impl IntoFragment<'a>,
	table_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let namespace_fragment = namespace_name.into_fragment().into_owned();
	let table_fragment = table_name.into_fragment().into_owned();
	let namespace = namespace_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
        code: "CA_015".to_string(),
        statement: None,
        message: format!("cannot update table `{}.{}` as it is marked for deletion in this transaction", namespace, table),
        fragment: table_fragment,
        label: Some("attempted update on deleted table".to_string()),
        help: Some("remove the delete operation or skip the update".to_string()),
        column: None,
        notes: vec![
            "A table marked for deletion cannot be updated in the same transaction".to_string(),
        ],
        cause: None}
}

pub fn cannot_update_deleted_view<'a>(
	namespace_name: impl IntoFragment<'a>,
	view_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let namespace_fragment = namespace_name.into_fragment().into_owned();
	let view_fragment = view_name.into_fragment().into_owned();
	let namespace = namespace_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
        code: "CA_016".to_string(),
        statement: None,
        message: format!("cannot update view `{}.{}` as it is marked for deletion in this transaction", namespace, view),
        fragment: view_fragment,
        label: Some("attempted update on deleted view".to_string()),
        help: Some("remove the delete operation or skip the update".to_string()),
        column: None,
        notes: vec![
            "A view marked for deletion cannot be updated in the same transaction".to_string(),
        ],
        cause: None}
}

pub fn cannot_delete_already_deleted_namespace<'a>(
	namespace_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = namespace_name.into_fragment().into_owned();
	let name = fragment.text();
	Diagnostic {
		code: "CA_017".to_string(),
		statement: None,
		message: format!(
			"namespace `{}` is already marked for deletion in this transaction",
			name
		),
		fragment,
		label: Some("duplicate namespace deletion".to_string()),
		help: Some("remove the duplicate delete operation".to_string()),
		column: None,
		notes: vec![
			"A namespace can only be deleted once per transaction"
				.to_string(),
		],
		cause: None,
	}
}

pub fn cannot_delete_already_deleted_table<'a>(
	namespace_name: impl IntoFragment<'a>,
	table_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let namespace_fragment = namespace_name.into_fragment().into_owned();
	let table_fragment = table_name.into_fragment().into_owned();
	let namespace = namespace_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
		code: "CA_018".to_string(),
		statement: None,
		message: format!(
			"table `{}.{}` is already marked for deletion in this transaction",
			namespace, table
		),
		fragment: table_fragment,
		label: Some("duplicate table deletion".to_string()),
		help: Some("remove the duplicate delete operation".to_string()),
		column: None,
		notes: vec!["A table can only be deleted once per transaction"
			.to_string()],
		cause: None,
	}
}

pub fn cannot_delete_already_deleted_view<'a>(
	namespace_name: impl IntoFragment<'a>,
	view_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let namespace_fragment = namespace_name.into_fragment().into_owned();
	let view_fragment = view_name.into_fragment().into_owned();
	let namespace = namespace_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
		code: "CA_019".to_string(),
		statement: None,
		message: format!(
			"view `{}.{}` is already marked for deletion in this transaction",
			namespace, view
		),
		fragment: view_fragment,
		label: Some("duplicate view deletion".to_string()),
		help: Some("remove the duplicate delete operation".to_string()),
		column: None,
		notes: vec!["A view can only be deleted once per transaction"
			.to_string()],
		cause: None,
	}
}

pub fn primary_key_empty<'a>(fragment: impl IntoFragment<'a>) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
		code: "CA_020".to_string(),
		statement: None,
		message: "primary key must contain at least one column"
			.to_string(),
		fragment,
		label: Some("empty primary key definition".to_string()),
		help: Some("specify at least one column for the primary key"
			.to_string()),
		column: None,
		notes: vec![],
		cause: None,
	}
}

pub fn primary_key_column_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	column_id: u64,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
		code: "CA_021".to_string(),
		statement: None,
		message: format!("column with ID {} not found for primary key", column_id),
		fragment,
		label: Some("invalid column reference in primary key".to_string()),
		help: Some("ensure all columns referenced in the primary key exist in the table or view".to_string()),
		column: None,
		notes: vec![],
		cause: None}
}

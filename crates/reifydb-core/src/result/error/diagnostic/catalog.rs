// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	OwnedFragment, interface::fragment::IntoFragment,
	result::error::diagnostic::Diagnostic,
};

pub fn schema_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_001".to_string(),
        statement: None,
        message: format!("schema `{}` already exists", schema),
        fragment,
        label: Some("duplicate schema definition".to_string()),
        help: Some("choose a different name or drop the existing schema first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn schema_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_002".to_string(),
        statement: None,
        message: format!("schema `{}` not found", schema),
        fragment,
        label: Some("undefined schema reference".to_string()),
        help: Some("make sure the schema exists before using it or create it first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn table_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
	table: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_003".to_string(),
        statement: None,
        message: format!("table `{}.{}` already exists", schema, table),
        fragment,
        label: Some("duplicate table definition".to_string()),
        help: Some("choose a different name, drop the existing table or create table in a different schema".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn view_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
	view: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_003".to_string(),
        statement: None,
        message: format!("view `{}.{}` already exists", schema, view),
        fragment,
        label: Some("duplicate view definition".to_string()),
        help: Some("choose a different name, drop the existing view or create view in a different schema".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn table_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
	table: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_004".to_string(),
        statement: None,
        message: format!("table `{}.{}` not found", schema, table),
        fragment,
        label: Some("unknown table reference".to_string()),
        help: Some("ensure the table exists or create it first using `CREATE TABLE`".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn table_column_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
	table: &str,
	column: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_005".to_string(),
        statement: None,
        message: format!("column `{}` already exists in table `{}`.`{}`", column, schema, table),
        fragment,
        label: Some("duplicate column definition".to_string()),
        help: Some("choose a different column name or drop the existing one first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn view_not_found<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
	view: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_004".to_string(),
        statement: None,
        message: format!("view `{}.{}` not found", schema, view),
        fragment,
        label: Some("unknown view reference".to_string()),
        help: Some("ensure the view exists or create it first using `CREATE VIEW`".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
}

pub fn view_column_already_exists<'a>(
	fragment: impl IntoFragment<'a>,
	schema: &str,
	view: &str,
	column: &str,
) -> Diagnostic {
	let fragment = fragment.into_fragment().into_owned();
	Diagnostic {
        code: "CA_005".to_string(),
        statement: None,
        message: format!("column `{}` already exists in view `{}`.`{}`", column, schema, view),
        fragment,
        label: Some("duplicate column definition".to_string()),
        help: Some("choose a different column name or drop the existing one first".to_string()),
        column: None,
        notes: vec![],
        cause: None,
    }
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
        cause: None,
    }
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
        cause: None,
    }
}

pub fn schema_already_pending_in_transaction<'a>(
	schema_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = schema_name.into_fragment().into_owned();
	let name = fragment.text();
	Diagnostic {
        code: "CA_011".to_string(),
        statement: None,
        message: format!("schema `{}` already has pending changes in this transaction", name),
        fragment,
        label: Some("duplicate schema modification in transaction".to_string()),
        help: Some("a schema can only be created, updated, or deleted once per transaction".to_string()),
        column: None,
        notes: vec![
            "This usually indicates a programming error in transaction management".to_string(),
            "Consider reviewing the transaction logic for duplicate operations".to_string(),
        ],
        cause: None,
    }
}

pub fn table_already_pending_in_transaction<'a>(
	schema_name: impl IntoFragment<'a>,
	table_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let schema_fragment = schema_name.into_fragment().into_owned();
	let table_fragment = table_name.into_fragment().into_owned();
	let schema = schema_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
        code: "CA_012".to_string(),
        statement: None,
        message: format!("table `{}.{}` already has pending changes in this transaction", schema, table),
        fragment: table_fragment,
        label: Some("duplicate table modification in transaction".to_string()),
        help: Some("a table can only be created, updated, or deleted once per transaction".to_string()),
        column: None,
        notes: vec![
            "This usually indicates a programming error in transaction management".to_string(),
            "Consider reviewing the transaction logic for duplicate operations".to_string(),
        ],
        cause: None,
    }
}

pub fn view_already_pending_in_transaction<'a>(
	schema_name: impl IntoFragment<'a>,
	view_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let schema_fragment = schema_name.into_fragment().into_owned();
	let view_fragment = view_name.into_fragment().into_owned();
	let schema = schema_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
        code: "CA_013".to_string(),
        statement: None,
        message: format!("view `{}.{}` already has pending changes in this transaction", schema, view),
        fragment: view_fragment,
        label: Some("duplicate view modification in transaction".to_string()),
        help: Some("a view can only be created, updated, or deleted once per transaction".to_string()),
        column: None,
        notes: vec![
            "This usually indicates a programming error in transaction management".to_string(),
            "Consider reviewing the transaction logic for duplicate operations".to_string(),
        ],
        cause: None,
    }
}

pub fn cannot_update_deleted_schema<'a>(
	schema_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = schema_name.into_fragment().into_owned();
	let name = fragment.text();
	Diagnostic {
        code: "CA_014".to_string(),
        statement: None,
        message: format!("cannot update schema `{}` as it is marked for deletion in this transaction", name),
        fragment,
        label: Some("attempted update on deleted schema".to_string()),
        help: Some("remove the delete operation or skip the update".to_string()),
        column: None,
        notes: vec![
            "A schema marked for deletion cannot be updated in the same transaction".to_string(),
        ],
        cause: None,
    }
}

pub fn cannot_update_deleted_table<'a>(
	schema_name: impl IntoFragment<'a>,
	table_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let schema_fragment = schema_name.into_fragment().into_owned();
	let table_fragment = table_name.into_fragment().into_owned();
	let schema = schema_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
        code: "CA_015".to_string(),
        statement: None,
        message: format!("cannot update table `{}.{}` as it is marked for deletion in this transaction", schema, table),
        fragment: table_fragment,
        label: Some("attempted update on deleted table".to_string()),
        help: Some("remove the delete operation or skip the update".to_string()),
        column: None,
        notes: vec![
            "A table marked for deletion cannot be updated in the same transaction".to_string(),
        ],
        cause: None,
    }
}

pub fn cannot_update_deleted_view<'a>(
	schema_name: impl IntoFragment<'a>,
	view_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let schema_fragment = schema_name.into_fragment().into_owned();
	let view_fragment = view_name.into_fragment().into_owned();
	let schema = schema_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
        code: "CA_016".to_string(),
        statement: None,
        message: format!("cannot update view `{}.{}` as it is marked for deletion in this transaction", schema, view),
        fragment: view_fragment,
        label: Some("attempted update on deleted view".to_string()),
        help: Some("remove the delete operation or skip the update".to_string()),
        column: None,
        notes: vec![
            "A view marked for deletion cannot be updated in the same transaction".to_string(),
        ],
        cause: None,
    }
}

pub fn cannot_delete_already_deleted_schema<'a>(
	schema_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let fragment = schema_name.into_fragment().into_owned();
	let name = fragment.text();
	Diagnostic {
		code: "CA_017".to_string(),
		statement: None,
		message: format!(
			"schema `{}` is already marked for deletion in this transaction",
			name
		),
		fragment,
		label: Some("duplicate schema deletion".to_string()),
		help: Some("remove the duplicate delete operation".to_string()),
		column: None,
		notes: vec![
			"A schema can only be deleted once per transaction"
				.to_string(),
		],
		cause: None,
	}
}

pub fn cannot_delete_already_deleted_table<'a>(
	schema_name: impl IntoFragment<'a>,
	table_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let schema_fragment = schema_name.into_fragment().into_owned();
	let table_fragment = table_name.into_fragment().into_owned();
	let schema = schema_fragment.text();
	let table = table_fragment.text();
	Diagnostic {
		code: "CA_018".to_string(),
		statement: None,
		message: format!(
			"table `{}.{}` is already marked for deletion in this transaction",
			schema, table
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
	schema_name: impl IntoFragment<'a>,
	view_name: impl IntoFragment<'a>,
) -> Diagnostic {
	let schema_fragment = schema_name.into_fragment().into_owned();
	let view_fragment = view_name.into_fragment().into_owned();
	let schema = schema_fragment.text();
	let view = view_fragment.text();
	Diagnostic {
		code: "CA_019".to_string(),
		statement: None,
		message: format!(
			"view `{}.{}` is already marked for deletion in this transaction",
			schema, view
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

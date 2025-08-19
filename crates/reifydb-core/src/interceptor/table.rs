use std::marker::PhantomData;

use crate::{
	RowId, define_closure_interceptor, define_helper_function,
	define_interceptor, impl_add_to_builder,
	interface::{CommandTransaction, TableDef, Transaction},
	row::EncodedRow,
};

// PRE INSERT
define_interceptor!(
	context: TablePreInsertContext<T>,
	trait: TablePreInsertInterceptor,
	fields: {
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		row: &'a EncodedRow,
	}
);

define_closure_interceptor!(
	ClosureTablePreInsertInterceptor,
	TablePreInsertInterceptor,
	TablePreInsertContext,
	with_transaction
);

define_helper_function!(
	table_pre_insert,
	ClosureTablePreInsertInterceptor<T, F>,
	TablePreInsertContext<T>
);

impl_add_to_builder!(
	ClosureTablePreInsertInterceptor<T, F>,
	TablePreInsertContext<T>,
	add_table_pre_insert
);

// POST INSERT
define_interceptor!(
	context: TablePostInsertContext<T>,
	trait: TablePostInsertInterceptor,
	fields: {
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		row: &'a EncodedRow,
	}
);

define_closure_interceptor!(
	ClosureTablePostInsertInterceptor,
	TablePostInsertInterceptor,
	TablePostInsertContext,
	with_transaction
);

define_helper_function!(
	table_post_insert,
	ClosureTablePostInsertInterceptor<T, F>,
	TablePostInsertContext<T>
);

impl_add_to_builder!(
	ClosureTablePostInsertInterceptor<T, F>,
	TablePostInsertContext<T>,
	add_table_post_insert
);

// PRE UPDATE
define_interceptor!(
	context: TablePreUpdateContext<T>,
	trait: TablePreUpdateInterceptor,
	fields: {
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		row: &'a EncodedRow,
	}
);

define_closure_interceptor!(
	ClosureTablePreUpdateInterceptor,
	TablePreUpdateInterceptor,
	TablePreUpdateContext,
	with_transaction
);

define_helper_function!(
	table_pre_update,
	ClosureTablePreUpdateInterceptor<T, F>,
	TablePreUpdateContext<T>
);

impl_add_to_builder!(
	ClosureTablePreUpdateInterceptor<T, F>,
	TablePreUpdateContext<T>,
	add_table_pre_update
);

// POST UPDATE
define_interceptor!(
	context: TablePostUpdateContext<T>,
	trait: TablePostUpdateInterceptor,
	fields: {
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		row: &'a EncodedRow,
		old_row: &'a EncodedRow,
	}
);

define_closure_interceptor!(
	ClosureTablePostUpdateInterceptor,
	TablePostUpdateInterceptor,
	TablePostUpdateContext,
	with_transaction
);

define_helper_function!(
	table_post_update,
	ClosureTablePostUpdateInterceptor<T, F>,
	TablePostUpdateContext<T>
);

impl_add_to_builder!(
	ClosureTablePostUpdateInterceptor<T, F>,
	TablePostUpdateContext<T>,
	add_table_post_update
);

// PRE DELETE
define_interceptor!(
	context: TablePreDeleteContext<T>,
	trait: TablePreDeleteInterceptor,
	fields: {
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
	}
);

define_closure_interceptor!(
	ClosureTablePreDeleteInterceptor,
	TablePreDeleteInterceptor,
	TablePreDeleteContext,
	with_transaction
);

define_helper_function!(
	table_pre_delete,
	ClosureTablePreDeleteInterceptor<T, F>,
	TablePreDeleteContext<T>
);

impl_add_to_builder!(
	ClosureTablePreDeleteInterceptor<T, F>,
	TablePreDeleteContext<T>,
	add_table_pre_delete
);

// POST DELETE
define_interceptor!(
	context: TablePostDeleteContext<T>,
	trait: TablePostDeleteInterceptor,
	fields: {
		txn: &'a mut CommandTransaction<T>,
		table: &'a TableDef,
		id: RowId,
		deleted_row: &'a EncodedRow,
	}
);

define_closure_interceptor!(
	ClosureTablePostDeleteInterceptor,
	TablePostDeleteInterceptor,
	TablePostDeleteContext,
	with_transaction
);

define_helper_function!(
	table_post_delete,
	ClosureTablePostDeleteInterceptor<T, F>,
	TablePostDeleteContext<T>
);

impl_add_to_builder!(
	ClosureTablePostDeleteInterceptor<T, F>,
	TablePostDeleteContext<T>,
	add_table_post_delete
);

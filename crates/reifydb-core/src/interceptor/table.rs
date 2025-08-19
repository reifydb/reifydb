use std::marker::PhantomData;

use crate::{
	RowId, define_api_function, define_closure_interceptor,
	define_interceptor, impl_register_interceptor,
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

define_api_function!(
	table_pre_insert,
	ClosureTablePreInsertInterceptor<T, F>,
	TablePreInsertContext<T>
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

define_api_function!(
	table_post_insert,
	ClosureTablePostInsertInterceptor<T, F>,
	TablePostInsertContext<T>
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

define_api_function!(
	table_pre_update,
	ClosureTablePreUpdateInterceptor<T, F>,
	TablePreUpdateContext<T>
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

define_api_function!(
	table_post_update,
	ClosureTablePostUpdateInterceptor<T, F>,
	TablePostUpdateContext<T>
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

define_api_function!(
	table_pre_delete,
	ClosureTablePreDeleteInterceptor<T, F>,
	TablePreDeleteContext<T>
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

define_api_function!(
	table_post_delete,
	ClosureTablePostDeleteInterceptor<T, F>,
	TablePostDeleteContext<T>
);

impl_register_interceptor!(
	ClosureTablePreInsertInterceptor<T, F>,
	TablePreInsertContext<T>,
	TablePreInsertInterceptor,
	table_pre_insert
);

impl_register_interceptor!(
	ClosureTablePostInsertInterceptor<T, F>,
	TablePostInsertContext<T>,
	TablePostInsertInterceptor,
	table_post_insert
);

impl_register_interceptor!(
	ClosureTablePreUpdateInterceptor<T, F>,
	TablePreUpdateContext<T>,
	TablePreUpdateInterceptor,
	table_pre_update
);

impl_register_interceptor!(
	ClosureTablePostUpdateInterceptor<T, F>,
	TablePostUpdateContext<T>,
	TablePostUpdateInterceptor,
	table_post_update
);

impl_register_interceptor!(
	ClosureTablePreDeleteInterceptor<T, F>,
	TablePreDeleteContext<T>,
	TablePreDeleteInterceptor,
	table_pre_delete
);

impl_register_interceptor!(
	ClosureTablePostDeleteInterceptor<T, F>,
	TablePostDeleteContext<T>,
	TablePostDeleteInterceptor,
	table_post_delete
);

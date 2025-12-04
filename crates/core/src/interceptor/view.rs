use std::marker::PhantomData;

use reifydb_type::RowNumber;

use crate::{
	define_api_function, define_closure_interceptor, define_interceptor, impl_register_interceptor,
	interface::ViewDef, value::encoded::EncodedValues,
};

// PRE INSERT
define_interceptor!(
	context: ViewPreInsertContext<T>,
	trait: ViewPreInsertInterceptor,
	fields: {
		txn: &'a mut T,
		view: &'a ViewDef,
		rn: RowNumber,
		row: &'a EncodedValues}
);

define_closure_interceptor!(
	ClosureViewPreInsertInterceptor,
	ViewPreInsertInterceptor,
	ViewPreInsertContext,
	with_transaction
);

define_api_function!(
	view_pre_insert,
	ClosureViewPreInsertInterceptor<T, F>,
	ViewPreInsertContext<T>
);

// POST INSERT
define_interceptor!(
	context: ViewPostInsertContext<T>,
	trait: ViewPostInsertInterceptor,
	fields: {
		txn: &'a mut T,
		view: &'a ViewDef,
		id: RowNumber,
		row: &'a EncodedValues}
);

define_closure_interceptor!(
	ClosureViewPostInsertInterceptor,
	ViewPostInsertInterceptor,
	ViewPostInsertContext,
	with_transaction
);

define_api_function!(
	view_post_insert,
	ClosureViewPostInsertInterceptor<T, F>,
	ViewPostInsertContext<T>
);

// PRE UPDATE
define_interceptor!(
	context: ViewPreUpdateContext<T>,
	trait: ViewPreUpdateInterceptor,
	fields: {
		txn: &'a mut T,
		view: &'a ViewDef,
		id: RowNumber,
		row: &'a EncodedValues}
);

define_closure_interceptor!(
	ClosureViewPreUpdateInterceptor,
	ViewPreUpdateInterceptor,
	ViewPreUpdateContext,
	with_transaction
);

define_api_function!(
	view_pre_update,
	ClosureViewPreUpdateInterceptor<T, F>,
	ViewPreUpdateContext<T>
);

// POST UPDATE
define_interceptor!(
	context: ViewPostUpdateContext<T>,
	trait: ViewPostUpdateInterceptor,
	fields: {
		txn: &'a mut T,
		view: &'a ViewDef,
		id: RowNumber,
		row: &'a EncodedValues,
		old_row: &'a EncodedValues}
);

define_closure_interceptor!(
	ClosureViewPostUpdateInterceptor,
	ViewPostUpdateInterceptor,
	ViewPostUpdateContext,
	with_transaction
);

define_api_function!(
	view_post_update,
	ClosureViewPostUpdateInterceptor<T, F>,
	ViewPostUpdateContext<T>
);

// PRE DELETE
define_interceptor!(
	context: ViewPreDeleteContext<T>,
	trait: ViewPreDeleteInterceptor,
	fields: {
		txn: &'a mut T,
		view: &'a ViewDef,
		id: RowNumber}
);

define_closure_interceptor!(
	ClosureViewPreDeleteInterceptor,
	ViewPreDeleteInterceptor,
	ViewPreDeleteContext,
	with_transaction
);

define_api_function!(
	view_pre_delete,
	ClosureViewPreDeleteInterceptor<T, F>,
	ViewPreDeleteContext<T>
);

// POST DELETE
define_interceptor!(
	context: ViewPostDeleteContext<T>,
	trait: ViewPostDeleteInterceptor,
	fields: {
		txn: &'a mut T,
		view: &'a ViewDef,
		id: RowNumber,
		deleted_row: &'a EncodedValues}
);

define_closure_interceptor!(
	ClosureViewPostDeleteInterceptor,
	ViewPostDeleteInterceptor,
	ViewPostDeleteContext,
	with_transaction
);

define_api_function!(
	view_post_delete,
	ClosureViewPostDeleteInterceptor<T, F>,
	ViewPostDeleteContext<T>
);

impl_register_interceptor!(
	ClosureViewPreInsertInterceptor<T, F>,
	ViewPreInsertContext<T>,
	ViewPreInsertInterceptor,
	view_pre_insert
);

impl_register_interceptor!(
	ClosureViewPostInsertInterceptor<T, F>,
	ViewPostInsertContext<T>,
	ViewPostInsertInterceptor,
	view_post_insert
);

impl_register_interceptor!(
	ClosureViewPreUpdateInterceptor<T, F>,
	ViewPreUpdateContext<T>,
	ViewPreUpdateInterceptor,
	view_pre_update
);

impl_register_interceptor!(
	ClosureViewPostUpdateInterceptor<T, F>,
	ViewPostUpdateContext<T>,
	ViewPostUpdateInterceptor,
	view_post_update
);

impl_register_interceptor!(
	ClosureViewPreDeleteInterceptor<T, F>,
	ViewPreDeleteContext<T>,
	ViewPreDeleteInterceptor,
	view_pre_delete
);

impl_register_interceptor!(
	ClosureViewPostDeleteInterceptor<T, F>,
	ViewPostDeleteContext<T>,
	ViewPostDeleteInterceptor,
	view_post_delete
);

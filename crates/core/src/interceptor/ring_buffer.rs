use std::marker::PhantomData;

use reifydb_type::RowNumber;

use crate::{
	define_api_function, define_closure_interceptor, define_interceptor, impl_register_interceptor,
	interface::RingBufferDef, value::row::EncodedRow,
};

// PRE INSERT
define_interceptor!(
	context: RingBufferPreInsertContext<T>,
	trait: RingBufferPreInsertInterceptor,
	fields: {
		txn: &'a mut T,
		ring_buffer: &'a RingBufferDef,
		row: &'a EncodedRow}
);

define_closure_interceptor!(
	ClosureRingBufferPreInsertInterceptor,
	RingBufferPreInsertInterceptor,
	RingBufferPreInsertContext,
	with_transaction
);

define_api_function!(
	ring_buffer_pre_insert,
	ClosureRingBufferPreInsertInterceptor<T, F>,
	RingBufferPreInsertContext<T>
);

// POST INSERT
define_interceptor!(
	context: RingBufferPostInsertContext<T>,
	trait: RingBufferPostInsertInterceptor,
	fields: {
		txn: &'a mut T,
		ring_buffer: &'a RingBufferDef,
		id: RowNumber,
		row: &'a EncodedRow}
);

define_closure_interceptor!(
	ClosureRingBufferPostInsertInterceptor,
	RingBufferPostInsertInterceptor,
	RingBufferPostInsertContext,
	with_transaction
);

define_api_function!(
	ring_buffer_post_insert,
	ClosureRingBufferPostInsertInterceptor<T, F>,
	RingBufferPostInsertContext<T>
);

// PRE UPDATE
define_interceptor!(
	context: RingBufferPreUpdateContext<T>,
	trait: RingBufferPreUpdateInterceptor,
	fields: {
		txn: &'a mut T,
		ring_buffer: &'a RingBufferDef,
		id: RowNumber,
		row: &'a EncodedRow}
);

define_closure_interceptor!(
	ClosureRingBufferPreUpdateInterceptor,
	RingBufferPreUpdateInterceptor,
	RingBufferPreUpdateContext,
	with_transaction
);

define_api_function!(
	ring_buffer_pre_update,
	ClosureRingBufferPreUpdateInterceptor<T, F>,
	RingBufferPreUpdateContext<T>
);

// POST UPDATE
define_interceptor!(
	context: RingBufferPostUpdateContext<T>,
	trait: RingBufferPostUpdateInterceptor,
	fields: {
		txn: &'a mut T,
		ring_buffer: &'a RingBufferDef,
		id: RowNumber,
		row: &'a EncodedRow,
		old_row: &'a EncodedRow}
);

define_closure_interceptor!(
	ClosureRingBufferPostUpdateInterceptor,
	RingBufferPostUpdateInterceptor,
	RingBufferPostUpdateContext,
	with_transaction
);

define_api_function!(
	ring_buffer_post_update,
	ClosureRingBufferPostUpdateInterceptor<T, F>,
	RingBufferPostUpdateContext<T>
);

// PRE DELETE
define_interceptor!(
	context: RingBufferPreDeleteContext<T>,
	trait: RingBufferPreDeleteInterceptor,
	fields: {
		txn: &'a mut T,
		ring_buffer: &'a RingBufferDef,
		id: RowNumber}
);

define_closure_interceptor!(
	ClosureRingBufferPreDeleteInterceptor,
	RingBufferPreDeleteInterceptor,
	RingBufferPreDeleteContext,
	with_transaction
);

define_api_function!(
	ring_buffer_pre_delete,
	ClosureRingBufferPreDeleteInterceptor<T, F>,
	RingBufferPreDeleteContext<T>
);

// POST DELETE
define_interceptor!(
	context: RingBufferPostDeleteContext<T>,
	trait: RingBufferPostDeleteInterceptor,
	fields: {
		txn: &'a mut T,
		ring_buffer: &'a RingBufferDef,
		id: RowNumber,
		deleted_row: &'a EncodedRow}
);

define_closure_interceptor!(
	ClosureRingBufferPostDeleteInterceptor,
	RingBufferPostDeleteInterceptor,
	RingBufferPostDeleteContext,
	with_transaction
);

define_api_function!(
	ring_buffer_post_delete,
	ClosureRingBufferPostDeleteInterceptor<T, F>,
	RingBufferPostDeleteContext<T>
);

impl_register_interceptor!(
	ClosureRingBufferPreInsertInterceptor<T, F>,
	RingBufferPreInsertContext<T>,
	RingBufferPreInsertInterceptor,
	ring_buffer_pre_insert
);

impl_register_interceptor!(
	ClosureRingBufferPostInsertInterceptor<T, F>,
	RingBufferPostInsertContext<T>,
	RingBufferPostInsertInterceptor,
	ring_buffer_post_insert
);

impl_register_interceptor!(
	ClosureRingBufferPreUpdateInterceptor<T, F>,
	RingBufferPreUpdateContext<T>,
	RingBufferPreUpdateInterceptor,
	ring_buffer_pre_update
);

impl_register_interceptor!(
	ClosureRingBufferPostUpdateInterceptor<T, F>,
	RingBufferPostUpdateContext<T>,
	RingBufferPostUpdateInterceptor,
	ring_buffer_post_update
);

impl_register_interceptor!(
	ClosureRingBufferPreDeleteInterceptor<T, F>,
	RingBufferPreDeleteContext<T>,
	RingBufferPreDeleteInterceptor,
	ring_buffer_pre_delete
);

impl_register_interceptor!(
	ClosureRingBufferPostDeleteInterceptor<T, F>,
	RingBufferPostDeleteContext<T>,
	RingBufferPostDeleteInterceptor,
	ring_buffer_post_delete
);

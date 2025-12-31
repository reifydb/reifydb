// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::marker::PhantomData;

use crate::{define_api_function, define_closure_interceptor, define_interceptor, interface::RingBufferDef};

// RING BUFFER POST CREATE
define_interceptor!(
    context: RingBufferDefPostCreateContext<T>,
    trait: RingBufferDefPostCreateInterceptor,
    fields: {
	txn: &'a mut T,
	post: &'a RingBufferDef}
);

define_closure_interceptor!(
	ClosureRingBufferDefPostCreateInterceptor,
	RingBufferDefPostCreateInterceptor,
	RingBufferDefPostCreateContext,
	with_transaction
);

define_api_function!(
    ringbuffer_def_post_create,
    ClosureRingBufferDefPostCreateInterceptor<T, F>,
    RingBufferDefPostCreateContext<T>
);

// RING BUFFER PRE UPDATE
define_interceptor!(
    context: RingBufferDefPreUpdateContext<T>,
    trait: RingBufferDefPreUpdateInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a RingBufferDef}
);

define_closure_interceptor!(
	ClosureRingBufferDefPreUpdateInterceptor,
	RingBufferDefPreUpdateInterceptor,
	RingBufferDefPreUpdateContext,
	with_transaction
);

define_api_function!(
    ringbuffer_def_pre_update,
    ClosureRingBufferDefPreUpdateInterceptor<T, F>,
    RingBufferDefPreUpdateContext<T>
);

// RING BUFFER POST UPDATE
define_interceptor!(
    context: RingBufferDefPostUpdateContext<T>,
    trait: RingBufferDefPostUpdateInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a RingBufferDef,
	post: &'a RingBufferDef}
);

define_closure_interceptor!(
	ClosureRingBufferDefPostUpdateInterceptor,
	RingBufferDefPostUpdateInterceptor,
	RingBufferDefPostUpdateContext,
	with_transaction
);

define_api_function!(
    ringbuffer_def_post_update,
    ClosureRingBufferDefPostUpdateInterceptor<T, F>,
    RingBufferDefPostUpdateContext<T>
);

// RING BUFFER PRE DELETE
define_interceptor!(
    context: RingBufferDefPreDeleteContext<T>,
    trait: RingBufferDefPreDeleteInterceptor,
    fields: {
	txn: &'a mut T,
	pre: &'a RingBufferDef}
);

define_closure_interceptor!(
	ClosureRingBufferDefPreDeleteInterceptor,
	RingBufferDefPreDeleteInterceptor,
	RingBufferDefPreDeleteContext,
	with_transaction
);

define_api_function!(
    ringbuffer_def_pre_delete,
    ClosureRingBufferDefPreDeleteInterceptor<T, F>,
    RingBufferDefPreDeleteContext<T>
);

use crate::impl_register_interceptor;

impl_register_interceptor!(
    ClosureRingBufferDefPostCreateInterceptor<T, F>,
    RingBufferDefPostCreateContext<T>,
    RingBufferDefPostCreateInterceptor,
    ringbuffer_def_post_create
);

impl_register_interceptor!(
    ClosureRingBufferDefPreUpdateInterceptor<T, F>,
    RingBufferDefPreUpdateContext<T>,
    RingBufferDefPreUpdateInterceptor,
    ringbuffer_def_pre_update
);

impl_register_interceptor!(
    ClosureRingBufferDefPostUpdateInterceptor<T, F>,
    RingBufferDefPostUpdateContext<T>,
    RingBufferDefPostUpdateInterceptor,
    ringbuffer_def_post_update
);

impl_register_interceptor!(
    ClosureRingBufferDefPreDeleteInterceptor<T, F>,
    RingBufferDefPreDeleteContext<T>,
    RingBufferDefPreDeleteInterceptor,
    ringbuffer_def_pre_delete
);

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use crate::{
	Version, define_api_function, define_closure_interceptor,
	define_interceptor, interface::TransactionId,
};

// PRE COMMIT
define_interceptor!(
	context: PreCommitContext<T>,
	trait: PreCommitInterceptor,
	fields: {
		txn: &'a mut T}
);

define_closure_interceptor!(
	ClosurePreCommitInterceptor,
	PreCommitInterceptor,
	PreCommitContext,
	with_transaction
);

define_api_function!(
	pre_commit,
	ClosurePreCommitInterceptor<T, F>,
	PreCommitContext<T>
);

// POST COMMIT
define_interceptor!(
	context: PostCommitContext,
	trait: PostCommitInterceptor<T>,
	fields: {
		id: TransactionId,
		version: Version,
		changes: TransactionalChanges}
);

define_closure_interceptor!(
	ClosurePostCommitInterceptor,
	PostCommitInterceptor,
	PostCommitContext,
	no_transaction_param
);

define_api_function!(
	post_commit,
	ClosurePostCommitInterceptor<F>,
	PostCommitContext
);

use crate::{impl_register_interceptor, interface::TransactionalChanges};

impl_register_interceptor!(
	ClosurePreCommitInterceptor<T, F>,
	PreCommitContext<T>,
	PreCommitInterceptor,
	pre_commit
);

impl_register_interceptor!(
	ClosurePostCommitInterceptor<F>,
	PostCommitContext,
	PostCommitInterceptor<T>,
	post_commit
);

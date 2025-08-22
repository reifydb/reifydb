// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use crate::{
    Version, define_api_function, define_closure_interceptor,
    define_interceptor,
    interface::Transaction,
    transaction::StandardCommandTransaction,
};

// PRE COMMIT
define_interceptor!(
	context: PreCommitContext<T>,
	trait: PreCommitInterceptor,
	fields: {
		txn: &'a mut StandardCommandTransaction<T>,
	}
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

// impl_add_to_builder!(
// 	ClosurePreCommitInterceptor<T, F>,
// 	PreCommitContext<T>,
// 	add_pre_commit
// );

// POST COMMIT
define_interceptor!(
	context: PostCommitContext,
	trait: PostCommitInterceptor<T>,
	fields: {
		version: Version,
	}
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
// impl_add_to_builder!(
// 	ClosurePostCommitInterceptor<F>,
// 	PostCommitContext,
// 	add_post_commit
// );

use crate::impl_register_interceptor;

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

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_value::value::identity::IdentityId;

use crate::context::RuntimeContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdentityContext {
	pub session: IdentityId,
	pub current: IdentityId,
}

#[derive(Debug)]
pub struct TransactionContext {
	pub identity: IdentityContext,
	pub runtime: RuntimeContext,
}

impl TransactionContext {
	pub fn new(identity: IdentityId, runtime: RuntimeContext) -> Self {
		Self {
			identity: IdentityContext {
				session: identity,
				current: identity,
			},
			runtime,
		}
	}

	pub fn system(runtime: RuntimeContext) -> Self {
		Self::new(IdentityId::system(), runtime)
	}

	pub fn with_current(self: &Arc<Self>, id: IdentityId) -> Arc<Self> {
		Arc::new(Self {
			identity: IdentityContext {
				session: self.identity.session,
				current: id,
			},
			runtime: self.runtime.clone(),
		})
	}
}

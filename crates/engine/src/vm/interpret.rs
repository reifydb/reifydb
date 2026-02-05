// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_transaction::transaction::{
	Transaction, admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction,
};

pub(crate) enum TransactionAccess<'a> {
	Admin(&'a mut AdminTransaction),
	Command(&'a mut CommandTransaction),
	Query(&'a mut QueryTransaction),
}

impl<'a> TransactionAccess<'a> {
	pub fn as_transaction(&mut self) -> Transaction<'_> {
		match self {
			TransactionAccess::Admin(txn) => Transaction::from(&mut **txn),
			TransactionAccess::Command(txn) => Transaction::from(&mut **txn),
			TransactionAccess::Query(txn) => Transaction::from(&mut **txn),
		}
	}
}

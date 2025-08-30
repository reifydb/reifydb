// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result, SortKey,
	interface::{
		Params, QueryTransaction, Transaction,
		evaluate::expression::Expression,
		virtual_table::VirtualTableDef,
	},
	value::columnar::Columns,
};

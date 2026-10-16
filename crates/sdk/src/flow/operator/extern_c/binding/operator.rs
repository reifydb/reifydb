// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	operator_with::ApplyWith,
};
use reifydb_value::config::ExtensionParams;

use crate::{
	error::Result,
	flow::operator::{
		MountedOperator, OperatorMetadata,
		change::{BorrowedChange, BorrowedColumns},
		column::operator::OperatorColumn,
		extern_c::binding::{context::ExternCContext, dictionary::raw_get_many, view::ExternCChangeView},
		timer::Timer,
		view::in_process::DictionaryValues,
	},
};

fn dictionary_values(ctx: &ExternCContext, columns: BorrowedColumns<'_>) -> Result<DictionaryValues> {
	let mut values = DictionaryValues::new();
	for (index, column) in columns.columns().enumerate() {
		let Some(dictionary) = column.dictionary() else {
			continue;
		};
		let mut ids = Vec::with_capacity(column.row_count());
		for row in 0..column.row_count() {
			if !column.defined_at(row)? {
				continue;
			}
			if let Some(id) = column.dictionary_id_at(row)? {
				ids.push(id);
			}
		}
		ids.sort_unstable();
		ids.dedup();
		let found = raw_get_many(ctx, dictionary, &ids)?;
		let entries = ids
			.into_iter()
			.zip(found)
			.filter_map(|(id, value)| value.map(|value| (id, value)))
			.collect::<HashMap<_, _>>();
		values.insert(index, entries);
	}
	Ok(values)
}

pub trait ExternCOperator: 'static {
	fn new(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self>
	where
		Self: Sized;

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut ExternCContext, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub struct ExternCOperatorAdapter<C> {
	core: C,
}

impl<C: OperatorMetadata> OperatorMetadata for ExternCOperatorAdapter<C> {
	const NAME: &'static str = C::NAME;
	const VERSION: &'static str = C::VERSION;
	const DESCRIPTION: &'static str = C::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = C::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = C::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = C::CAPABILITIES;
}

impl<C: MountedOperator + OperatorMetadata + 'static> ExternCOperator for ExternCOperatorAdapter<C> {
	fn new(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		Ok(Self {
			core: C::create(operator_id, params, with)?,
		})
	}

	fn apply(&mut self, ctx: &mut ExternCContext, input: BorrowedChange<'_>) -> Result<()> {
		let values = input
			.diffs()
			.map(|diff| Ok([dictionary_values(ctx, diff.pre())?, dictionary_values(ctx, diff.post())?]))
			.collect::<Result<Vec<_>>>()?;
		self.core.apply(ctx, ExternCChangeView::new(input, &values))
	}

	fn on_timer(&mut self, ctx: &mut ExternCContext, timer: Timer<'_>) -> Result<()> {
		self.core.on_timer(ctx, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.core.sample()
	}
}

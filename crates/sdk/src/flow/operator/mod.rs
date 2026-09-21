// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod change;
pub mod column;
pub mod context;
pub mod dictionary;
pub mod diff;
pub mod extern_c;
pub mod state;
pub mod timer;
pub mod view;
pub mod view_column;
pub mod windowed;

use reifydb_core::{
	common::{WindowRequirements, WindowSizeDomain},
	error::CoreError,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	operator_with::ApplyWith,
};
use reifydb_value::{config::ExtensionParams, error::Error as ValueError};

use crate::{
	error::Result,
	flow::operator::{
		column::operator::OperatorColumn,
		context::{ClassValue, GuestContext, Managed, Nostate, Unmanaged},
		timer::Timer,
		view::ChangeView,
	},
};

pub trait OperatorMetadata {
	const NAME: &'static str;
	const VERSION: &'static str;
	const DESCRIPTION: &'static str;
	const INPUT_COLUMNS: &'static [OperatorColumn];
	const OUTPUT_COLUMNS: &'static [OperatorColumn];
	const CAPABILITIES: &'static [OperatorCapability];
}

pub trait ManagedOperator: OperatorMetadata + Send + Sync + Sized {
	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self>;

	fn apply(&mut self, ctx: &mut impl GuestContext<Managed>, change: impl ChangeView) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut impl GuestContext<Managed>, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub trait UnmanagedOperator: OperatorMetadata + Send + Sync + Sized {
	const UNMANAGED_BECAUSE: &'static str;

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self>;

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut impl GuestContext<Unmanaged>, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub trait NostateOperator: OperatorMetadata + Send + Sync + Sized {
	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self>;

	fn apply(&mut self, ctx: &mut impl GuestContext<Nostate>, change: impl ChangeView) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut impl GuestContext<Nostate>, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

#[doc(hidden)]
pub trait MountedOperator: Send + Sync + Sized {
	type Class: ClassValue;

	const WINDOW: WindowRequirements;

	const UNMANAGED_BECAUSE: Option<&'static str>;

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self>;

	fn apply(&mut self, ctx: &mut impl GuestContext<Self::Class>, change: impl ChangeView) -> Result<()>;

	fn on_timer(&mut self, _ctx: &mut impl GuestContext<Self::Class>, _timer: Timer<'_>) -> Result<()> {
		Ok(())
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}
}

pub struct ManagedMount<T>(T);

impl<T> ManagedMount<T> {
	pub fn new(operator: T) -> Self {
		Self(operator)
	}
}

impl<T: OperatorMetadata> OperatorMetadata for ManagedMount<T> {
	const NAME: &'static str = T::NAME;
	const VERSION: &'static str = T::VERSION;
	const DESCRIPTION: &'static str = T::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = T::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = T::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = T::CAPABILITIES;
}

impl<T: ManagedOperator> MountedOperator for ManagedMount<T> {
	type Class = Managed;

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	const UNMANAGED_BECAUSE: Option<&'static str> = None;

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		with.reject_window()?;
		if with.lateness_duration()?.is_none_or(|lateness| lateness.is_zero()) {
			return Err(ValueError::from(CoreError::OperatorLatenessRequired).into());
		}
		Ok(Self(T::create(operator_id, params, with)?))
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Managed>, change: impl ChangeView) -> Result<()> {
		self.0.apply(ctx, change)
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext<Managed>, timer: Timer<'_>) -> Result<()> {
		self.0.on_timer(ctx, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.0.sample()
	}
}

pub struct UnmanagedMount<T>(T);

impl<T> UnmanagedMount<T> {
	pub fn new(operator: T) -> Self {
		Self(operator)
	}
}

impl<T: OperatorMetadata> OperatorMetadata for UnmanagedMount<T> {
	const NAME: &'static str = T::NAME;
	const VERSION: &'static str = T::VERSION;
	const DESCRIPTION: &'static str = T::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = T::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = T::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = T::CAPABILITIES;
}

impl<T: UnmanagedOperator> MountedOperator for UnmanagedMount<T> {
	type Class = Unmanaged;

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	const UNMANAGED_BECAUSE: Option<&'static str> = Some(T::UNMANAGED_BECAUSE);

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		Ok(Self(T::create(operator_id, params, with)?))
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Unmanaged>, change: impl ChangeView) -> Result<()> {
		self.0.apply(ctx, change)
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext<Unmanaged>, timer: Timer<'_>) -> Result<()> {
		self.0.on_timer(ctx, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.0.sample()
	}
}

pub struct NostateMount<T>(T);

impl<T> NostateMount<T> {
	pub fn new(operator: T) -> Self {
		Self(operator)
	}
}

impl<T: OperatorMetadata> OperatorMetadata for NostateMount<T> {
	const NAME: &'static str = T::NAME;
	const VERSION: &'static str = T::VERSION;
	const DESCRIPTION: &'static str = T::DESCRIPTION;
	const INPUT_COLUMNS: &'static [OperatorColumn] = T::INPUT_COLUMNS;
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = T::OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = T::CAPABILITIES;
}

impl<T: NostateOperator> MountedOperator for NostateMount<T> {
	type Class = Nostate;

	const WINDOW: WindowRequirements = WindowRequirements {
		takes_window: false,
		kinds: &[],
		domain: WindowSizeDomain::Time,
		needs_pane: false,
	};

	const UNMANAGED_BECAUSE: Option<&'static str> = None;

	fn create(operator_id: OperatorId, params: &ExtensionParams, with: &ApplyWith) -> Result<Self> {
		if *with != ApplyWith::default() {
			return Err(ValueError::from(CoreError::OperatorWithNotAccepted).into());
		}
		Ok(Self(T::create(operator_id, params, with)?))
	}

	fn apply(&mut self, ctx: &mut impl GuestContext<Nostate>, change: impl ChangeView) -> Result<()> {
		self.0.apply(ctx, change)
	}

	fn on_timer(&mut self, ctx: &mut impl GuestContext<Nostate>, timer: Timer<'_>) -> Result<()> {
		self.0.on_timer(ctx, timer)
	}

	fn sample(&self) -> Option<OperatorSample> {
		self.0.sample()
	}
}

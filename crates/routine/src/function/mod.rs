// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

pub mod error;
pub mod registry;

pub mod blob;
pub mod clock;
pub mod date;
pub mod datetime;
pub mod duration;
pub mod flow;
pub mod identity;
pub mod is;
pub mod json;
pub mod math;
pub mod meta;
pub mod rql;
pub mod series;
pub mod text;
pub mod time;
pub mod uuid;

use std::sync::Arc;

use error::FunctionError;
use reifydb_core::value::column::{
	Column,
	columns::Columns,
	data::ColumnData,
	view::group_by::{GroupByView, GroupKey},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		identity::IdentityId,
		r#type::{Type, input_types::InputTypes},
	},
};

use crate::function::uuid::{v4::UuidV4, v7::UuidV7};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FunctionCapability {
	Scalar,
	Aggregate,
	Generator,
}

#[derive(Debug, Clone)]
pub struct FunctionInfo {
	pub name: String,
	pub description: Option<String>,
}

impl FunctionInfo {
	pub fn new(name: &str) -> Self {
		Self {
			name: name.to_string(),
			description: None,
		}
	}
}

pub struct FunctionContext<'a> {
	pub fragment: Fragment,
	pub runtime_context: &'a RuntimeContext,
	pub identity: IdentityId,
	pub row_count: usize,
}

impl<'a> FunctionContext<'a> {
	pub fn new(
		fragment: Fragment,
		runtime_context: &'a RuntimeContext,
		identity: IdentityId,
		row_count: usize,
	) -> Self {
		Self {
			fragment,
			runtime_context,
			identity,
			row_count,
		}
	}
}

pub trait Function: Send + Sync {
	fn info(&self) -> &FunctionInfo;
	fn capabilities(&self) -> &[FunctionCapability];

	fn return_type(&self, input_types: &[Type]) -> Type;
	fn accepted_types(&self) -> InputTypes {
		InputTypes::any()
	}

	fn propagates_options(&self) -> bool {
		true
	}

	fn execute(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError>;

	/// Calls the function, automatically propagating Option columns if
	/// `propagates_options()` returns true.
	fn call(&self, ctx: &FunctionContext, args: &Columns) -> Result<Columns, FunctionError> {
		if !self.propagates_options() {
			return self.execute(ctx, args);
		}

		let has_option = args.iter().any(|c| matches!(c.data(), ColumnData::Option { .. }));
		if !has_option {
			return self.execute(ctx, args);
		}

		let mut combined_bv: Option<BitVec> = None;
		let mut unwrapped = Vec::with_capacity(args.len());
		for col in args.iter() {
			let (inner, bv) = col.data().unwrap_option();
			if let Some(bv) = bv {
				combined_bv = Some(match combined_bv {
					Some(existing) => existing.and(bv),
					None => bv.clone(),
				});
			}
			unwrapped.push(Column::new(col.name().clone(), inner.clone()));
		}

		// Short-circuit: when all combined values are None, skip the inner function
		// call entirely to avoid type-validation errors on placeholder inner types.
		if let Some(ref bv) = combined_bv
			&& bv.count_ones() == 0
		{
			let row_count = args.row_count();
			let input_types: Vec<Type> = unwrapped.iter().map(|c| c.data.get_type()).collect();
			let result_type = self.return_type(&input_types);
			let result_data = ColumnData::none_typed(result_type, row_count);
			return Ok(Columns::new(vec![Column::new(ctx.fragment.clone(), result_data)]));
		}

		let unwrapped_args = Columns::new(unwrapped);
		let result = self.execute(ctx, &unwrapped_args)?;

		match combined_bv {
			Some(bv) => {
				let wrapped_cols: Vec<Column> = result
					.into_iter()
					.map(|col| {
						Column::new(
							col.name,
							ColumnData::Option {
								inner: Box::new(col.data),
								bitvec: bv.clone(),
							},
						)
					})
					.collect();
				Ok(Columns::new(wrapped_cols))
			}
			None => Ok(result),
		}
	}

	fn accumulator(&self, _ctx: &FunctionContext) -> Option<Box<dyn Accumulator>> {
		None
	}
}

pub trait Accumulator: Send + Sync {
	fn update(&mut self, args: &Columns, groups: &GroupByView) -> Result<(), FunctionError>;
	fn finalize(&mut self) -> Result<(Vec<GroupKey>, ColumnData), FunctionError>;
}

pub fn default_functions() -> registry::FunctionsConfigurator {
	registry::Functions::builder()
		.register_function(Arc::new(math::sum::Sum::new()))
		.register_function(Arc::new(math::avg::Avg::new()))
		.register_function(Arc::new(math::count::Count::new()))
		.register_function(Arc::new(math::min::Min::new()))
		.register_function(Arc::new(math::max::Max::new()))
		.register_function(Arc::new(math::abs::Abs::new()))
		.register_function(Arc::new(flow::to_json::FlowNodeToJson::new()))
		.register_function(Arc::new(clock::now::Now::new()))
		.register_function(Arc::new(blob::b58::BlobB58::new()))
		.register_function(Arc::new(blob::b64::BlobB64::new()))
		.register_function(Arc::new(blob::b64url::BlobB64url::new()))
		.register_function(Arc::new(blob::hex::BlobHex::new()))
		.register_function(Arc::new(blob::utf8::BlobUtf8::new()))
		.register_function(Arc::new(math::acos::Acos::new()))
		.register_function(Arc::new(math::asin::Asin::new()))
		.register_function(Arc::new(math::atan::Atan::new()))
		.register_function(Arc::new(math::atan2::Atan2::new()))
		.register_function(Arc::new(math::ceil::Ceil::new()))
		.register_function(Arc::new(math::clamp::Clamp::new()))
		.register_function(Arc::new(math::cos::Cos::new()))
		.register_function(Arc::new(math::euler::Euler::new()))
		.register_function(Arc::new(math::exp::Exp::new()))
		.register_function(Arc::new(math::floor::Floor::new()))
		.register_function(Arc::new(math::gcd::Gcd::new()))
		.register_function(Arc::new(math::lcm::Lcm::new()))
		.register_function(Arc::new(math::log::Log::new()))
		.register_function(Arc::new(math::log10::Log10::new()))
		.register_function(Arc::new(math::log2::Log2::new()))
		.register_function(Arc::new(math::modulo::Modulo::new()))
		.register_function(Arc::new(math::pi::Pi::new()))
		.register_function(Arc::new(math::power::Power::new()))
		.register_function(Arc::new(math::round::Round::new()))
		.register_function(Arc::new(math::sign::Sign::new()))
		.register_function(Arc::new(math::sin::Sin::new()))
		.register_function(Arc::new(math::sqrt::Sqrt::new()))
		.register_function(Arc::new(math::tan::Tan::new()))
		.register_function(Arc::new(math::truncate::Truncate::new()))
		.register_function(Arc::new(date::year::DateYear::new()))
		.register_function(Arc::new(date::month::DateMonth::new()))
		.register_function(Arc::new(date::day::DateDay::new()))
		.register_function(Arc::new(date::day_of_year::DateDayOfYear::new()))
		.register_function(Arc::new(date::day_of_week::DateDayOfWeek::new()))
		.register_function(Arc::new(date::quarter::DateQuarter::new()))
		.register_function(Arc::new(date::week::DateWeek::new()))
		.register_function(Arc::new(date::is_leap_year::DateIsLeapYear::new()))
		.register_function(Arc::new(date::days_in_month::DateDaysInMonth::new()))
		.register_function(Arc::new(date::end_of_month::DateEndOfMonth::new()))
		.register_function(Arc::new(date::start_of_month::DateStartOfMonth::new()))
		.register_function(Arc::new(date::start_of_year::DateStartOfYear::new()))
		.register_function(Arc::new(date::new::DateNew::new()))
		.register_function(Arc::new(date::now::DateNow::new()))
		.register_function(Arc::new(date::add::DateAdd::new()))
		.register_function(Arc::new(date::subtract::DateSubtract::new()))
		.register_function(Arc::new(date::diff::DateDiff::new()))
		.register_function(Arc::new(date::trunc::DateTrunc::new()))
		.register_function(Arc::new(date::age::DateAge::new()))
		.register_function(Arc::new(date::format::DateFormat::new()))
		.register_function(Arc::new(time::hour::TimeHour::new()))
		.register_function(Arc::new(time::minute::TimeMinute::new()))
		.register_function(Arc::new(time::second::TimeSecond::new()))
		.register_function(Arc::new(time::nanosecond::TimeNanosecond::new()))
		.register_function(Arc::new(time::new::TimeNew::new()))
		.register_function(Arc::new(time::now::TimeNow::new()))
		.register_function(Arc::new(time::add::TimeAdd::new()))
		.register_function(Arc::new(time::subtract::TimeSubtract::new()))
		.register_function(Arc::new(time::diff::TimeDiff::new()))
		.register_function(Arc::new(time::trunc::TimeTrunc::new()))
		.register_function(Arc::new(time::age::TimeAge::new()))
		.register_function(Arc::new(time::format::TimeFormat::new()))
		.register_function(Arc::new(datetime::year::DateTimeYear::new()))
		.register_function(Arc::new(datetime::month::DateTimeMonth::new()))
		.register_function(Arc::new(datetime::day::DateTimeDay::new()))
		.register_function(Arc::new(datetime::hour::DateTimeHour::new()))
		.register_function(Arc::new(datetime::minute::DateTimeMinute::new()))
		.register_function(Arc::new(datetime::second::DateTimeSecond::new()))
		.register_function(Arc::new(datetime::nanosecond::DateTimeNanosecond::new()))
		.register_function(Arc::new(datetime::day_of_year::DateTimeDayOfYear::new()))
		.register_function(Arc::new(datetime::day_of_week::DateTimeDayOfWeek::new()))
		.register_function(Arc::new(datetime::quarter::DateTimeQuarter::new()))
		.register_function(Arc::new(datetime::week::DateTimeWeek::new()))
		.register_function(Arc::new(datetime::date::DateTimeDate::new()))
		.register_function(Arc::new(datetime::time::DateTimeTime::new()))
		.register_function(Arc::new(datetime::epoch::DateTimeEpoch::new()))
		.register_function(Arc::new(datetime::epoch_millis::DateTimeEpochMillis::new()))
		.register_function(Arc::new(datetime::new::DateTimeNew::new()))
		.register_function(Arc::new(datetime::now::DateTimeNow::new()))
		.register_function(Arc::new(datetime::from_epoch::DateTimeFromEpoch::new()))
		.register_function(Arc::new(datetime::from_epoch_millis::DateTimeFromEpochMillis::new()))
		.register_function(Arc::new(datetime::add::DateTimeAdd::new()))
		.register_function(Arc::new(datetime::subtract::DateTimeSubtract::new()))
		.register_function(Arc::new(datetime::diff::DateTimeDiff::new()))
		.register_function(Arc::new(datetime::trunc::DateTimeTrunc::new()))
		.register_function(Arc::new(datetime::age::DateTimeAge::new()))
		.register_function(Arc::new(datetime::format::DateTimeFormat::new()))
		.register_function(Arc::new(duration::years::DurationYears::new()))
		.register_function(Arc::new(duration::months::DurationMonths::new()))
		.register_function(Arc::new(duration::weeks::DurationWeeks::new()))
		.register_function(Arc::new(duration::days::DurationDays::new()))
		.register_function(Arc::new(duration::hours::DurationHours::new()))
		.register_function(Arc::new(duration::minutes::DurationMinutes::new()))
		.register_function(Arc::new(duration::seconds::DurationSeconds::new()))
		.register_function(Arc::new(duration::millis::DurationMillis::new()))
		.register_function(Arc::new(duration::get_months::DurationGetMonths::new()))
		.register_function(Arc::new(duration::get_days::DurationGetDays::new()))
		.register_function(Arc::new(duration::get_nanos::DurationGetNanos::new()))
		.register_function(Arc::new(duration::add::DurationAdd::new()))
		.register_function(Arc::new(duration::subtract::DurationSubtract::new()))
		.register_function(Arc::new(duration::negate::DurationNegate::new()))
		.register_function(Arc::new(duration::scale::DurationScale::new()))
		.register_function(Arc::new(duration::trunc::DurationTrunc::new()))
		.register_function(Arc::new(duration::format::DurationFormat::new()))
		.register_alias("duration::year", "duration::years")
		.register_alias("duration::month", "duration::months")
		.register_alias("duration::week", "duration::weeks")
		.register_alias("duration::day", "duration::days")
		.register_alias("duration::hour", "duration::hours")
		.register_alias("duration::minute", "duration::minutes")
		.register_alias("duration::second", "duration::seconds")
		.register_function(Arc::new(text::ascii::TextAscii::new()))
		.register_function(Arc::new(text::char::TextChar::new()))
		.register_function(Arc::new(text::concat::TextConcat::new()))
		.register_function(Arc::new(text::contains::TextContains::new()))
		.register_function(Arc::new(text::count::TextCount::new()))
		.register_function(Arc::new(text::ends_with::TextEndsWith::new()))
		.register_function(Arc::new(text::index_of::TextIndexOf::new()))
		.register_function(Arc::new(text::pad_left::TextPadLeft::new()))
		.register_function(Arc::new(text::pad_right::TextPadRight::new()))
		.register_function(Arc::new(text::repeat::TextRepeat::new()))
		.register_function(Arc::new(text::replace::TextReplace::new()))
		.register_function(Arc::new(text::reverse::TextReverse::new()))
		.register_function(Arc::new(text::starts_with::TextStartsWith::new()))
		.register_function(Arc::new(text::length::TextLength::new()))
		.register_function(Arc::new(text::trim::TextTrim::new()))
		.register_function(Arc::new(text::trim_end::TextTrimEnd::new()))
		.register_function(Arc::new(text::trim_start::TextTrimStart::new()))
		.register_function(Arc::new(text::upper::TextUpper::new()))
		.register_function(Arc::new(text::lower::TextLower::new()))
		.register_function(Arc::new(text::substring::TextSubstring::new()))
		.register_function(Arc::new(text::format_bytes::FormatBytes::new()))
		.register_function(Arc::new(text::format_bytes_si::FormatBytesSi::new()))
		.register_function(Arc::new(meta::r#type::Type::new()))
		.register_function(Arc::new(identity::id::Id::new()))
		.register_function(Arc::new(is::some::IsSome::new()))
		.register_function(Arc::new(is::none::IsNone::new()))
		.register_function(Arc::new(is::r#type::IsType::new()))
		.register_function(Arc::new(is::root::IsRoot::new()))
		.register_function(Arc::new(is::anonymous::IsAnonymous::new()))
		.register_function(Arc::new(json::object::JsonObject::new()))
		.register_function(Arc::new(json::array::JsonArray::new()))
		.register_function(Arc::new(json::pretty::JsonPretty::new()))
		.register_function(Arc::new(json::serialize::JsonSerialize::new()))
		.register_function(Arc::new(UuidV4::new()))
		.register_function(Arc::new(UuidV7::new()))
		.register_function(Arc::new(series::Series::new()))
		.register_function(Arc::new(series::GenerateSeries::new()))
		.register_function(Arc::new(rql::fingerprint::RqlFingerprint::new()))
}

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
pub mod subscription;
pub mod testing;
pub mod text;
pub mod time;
pub mod uuid;

use error::{AggregateFunctionResult, GeneratorFunctionResult, ScalarFunctionResult};
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	util::ioc::IocContainer,
	value::column::{
		Column,
		columns::Columns,
		data::ColumnData,
		view::group_by::{GroupByView, GroupKey},
	},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	fragment::Fragment,
	util::bitvec::BitVec,
	value::{
		identity::IdentityId,
		r#type::{Type, input_types::InputTypes},
	},
};

use self::uuid::{v4::UuidV4, v7::UuidV7};

pub struct GeneratorContext<'a> {
	pub fragment: Fragment,
	pub params: Columns,
	pub txn: &'a mut Transaction<'a>,
	pub catalog: &'a Catalog,
	pub identity: IdentityId,
	pub ioc: &'a IocContainer,
}

pub trait GeneratorFunction: Send + Sync {
	fn generate<'a>(&self, ctx: GeneratorContext<'a>) -> GeneratorFunctionResult<Columns>;
}

pub struct ScalarFunctionContext<'a> {
	pub fragment: Fragment,
	pub columns: &'a Columns,
	pub row_count: usize,
	pub runtime_context: &'a RuntimeContext,
	pub identity: IdentityId,
}

pub trait ScalarFunction: Send + Sync {
	fn scalar<'a>(&'a self, ctx: ScalarFunctionContext<'a>) -> ScalarFunctionResult<ColumnData>;
	fn return_type(&self, input_types: &[Type]) -> Type;
}

pub struct AggregateFunctionContext<'a> {
	pub fragment: Fragment,
	pub column: &'a Column,
	pub groups: &'a GroupByView,
}

pub trait AggregateFunction: Send + Sync {
	fn aggregate<'a>(&'a mut self, ctx: AggregateFunctionContext<'a>) -> AggregateFunctionResult<()>;
	fn finalize(&mut self) -> AggregateFunctionResult<(Vec<GroupKey>, ColumnData)>;
	fn return_type(&self, input_type: &Type) -> Type;
	fn accepted_types(&self) -> InputTypes;
}

/// Helper for scalar functions to opt into Option propagation.
///
/// If any argument column is `ColumnData::Option`,
/// this unwraps the Option wrappers, calls `func.scalar()` recursively on the
/// inner data, and re-wraps the result with the combined bitvec.
///
/// Returns `None` when no Option columns are present (the caller should
/// proceed with its normal typed logic).
///
/// Functions that need raw access to Options (e.g. `is::some`, `is::none`)
/// simply don't call this helper.
pub fn propagate_options(
	func: &dyn ScalarFunction,
	ctx: &ScalarFunctionContext,
) -> Option<ScalarFunctionResult<ColumnData>> {
	let has_option = ctx.columns.iter().any(|c| matches!(c.data(), ColumnData::Option { .. }));
	if !has_option {
		return None;
	}

	let mut combined_bv: Option<BitVec> = None;
	let mut unwrapped = Vec::with_capacity(ctx.columns.len());
	for col in ctx.columns.iter() {
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
	// call entirely to avoid type-validation errors on placeholder inner types
	// (e.g. none typed as Option<Any> would fail numeric type checks).
	if let Some(ref bv) = combined_bv
		&& bv.count_ones() == 0
	{
		let input_types: Vec<Type> = unwrapped.iter().map(|c| c.data().get_type()).collect();
		let result_type = func.return_type(&input_types);
		return Some(Ok(ColumnData::none_typed(result_type, ctx.row_count)));
	}

	let unwrapped_columns = Columns::new(unwrapped);
	let result = func.scalar(ScalarFunctionContext {
		fragment: ctx.fragment.clone(),
		columns: &unwrapped_columns,
		row_count: ctx.row_count,
		runtime_context: ctx.runtime_context,
		identity: ctx.identity,
	});

	Some(result.map(|data| match combined_bv {
		Some(bv) => ColumnData::Option {
			inner: Box::new(data),
			bitvec: bv,
		},
		None => data,
	}))
}

pub fn default_functions() -> registry::FunctionsBuilder {
	let builder = registry::Functions::builder()
		.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
		.register_aggregate("math::min", math::aggregate::min::Min::new)
		.register_aggregate("math::max", math::aggregate::max::Max::new)
		.register_aggregate("math::avg", math::aggregate::avg::Avg::new)
		.register_aggregate("math::count", math::aggregate::count::Count::new)
		.register_scalar("flow_node::to_json", flow::to_json::FlowNodeToJson::new)
		.register_scalar("clock::now", clock::now::Now::new)
		.register_scalar("blob::b58", blob::b58::BlobB58::new)
		.register_scalar("blob::b64", blob::b64::BlobB64::new)
		.register_scalar("blob::b64url", blob::b64url::BlobB64url::new)
		.register_scalar("blob::hex", blob::hex::BlobHex::new)
		.register_scalar("blob::utf8", blob::utf8::BlobUtf8::new)
		.register_scalar("math::abs", math::scalar::abs::Abs::new)
		.register_scalar("math::acos", math::scalar::acos::Acos::new)
		.register_scalar("math::asin", math::scalar::asin::Asin::new)
		.register_scalar("math::atan", math::scalar::atan::Atan::new)
		.register_scalar("math::atan2", math::scalar::atan2::Atan2::new)
		.register_scalar("math::avg", math::scalar::avg::Avg::new)
		.register_scalar("math::ceil", math::scalar::ceil::Ceil::new)
		.register_scalar("math::clamp", math::scalar::clamp::Clamp::new)
		.register_scalar("math::cos", math::scalar::cos::Cos::new)
		.register_scalar("math::e", math::scalar::euler::Euler::new)
		.register_scalar("math::exp", math::scalar::exp::Exp::new)
		.register_scalar("math::floor", math::scalar::floor::Floor::new)
		.register_scalar("math::gcd", math::scalar::gcd::Gcd::new)
		.register_scalar("math::lcm", math::scalar::lcm::Lcm::new)
		.register_scalar("math::log", math::scalar::log::Log::new)
		.register_scalar("math::log10", math::scalar::log10::Log10::new)
		.register_scalar("math::log2", math::scalar::log2::Log2::new)
		.register_scalar("math::max", math::scalar::max::Max::new)
		.register_scalar("math::min", math::scalar::min::Min::new)
		.register_scalar("math::mod", math::scalar::modulo::Modulo::new)
		.register_scalar("math::pi", math::scalar::pi::Pi::new)
		.register_scalar("math::power", math::scalar::power::Power::new)
		.register_scalar("math::round", math::scalar::round::Round::new)
		.register_scalar("math::sign", math::scalar::sign::Sign::new)
		.register_scalar("math::sin", math::scalar::sin::Sin::new)
		.register_scalar("math::sqrt", math::scalar::sqrt::Sqrt::new)
		.register_scalar("math::tan", math::scalar::tan::Tan::new)
		.register_scalar("math::truncate", math::scalar::truncate::Truncate::new)
		.register_scalar("date::year", date::year::DateYear::new)
		.register_scalar("date::month", date::month::DateMonth::new)
		.register_scalar("date::day", date::day::DateDay::new)
		.register_scalar("date::day_of_year", date::day_of_year::DateDayOfYear::new)
		.register_scalar("date::day_of_week", date::day_of_week::DateDayOfWeek::new)
		.register_scalar("date::quarter", date::quarter::DateQuarter::new)
		.register_scalar("date::week", date::week::DateWeek::new)
		.register_scalar("date::is_leap_year", date::is_leap_year::DateIsLeapYear::new)
		.register_scalar("date::days_in_month", date::days_in_month::DateDaysInMonth::new)
		.register_scalar("date::end_of_month", date::end_of_month::DateEndOfMonth::new)
		.register_scalar("date::start_of_month", date::start_of_month::DateStartOfMonth::new)
		.register_scalar("date::start_of_year", date::start_of_year::DateStartOfYear::new)
		.register_scalar("date::new", date::new::DateNew::new)
		.register_scalar("date::now", date::now::DateNow::new)
		.register_scalar("date::add", date::add::DateAdd::new)
		.register_scalar("date::subtract", date::subtract::DateSubtract::new)
		.register_scalar("date::diff", date::diff::DateDiff::new)
		.register_scalar("date::trunc", date::trunc::DateTrunc::new)
		.register_scalar("date::age", date::age::DateAge::new)
		.register_scalar("date::format", date::format::DateFormat::new)
		.register_scalar("time::hour", time::hour::TimeHour::new)
		.register_scalar("time::minute", time::minute::TimeMinute::new)
		.register_scalar("time::second", time::second::TimeSecond::new)
		.register_scalar("time::nanosecond", time::nanosecond::TimeNanosecond::new)
		.register_scalar("time::new", time::new::TimeNew::new)
		.register_scalar("time::now", time::now::TimeNow::new)
		.register_scalar("time::add", time::add::TimeAdd::new)
		.register_scalar("time::subtract", time::subtract::TimeSubtract::new)
		.register_scalar("time::diff", time::diff::TimeDiff::new)
		.register_scalar("time::trunc", time::trunc::TimeTrunc::new)
		.register_scalar("time::age", time::age::TimeAge::new)
		.register_scalar("time::format", time::format::TimeFormat::new)
		.register_scalar("datetime::year", datetime::year::DateTimeYear::new)
		.register_scalar("datetime::month", datetime::month::DateTimeMonth::new)
		.register_scalar("datetime::day", datetime::day::DateTimeDay::new)
		.register_scalar("datetime::hour", datetime::hour::DateTimeHour::new)
		.register_scalar("datetime::minute", datetime::minute::DateTimeMinute::new)
		.register_scalar("datetime::second", datetime::second::DateTimeSecond::new)
		.register_scalar("datetime::nanosecond", datetime::nanosecond::DateTimeNanosecond::new)
		.register_scalar("datetime::day_of_year", datetime::day_of_year::DateTimeDayOfYear::new)
		.register_scalar("datetime::day_of_week", datetime::day_of_week::DateTimeDayOfWeek::new)
		.register_scalar("datetime::quarter", datetime::quarter::DateTimeQuarter::new)
		.register_scalar("datetime::week", datetime::week::DateTimeWeek::new)
		.register_scalar("datetime::date", datetime::date::DateTimeDate::new)
		.register_scalar("datetime::time", datetime::time::DateTimeTime::new)
		.register_scalar("datetime::epoch", datetime::epoch::DateTimeEpoch::new)
		.register_scalar("datetime::epoch_millis", datetime::epoch_millis::DateTimeEpochMillis::new)
		.register_scalar("datetime::new", datetime::new::DateTimeNew::new)
		.register_scalar("datetime::now", datetime::now::DateTimeNow::new)
		.register_scalar("datetime::from_epoch", datetime::from_epoch::DateTimeFromEpoch::new)
		.register_scalar(
			"datetime::from_epoch_millis",
			datetime::from_epoch_millis::DateTimeFromEpochMillis::new,
		)
		.register_scalar("datetime::add", datetime::add::DateTimeAdd::new)
		.register_scalar("datetime::subtract", datetime::subtract::DateTimeSubtract::new)
		.register_scalar("datetime::diff", datetime::diff::DateTimeDiff::new)
		.register_scalar("datetime::trunc", datetime::trunc::DateTimeTrunc::new)
		.register_scalar("datetime::age", datetime::age::DateTimeAge::new)
		.register_scalar("datetime::format", datetime::format::DateTimeFormat::new)
		.register_scalar("duration::years", duration::years::DurationYears::new)
		.register_scalar("duration::months", duration::months::DurationMonths::new)
		.register_scalar("duration::weeks", duration::weeks::DurationWeeks::new)
		.register_scalar("duration::days", duration::days::DurationDays::new)
		.register_scalar("duration::hours", duration::hours::DurationHours::new)
		.register_scalar("duration::minutes", duration::minutes::DurationMinutes::new)
		.register_scalar("duration::seconds", duration::seconds::DurationSeconds::new)
		.register_scalar("duration::millis", duration::millis::DurationMillis::new)
		.register_scalar("duration::year", duration::years::DurationYears::new)
		.register_scalar("duration::month", duration::months::DurationMonths::new)
		.register_scalar("duration::week", duration::weeks::DurationWeeks::new)
		.register_scalar("duration::day", duration::days::DurationDays::new)
		.register_scalar("duration::hour", duration::hours::DurationHours::new)
		.register_scalar("duration::minute", duration::minutes::DurationMinutes::new)
		.register_scalar("duration::second", duration::seconds::DurationSeconds::new)
		.register_scalar("duration::get_months", duration::get_months::DurationGetMonths::new)
		.register_scalar("duration::get_days", duration::get_days::DurationGetDays::new)
		.register_scalar("duration::get_nanos", duration::get_nanos::DurationGetNanos::new)
		.register_scalar("duration::add", duration::add::DurationAdd::new)
		.register_scalar("duration::subtract", duration::subtract::DurationSubtract::new)
		.register_scalar("duration::negate", duration::negate::DurationNegate::new)
		.register_scalar("duration::scale", duration::scale::DurationScale::new)
		.register_scalar("duration::trunc", duration::trunc::DurationTrunc::new)
		.register_scalar("duration::format", duration::format::DurationFormat::new)
		.register_scalar("text::ascii", text::ascii::TextAscii::new)
		.register_scalar("text::char", text::char::TextChar::new)
		.register_scalar("text::concat", text::concat::TextConcat::new)
		.register_scalar("text::contains", text::contains::TextContains::new)
		.register_scalar("text::count", text::count::TextCount::new)
		.register_scalar("text::ends_with", text::ends_with::TextEndsWith::new)
		.register_scalar("text::index_of", text::index_of::TextIndexOf::new)
		.register_scalar("text::pad_left", text::pad_left::TextPadLeft::new)
		.register_scalar("text::pad_right", text::pad_right::TextPadRight::new)
		.register_scalar("text::repeat", text::repeat::TextRepeat::new)
		.register_scalar("text::replace", text::replace::TextReplace::new)
		.register_scalar("text::reverse", text::reverse::TextReverse::new)
		.register_scalar("text::starts_with", text::starts_with::TextStartsWith::new)
		.register_scalar("text::length", text::length::TextLength::new)
		.register_scalar("text::trim", text::trim::TextTrim::new)
		.register_scalar("text::trim_end", text::trim_end::TextTrimEnd::new)
		.register_scalar("text::trim_start", text::trim_start::TextTrimStart::new)
		.register_scalar("text::upper", text::upper::TextUpper::new)
		.register_scalar("text::lower", text::lower::TextLower::new)
		.register_scalar("text::substring", text::substring::TextSubstring::new)
		.register_scalar("text::format_bytes", text::format_bytes::FormatBytes::new)
		.register_scalar("text::format_bytes_si", text::format_bytes_si::FormatBytesSi::new)
		.register_scalar("meta::type", meta::r#type::Type::new)
		.register_scalar("identity::id", identity::id::Id::new)
		.register_scalar("is::some", is::some::IsSome::new)
		.register_scalar("is::none", is::none::IsNone::new)
		.register_scalar("is::type", is::r#type::IsType::new)
		.register_scalar("is::root", is::root::IsRoot::new)
		.register_scalar("is::anonymous", is::anonymous::IsAnonymous::new)
		.register_scalar("json::object", json::object::JsonObject::new)
		.register_scalar("json::array", json::array::JsonArray::new)
		.register_scalar("json::pretty", json::pretty::JsonPretty::new)
		.register_scalar("json::serialize", json::serialize::JsonSerialize::new)
		.register_scalar("uuid::v4", UuidV4::new)
		.register_scalar("uuid::v7", UuidV7::new)
		.register_scalar("gen::series", series::Series::new)
		.register_generator("generate_series", series::GenerateSeries::new)
		.register_generator("inspect_subscription", subscription::inspect::InspectSubscription::new)
		.register_scalar("rql::fingerprint", rql::fingerprint::RqlFingerprint::new);
	testing::register_testing_functions(builder)
}

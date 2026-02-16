// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, ops::Deref, sync::Arc};

use crate::{AggregateFunction, GeneratorFunction, ScalarFunction};

#[derive(Clone)]
pub struct Functions(Arc<FunctionsInner>);

impl Functions {
	pub fn empty() -> Functions {
		Functions::builder().build()
	}

	pub fn builder() -> FunctionsBuilder {
		FunctionsBuilder(FunctionsInner {
			scalars: HashMap::new(),
			aggregates: HashMap::new(),
			generators: HashMap::new(),
		})
	}

	pub fn defaults() -> FunctionsBuilder {
		use crate::{
			blob, clock, date, datetime, duration, flow, is, math, meta, series, subscription, text, time,
		};

		Functions::builder()
			.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
			.register_aggregate("math::min", math::aggregate::min::Min::new)
			.register_aggregate("math::max", math::aggregate::max::Max::new)
			.register_aggregate("math::avg", math::aggregate::avg::Avg::new)
			.register_aggregate("math::count", math::aggregate::count::Count::new)
			.register_scalar("flow_node::to_json", flow::to_json::FlowNodeToJson::new)
			.register_scalar("clock::now", clock::now::Now::new)
			.register_scalar("clock::set", clock::set::Set::new)
			.register_scalar("clock::advance", clock::advance::Advance::new)
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
			.register_scalar("is::some", is::some::IsSome::new)
			.register_scalar("is::none", is::none::IsNone::new)
			.register_scalar("is::type", is::r#type::IsType::new)
			.register_generator("generate_series", series::GenerateSeries::new)
			.register_generator("inspect_subscription", subscription::inspect::InspectSubscription::new)
	}
}

impl Deref for Functions {
	type Target = FunctionsInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

#[derive(Clone)]
pub struct FunctionsInner {
	scalars: HashMap<String, Arc<dyn Fn() -> Box<dyn ScalarFunction> + Send + Sync>>,
	aggregates: HashMap<String, Arc<dyn Fn() -> Box<dyn AggregateFunction> + Send + Sync>>,
	generators: HashMap<String, Arc<dyn Fn() -> Box<dyn GeneratorFunction> + Send + Sync>>,
}

impl FunctionsInner {
	pub fn get_aggregate(&self, name: &str) -> Option<Box<dyn AggregateFunction>> {
		self.aggregates.get(name).map(|func| func())
	}

	pub fn get_scalar(&self, name: &str) -> Option<Box<dyn ScalarFunction>> {
		self.scalars.get(name).map(|func| func())
	}

	pub fn get_generator(&self, name: &str) -> Option<Box<dyn GeneratorFunction>> {
		self.generators.get(name).map(|func| func())
	}

	pub fn scalar_names(&self) -> Vec<&str> {
		self.scalars.keys().map(|s| s.as_str()).collect()
	}

	pub fn aggregate_names(&self) -> Vec<&str> {
		self.aggregates.keys().map(|s| s.as_str()).collect()
	}

	pub fn generator_names(&self) -> Vec<&str> {
		self.generators.keys().map(|s| s.as_str()).collect()
	}

	pub fn get_scalar_factory(&self, name: &str) -> Option<Arc<dyn Fn() -> Box<dyn ScalarFunction> + Send + Sync>> {
		self.scalars.get(name).cloned()
	}

	pub fn get_aggregate_factory(
		&self,
		name: &str,
	) -> Option<Arc<dyn Fn() -> Box<dyn AggregateFunction> + Send + Sync>> {
		self.aggregates.get(name).cloned()
	}
}

pub struct FunctionsBuilder(FunctionsInner);

impl FunctionsBuilder {
	pub fn register_scalar<F, A>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> A + Send + Sync + 'static,
		A: ScalarFunction + 'static,
	{
		self.0.scalars.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn ScalarFunction>));

		self
	}

	pub fn register_aggregate<F, A>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> A + Send + Sync + 'static,
		A: AggregateFunction + 'static,
	{
		self.0.aggregates
			.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn AggregateFunction>));

		self
	}

	pub fn register_generator<F, G>(mut self, name: &str, init: F) -> Self
	where
		F: Fn() -> G + Send + Sync + 'static,
		G: GeneratorFunction + 'static,
	{
		self.0.generators
			.insert(name.to_string(), Arc::new(move || Box::new(init()) as Box<dyn GeneratorFunction>));

		self
	}

	pub fn build(self) -> Functions {
		Functions(Arc::new(self.0))
	}
}

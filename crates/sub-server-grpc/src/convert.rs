// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::value::decode_value;
use reifydb_core::interface::catalog::subscription::{HydrationConfig, SubscribeOptions};
use reifydb_value::{
	params::Params,
	value::{Value, duration::Duration},
};

use crate::{
	error::GrpcError,
	generated::{self, TypedValue, params::Params as ProtoParamsOneof},
};

pub fn proto_params_to_params(proto: generated::Params) -> Result<Params, GrpcError> {
	match proto.params {
		None => Ok(Params::None),
		Some(ProtoParamsOneof::Positional(pos)) => {
			let values: Result<Vec<Value>, GrpcError> =
				pos.values.into_iter().map(typed_value_to_value).collect();
			Ok(Params::Positional(Arc::new(values?)))
		}
		Some(ProtoParamsOneof::Named(named)) => {
			let map: Result<HashMap<String, Value>, GrpcError> = named
				.values
				.into_iter()
				.map(|(k, tv)| typed_value_to_value(tv).map(|v| (k, v)))
				.collect();
			Ok(Params::Named(Arc::new(map?)))
		}
	}
}

fn typed_value_to_value(tv: TypedValue) -> Result<Value, GrpcError> {
	Ok(decode_value(&tv.encoded)?)
}

pub fn proto_subscribe_options(proto: Option<generated::SubscribeOptions>) -> Result<SubscribeOptions, String> {
	let Some(proto) = proto else {
		return Ok(SubscribeOptions::default());
	};
	Ok(SubscribeOptions {
		hydration: proto.hydration.map_or_else(HydrationConfig::default, |hydration| HydrationConfig {
			enabled: hydration.enabled.unwrap_or(HydrationConfig::default().enabled),
			max_rows: hydration.max_rows,
		}),
		throttle: proto.throttle.map(|throttle| typed_value_to_duration("throttle", throttle)).transpose()?,
		linger: proto.linger.map(|linger| typed_value_to_duration("linger", linger)).transpose()?,
	})
}

fn typed_value_to_duration(option: &str, tv: TypedValue) -> Result<Duration, String> {
	match decode_value(&tv.encoded).map_err(|e| format!("option {option}: {e}"))? {
		Value::Duration(duration) => Ok(duration),
		other => Err(format!("option {option}: expected Duration, got {}", other.get_type())),
	}
}

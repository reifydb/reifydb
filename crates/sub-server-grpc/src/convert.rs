// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_codec::value::decode_value;
use reifydb_value::{params::Params, value::Value};

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

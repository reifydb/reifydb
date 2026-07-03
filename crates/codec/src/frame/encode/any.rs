// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

use crate::{error::EncodeError, value::encode_value_into};

pub(crate) fn encode_any_value(val: &Value, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
	encode_value_into(val, buf)
}

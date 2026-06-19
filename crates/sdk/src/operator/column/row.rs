// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::data::column::ColumnTypeCode;

use crate::{
	error::SdkError,
	operator::{column::sink::RowSink, view::RowView},
};

pub trait Row: Sized {
	const COLUMNS: &'static [(&'static str, ColumnTypeCode)];
	const AVG_VAR_BYTES: usize = 0;

	fn encode_into<S: RowSink>(&self, sink: &mut S) -> Result<(), SdkError>;
	fn decode_from<V: RowView>(view: &V) -> Option<Self>;
}

#[doc(hidden)]
#[macro_export]
macro_rules! __row_body {
	($($fname:ident : $fty:ty),+ $(,)?) => {
		const COLUMNS: &'static [(&'static str, reifydb_abi::data::column::ColumnTypeCode)] = &[
			$((stringify!($fname), <$fty as $crate::operator::column::cell::Cell>::COLUMN_TYPE),)+
		];

		const AVG_VAR_BYTES: usize = 0 $(+ <$fty as $crate::operator::column::cell::Cell>::AVG_BYTES)+;

		fn encode_into<__S: $crate::operator::column::sink::RowSink>(&self, e: &mut __S) -> Result<(), $crate::error::SdkError> {
			let mut __col = 0usize;
			$(
				<$fty as $crate::operator::column::cell::Cell>::encode(&self.$fname, e, __col)?;
				__col += 1;
			)+
			let _ = __col;
			Ok(())
		}

		fn decode_from<__V: $crate::operator::view::RowView>(view: &__V) -> Option<Self> {
			Some(Self {
				$($fname: <$fty as $crate::operator::column::cell::Cell>::decode(view, stringify!($fname))?,)+
			})
		}
	};
}

#[macro_export]
macro_rules! row {
	(impl ( $($gp:tt)+ ) for $name:ident<$($ga:ident),+ $(,)?> { $($fname:ident : $fty:ty),+ $(,)? }) => {
		impl $($gp)+ $crate::operator::column::row::Row for $name<$($ga),+> {
			$crate::__row_body!($($fname : $fty),+);
		}
	};
	($name:ident { $($fname:ident : $fty:ty),+ $(,)? }) => {
		impl $crate::operator::column::row::Row for $name {
			$crate::__row_body!($($fname : $fty),+);
		}
	};
}

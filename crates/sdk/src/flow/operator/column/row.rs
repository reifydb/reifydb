// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::tag::ValueKind;

use crate::{
	error::SdkError,
	flow::operator::{column::sink::RowSink, view::RowView},
};

pub trait Row: Sized {
	const COLUMNS: &'static [(&'static str, ValueKind)];
	const AVG_VAR_BYTES: usize = 0;

	fn encode_into<S: RowSink>(&self, sink: &mut S) -> Result<(), SdkError>;
	fn decode_from<V: RowView>(view: &V) -> Option<Self>;
}

pub trait OutputRows {
	type Key: Ord;
	type Row: Row;

	fn rows(self) -> impl Iterator<Item = (Option<Self::Key>, Self::Row)>;
}

impl<T: Row> OutputRows for T {
	type Key = ();
	type Row = T;

	fn rows(self) -> impl Iterator<Item = (Option<()>, T)> {
		std::iter::once((None, self))
	}
}

impl<K: Ord, R: Row> OutputRows for BTreeMap<K, R> {
	type Key = K;
	type Row = R;

	fn rows(self) -> impl Iterator<Item = (Option<K>, R)> {
		self.into_iter().map(|(key, row)| (Some(key), row))
	}
}

#[doc(hidden)]
#[macro_export]
macro_rules! __row_body {
	($($fname:ident : $fty:ty),+ $(,)?) => {
		const COLUMNS: &'static [(&'static str, ::reifydb_codec::tag::ValueKind)] = &[
			$((stringify!($fname), <$fty as $crate::flow::operator::column::cell::Cell>::COLUMN_TYPE),)+
		];

		const AVG_VAR_BYTES: usize = 0 $(+ <$fty as $crate::flow::operator::column::cell::Cell>::AVG_BYTES)+;

		fn encode_into<__S: $crate::flow::operator::column::sink::RowSink>(&self, e: &mut __S) -> Result<(), $crate::error::SdkError> {
			let mut __col = 0usize;
			$(
				<$fty as $crate::flow::operator::column::cell::Cell>::encode(&self.$fname, e, __col)?;
				__col += 1;
			)+
			let _ = __col;
			Ok(())
		}

		fn decode_from<__V: $crate::flow::operator::view::RowView>(view: &__V) -> Option<Self> {
			Some(Self {
				$($fname: <$fty as $crate::flow::operator::column::cell::Cell>::decode(view, stringify!($fname))?,)+
			})
		}
	};
}

#[macro_export]
macro_rules! row {
	(impl ( $($gp:tt)+ ) for $name:ident<$($ga:ident),+ $(,)?> { $($fname:ident : $fty:ty),+ $(,)? }) => {
		impl $($gp)+ $crate::flow::operator::column::row::Row for $name<$($ga),+> {
			$crate::__row_body!($($fname : $fty),+);
		}
	};
	($name:ident { $($fname:ident : $fty:ty),+ $(,)? }) => {
		impl $crate::flow::operator::column::row::Row for $name {
			$crate::__row_body!($($fname : $fty),+);
		}
	};
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use super::OutputRows;

	#[derive(Debug, PartialEq)]
	struct Pair {
		a: u64,
		b: u64,
	}

	row!(Pair {
		a: u64,
		b: u64
	});

	#[test]
	fn a_single_row_yields_exactly_one_item_with_no_key() {
		// A single-row operator must not grow a key: the engine keys its row by group and window alone.
		let items: Vec<_> = Pair {
			a: 1,
			b: 2,
		}
		.rows()
		.collect();

		assert_eq!(items.len(), 1);
		assert_eq!(items[0].0, None);
		assert_eq!(
			items[0].1,
			Pair {
				a: 1,
				b: 2
			}
		);
	}

	#[test]
	fn a_keyed_map_yields_one_item_per_entry_in_key_order_each_with_its_key() {
		// Top-k rows are keyed by rank; an unordered or keyless map would swap or merge ranks.
		let mut map = BTreeMap::new();
		for (key, a) in [(3u32, 30u64), (1, 10), (2, 20)] {
			map.insert(
				key,
				Pair {
					a,
					b: 0,
				},
			);
		}

		let items: Vec<_> = map.rows().collect();

		assert_eq!(items.iter().map(|(key, _)| *key).collect::<Vec<_>>(), vec![Some(1), Some(2), Some(3)]);
		assert_eq!(items.iter().map(|(_, row)| row.a).collect::<Vec<_>>(), vec![10, 20, 30]);
	}
}

// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::Value;

/// The reference implementation an operator is differentially tested against.
///
/// The driver feeds the same corpus to the operator and to the model, then bounds the operator's
/// materialized view from below by [`Model::live`] and from above by [`Model::all`].
///
/// The gap between those two bounds is where a model states how much latitude the operator has. A
/// model whose `live` and `all` are the same set makes an exact claim - the view must equal it - and
/// that is the strongest assertion available. A model that separates them permits, for example, a
/// closed window to linger in the view until a tick withdraws it. Both styles run on this one
/// driver; the strength is the model's business, not the harness's.
pub trait Model<R> {
	/// Routes an insert. Returns false when the model considers the row too late to be admitted, so
	/// the driver knows not to offer it for retraction later.
	///
	/// A model must key its contributions by the row's identity rather than matching a retraction on
	/// its values: for a count-based window two rows carrying the same value sit in DIFFERENT
	/// windows, and even for the time-based kinds a value match silently picks an arbitrary one of
	/// several equal contributions, which stops holding once an aggregation is order-sensitive.
	fn admit(&mut self, row: &R) -> bool;

	fn retract(&mut self, row: &R);

	fn advance_ledger(&mut self, at_ms: u64);

	/// Rows the operator MUST be publishing.
	fn live(&self) -> Vec<Vec<Value>>;

	/// Rows the operator MAY be publishing. Never narrower than [`Model::live`].
	fn all(&self) -> Vec<Vec<Value>>;

	/// Rows that must remain once the ledger has run past every horizon and the driver has ticked to
	/// quiescence. Empty for a shape where everything eventually expires; equal to [`Model::all`]
	/// for one where a tick only reclaims state and never withdraws a published row.
	fn after_drain(&self) -> Vec<Vec<Value>>;
}

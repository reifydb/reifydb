// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use super::Config;
use crate::value::duration::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowSealing {
	pub lateness: Option<Duration>,
	pub immutable: Option<Duration>,
}

impl Config {
	pub fn sealing(&self) -> Option<WindowSealing> {
		self.resolve_sealing().ok()
	}

	pub fn validated_sealing(&self) -> WindowSealing {
		match self.resolve_sealing() {
			Ok(sealing) => sealing,
			Err(violation) => panic!("{}: {}", self.name, violation),
		}
	}

	fn resolve_immutable(&self) -> Option<Duration> {
		match self.bool("immutable") {
			Some(true) => Some(Duration::zero()),
			Some(false) => None,
			None => self.duration("immutable"),
		}
	}

	fn resolve_sealing(&self) -> Result<WindowSealing, String> {
		let sealing = WindowSealing {
			lateness: self.duration("lateness"),
			immutable: self.resolve_immutable(),
		};
		if let (Some(lateness), Some(immutable)) = (sealing.lateness, sealing.immutable)
			&& immutable >= lateness
		{
			return Err(format!("immutable {immutable} must be strictly less than lateness {lateness}"));
		}
		Ok(sealing)
	}
}

#[cfg(test)]
mod tests {
	use super::{super::testutil::config, WindowSealing};
	use crate::value::{Value, duration::Duration};

	fn secs(n: i64) -> Duration {
		Duration::from_seconds(n).unwrap()
	}

	#[test]
	fn declared_immutable_below_lateness_is_kept_verbatim() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("immutable", Value::Duration(secs(15)))]);
		assert_eq!(
			cfg.sealing(),
			Some(WindowSealing {
				lateness: Some(secs(20)),
				immutable: Some(secs(15)),
			})
		);
	}

	#[test]
	fn lateness_without_immutable_resolves_with_no_immutable() {
		// Substituting the lateness would arm the sealing slots on a window that never asked to seal.
		let cfg = config(vec![("lateness", Value::Duration(secs(20)))]);
		assert_eq!(
			cfg.sealing(),
			Some(WindowSealing {
				lateness: Some(secs(20)),
				immutable: None,
			})
		);
	}

	#[test]
	fn declared_immutable_equal_to_lateness_is_rejected() {
		// The bound is strict; an immutable equal to the lateness would never seal before the window closes.
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("immutable", Value::Duration(secs(20)))]);
		assert_eq!(cfg.sealing(), None);
	}

	#[test]
	fn declared_immutable_above_lateness_is_rejected() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("immutable", Value::Duration(secs(30)))]);
		assert_eq!(cfg.sealing(), None);
	}

	#[test]
	fn immutable_without_lateness_is_accepted() {
		// The ordering bound needs both knobs, so an immutable alone must reach the operator untouched.
		let cfg = config(vec![("immutable", Value::Duration(secs(15)))]);
		assert_eq!(
			cfg.sealing(),
			Some(WindowSealing {
				lateness: None,
				immutable: Some(secs(15)),
			})
		);
	}

	#[test]
	fn neither_knob_declared_leaves_both_absent() {
		// An undeclared knob must stay absent, never resolve to zero, which is itself a legal declared value.
		let cfg = config(vec![("duration", Value::Duration(secs(60)))]);
		assert_eq!(
			cfg.sealing(),
			Some(WindowSealing {
				lateness: None,
				immutable: None,
			})
		);
	}

	#[test]
	fn validated_returns_both_when_declared() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("immutable", Value::Duration(secs(15)))]);
		assert_eq!(
			cfg.validated_sealing(),
			WindowSealing {
				lateness: Some(secs(20)),
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn validated_returns_no_immutable_when_only_the_lateness_is_declared() {
		// A window under the immutable floor declares a lateness alone and must still resolve.
		let cfg = config(vec![("lateness", Value::Duration(secs(20)))]);
		assert_eq!(
			cfg.validated_sealing(),
			WindowSealing {
				lateness: Some(secs(20)),
				immutable: None,
			}
		);
	}

	#[test]
	#[should_panic(expected = "must be strictly less than lateness")]
	fn validated_names_the_ordering_violation() {
		let cfg =
			config(vec![("lateness", Value::Duration(secs(20))), ("immutable", Value::Duration(secs(20)))]);
		cfg.validated_sealing();
	}

	#[test]
	fn validated_accepts_an_immutable_without_a_lateness() {
		// Validation covers the ordering rule only; neither knob is required, so this must not panic.
		let cfg = config(vec![("immutable", Value::Duration(secs(15)))]);
		assert_eq!(
			cfg.validated_sealing(),
			WindowSealing {
				lateness: None,
				immutable: Some(secs(15)),
			}
		);
	}

	#[test]
	fn validated_accepts_a_window_that_declares_no_knob_at_all() {
		let cfg = config(vec![("duration", Value::Duration(secs(60)))]);
		assert_eq!(
			cfg.validated_sealing(),
			WindowSealing {
				lateness: None,
				immutable: None,
			}
		);
	}

	#[test]
	fn boolean_true_resolves_to_a_zero_duration() {
		let cfg = config(vec![("lateness", Value::Duration(secs(20))), ("immutable", Value::Boolean(true))]);
		assert_eq!(
			cfg.sealing(),
			Some(WindowSealing {
				lateness: Some(secs(20)),
				immutable: Some(Duration::zero()),
			})
		);
	}

	#[test]
	fn boolean_false_resolves_as_if_absent() {
		let cfg = config(vec![("lateness", Value::Duration(secs(20))), ("immutable", Value::Boolean(false))]);
		assert_eq!(
			cfg.sealing(),
			Some(WindowSealing {
				lateness: Some(secs(20)),
				immutable: None,
			})
		);
	}
}

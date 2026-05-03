// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Filter for matching entities by namespace and name.
///
/// Supports patterns like:
/// - `"namespace.table"` - exact match
/// - `"namespace.*"` - all entities in namespace
/// - `"*.table"` - entity name in any namespace
/// - `"*"` - match all (wildcard)
#[derive(Debug, Clone)]
pub struct InterceptFilter {
	pub namespace: Option<String>,

	pub name: Option<String>,
}

impl InterceptFilter {
	pub fn all() -> Self {
		Self {
			namespace: None,
			name: None,
		}
	}

	pub fn exact(namespace: impl Into<String>, name: impl Into<String>) -> Self {
		Self {
			namespace: Some(namespace.into()),
			name: Some(name.into()),
		}
	}

	pub fn namespace(namespace: impl Into<String>) -> Self {
		Self {
			namespace: Some(namespace.into()),
			name: None,
		}
	}

	pub fn name(name: impl Into<String>) -> Self {
		Self {
			namespace: None,
			name: Some(name.into()),
		}
	}

	pub fn parse(spec: &str) -> Self {
		let spec = spec.trim();

		if spec == "*" {
			return Self::all();
		}

		if let Some((ns, name)) = spec.split_once('.') {
			let namespace = if ns == "*" {
				None
			} else {
				Some(ns.to_string())
			};
			let name = if name == "*" {
				None
			} else {
				Some(name.to_string())
			};
			Self {
				namespace,
				name,
			}
		} else {
			Self {
				namespace: Some(spec.to_string()),
				name: None,
			}
		}
	}

	pub fn matches(&self, namespace: &str, name: &str) -> bool {
		let ns_matches = self.namespace.as_ref().is_none_or(|ns| ns == namespace);
		let name_matches = self.name.as_ref().is_none_or(|n| n == name);
		ns_matches && name_matches
	}

	pub fn is_all(&self) -> bool {
		self.namespace.is_none() && self.name.is_none()
	}
}

#[cfg(test)]
pub mod tests {
	use super::*;

	#[test]
	fn test_parse_exact() {
		let filter = InterceptFilter::parse("myns.users");
		assert_eq!(filter.namespace, Some("myns".to_string()));
		assert_eq!(filter.name, Some("users".to_string()));
		assert!(filter.matches("myns", "users"));
		assert!(!filter.matches("myns", "orders"));
		assert!(!filter.matches("other", "users"));
	}

	#[test]
	fn test_parse_namespace_wildcard() {
		let filter = InterceptFilter::parse("myns.*");
		assert_eq!(filter.namespace, Some("myns".to_string()));
		assert_eq!(filter.name, None);
		assert!(filter.matches("myns", "users"));
		assert!(filter.matches("myns", "orders"));
		assert!(!filter.matches("other", "users"));
	}

	#[test]
	fn test_parse_name_wildcard() {
		let filter = InterceptFilter::parse("*.users");
		assert_eq!(filter.namespace, None);
		assert_eq!(filter.name, Some("users".to_string()));
		assert!(filter.matches("myns", "users"));
		assert!(filter.matches("other", "users"));
		assert!(!filter.matches("myns", "orders"));
	}

	#[test]
	fn test_parse_all() {
		let filter = InterceptFilter::parse("*");
		assert!(filter.is_all());
		assert!(filter.matches("any", "thing"));
	}

	#[test]
	fn test_all() {
		let filter = InterceptFilter::all();
		assert!(filter.is_all());
		assert!(filter.matches("any", "thing"));
	}
}

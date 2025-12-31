// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Filter for matching entities by namespace and name.
///
/// Supports patterns like:
/// - `"namespace.table"` - exact match
/// - `"namespace.*"` - all entities in namespace
/// - `"*.table"` - entity name in any namespace
/// - `"*"` - match all (wildcard)
#[derive(Debug, Clone)]
pub struct InterceptFilter {
	/// Namespace to match. None means match any namespace.
	pub namespace: Option<String>,
	/// Entity name to match. None means match any entity.
	pub name: Option<String>,
}

impl InterceptFilter {
	/// Create a filter that matches all entities.
	pub fn all() -> Self {
		Self {
			namespace: None,
			name: None,
		}
	}

	/// Create a filter for a specific namespace and name.
	pub fn exact(namespace: impl Into<String>, name: impl Into<String>) -> Self {
		Self {
			namespace: Some(namespace.into()),
			name: Some(name.into()),
		}
	}

	/// Create a filter for all entities in a namespace.
	pub fn namespace(namespace: impl Into<String>) -> Self {
		Self {
			namespace: Some(namespace.into()),
			name: None,
		}
	}

	/// Create a filter for an entity name in any namespace.
	pub fn name(name: impl Into<String>) -> Self {
		Self {
			namespace: None,
			name: Some(name.into()),
		}
	}

	/// Parse a filter specification string.
	///
	/// Formats:
	/// - `"namespace.name"` - exact match
	/// - `"namespace.*"` - all in namespace
	/// - `"*.name"` - name in any namespace
	/// - `"*"` - match all
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
			// No dot - treat as namespace only
			Self {
				namespace: Some(spec.to_string()),
				name: None,
			}
		}
	}

	/// Check if the filter matches the given namespace and name.
	pub fn matches(&self, namespace: &str, name: &str) -> bool {
		let ns_matches = self.namespace.as_ref().map_or(true, |ns| ns == namespace);
		let name_matches = self.name.as_ref().map_or(true, |n| n == name);
		ns_matches && name_matches
	}

	/// Check if this filter matches all entities (no restrictions).
	pub fn is_all(&self) -> bool {
		self.namespace.is_none() && self.name.is_none()
	}
}

#[cfg(test)]
mod tests {
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

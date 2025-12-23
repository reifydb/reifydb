/// Metrics for tracking FlowTransaction operations
#[derive(Debug, Clone, Default)]
pub struct FlowTransactionMetrics {
	pub reads: usize,
	pub writes: usize,
	pub removes: usize,
	pub state_operations: usize,
}

impl FlowTransactionMetrics {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn increment_reads(&mut self) {
		self.reads += 1;
	}

	pub fn increment_writes(&mut self) {
		self.writes += 1;
	}

	pub fn increment_removes(&mut self) {
		self.removes += 1;
	}

	pub fn increment_state_operations(&mut self) {
		self.state_operations += 1;
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn test_new_metrics() {
		let metrics = FlowTransactionMetrics::new();
		assert_eq!(metrics.reads, 0);
		assert_eq!(metrics.writes, 0);
		assert_eq!(metrics.removes, 0);
		assert_eq!(metrics.state_operations, 0);
	}

	#[tokio::test]
	async fn test_default_metrics() {
		let metrics = FlowTransactionMetrics::default();
		assert_eq!(metrics.reads, 0);
		assert_eq!(metrics.writes, 0);
		assert_eq!(metrics.removes, 0);
		assert_eq!(metrics.state_operations, 0);
	}

	#[tokio::test]
	async fn test_increment_reads() {
		let mut metrics = FlowTransactionMetrics::new();
		assert_eq!(metrics.reads, 0);

		metrics.increment_reads();
		assert_eq!(metrics.reads, 1);

		metrics.increment_reads();
		assert_eq!(metrics.reads, 2);

		// Other metrics should not change
		assert_eq!(metrics.writes, 0);
		assert_eq!(metrics.removes, 0);
		assert_eq!(metrics.state_operations, 0);
	}

	#[tokio::test]
	async fn test_increment_writes() {
		let mut metrics = FlowTransactionMetrics::new();
		assert_eq!(metrics.writes, 0);

		metrics.increment_writes();
		assert_eq!(metrics.writes, 1);

		metrics.increment_writes();
		assert_eq!(metrics.writes, 2);

		// Other metrics should not change
		assert_eq!(metrics.reads, 0);
		assert_eq!(metrics.removes, 0);
		assert_eq!(metrics.state_operations, 0);
	}

	#[tokio::test]
	async fn test_increment_removes() {
		let mut metrics = FlowTransactionMetrics::new();
		assert_eq!(metrics.removes, 0);

		metrics.increment_removes();
		assert_eq!(metrics.removes, 1);

		metrics.increment_removes();
		assert_eq!(metrics.removes, 2);

		// Other metrics should not change
		assert_eq!(metrics.reads, 0);
		assert_eq!(metrics.writes, 0);
		assert_eq!(metrics.state_operations, 0);
	}

	#[tokio::test]
	async fn test_increment_state_operations() {
		let mut metrics = FlowTransactionMetrics::new();
		assert_eq!(metrics.state_operations, 0);

		metrics.increment_state_operations();
		assert_eq!(metrics.state_operations, 1);

		metrics.increment_state_operations();
		assert_eq!(metrics.state_operations, 2);

		// Other metrics should not change
		assert_eq!(metrics.reads, 0);
		assert_eq!(metrics.writes, 0);
		assert_eq!(metrics.removes, 0);
	}

	#[tokio::test]
	async fn test_mixed_metrics() {
		let mut metrics = FlowTransactionMetrics::new();

		metrics.increment_reads();
		metrics.increment_reads();
		metrics.increment_writes();
		metrics.increment_removes();
		metrics.increment_state_operations();
		metrics.increment_state_operations();
		metrics.increment_state_operations();

		assert_eq!(metrics.reads, 2);
		assert_eq!(metrics.writes, 1);
		assert_eq!(metrics.removes, 1);
		assert_eq!(metrics.state_operations, 3);
	}

	#[tokio::test]
	async fn test_metrics_clone() {
		let mut original = FlowTransactionMetrics::new();
		original.increment_reads();
		original.increment_writes();

		let cloned = original.clone();
		assert_eq!(cloned.reads, 1);
		assert_eq!(cloned.writes, 1);
		assert_eq!(cloned.removes, 0);
		assert_eq!(cloned.state_operations, 0);
	}
}

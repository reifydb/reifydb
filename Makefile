.PHONY: all
all: check test build push

.PHONY: check
check:
	@if ! git diff-index --quiet HEAD --; then \
		echo "Error: You have uncommitted changes. Please commit or stash them before pushing."; \
		exit 1; \
	fi

.PHONY: test
test:
	cargo nextest run -p smoke --all-targets
	cargo nextest run --all-targets --no-fail-fast --final-status-level all

.PHONY: test
build:
	cargo build --release

.PHONY: coverage
coverage:
	cargo tarpaulin -o html --all --output-dir target/coverage

.PHONY: push
push: check
	git push


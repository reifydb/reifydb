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
	cargo test --lib --bins --tests

.PHONY: test
build:
	cargo build --release

.PHONY: push
push: check
	git push


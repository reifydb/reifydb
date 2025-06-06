.PHONY: all
all: check clean test build push

.PHONY: check
check:
	@if ! git diff-index --quiet HEAD --; then \
		echo "Error: You have uncommitted changes. Please commit or stash them before pushing."; \
		exit 1; \
	fi

.PHONY: clean
clean:
	@for pkg in $$(cargo metadata --format-version 1 --no-deps | jq -r '.packages[].name' | grep '^reifydb_'); do \
		cargo clean -p $$pkg; \
	done

.PHONY: test
test:
	cargo nextest run --all-targets --no-fail-fast --status-level fail --final-status-level fail

.PHONY: test
build:
	cargo build --release

.PHONY: coverage
coverage:
	cargo tarpaulin -o html --all --output-dir target/coverage

.PHONY: push
push: check
	git push


# Path to the test suites directory
TESTSUITES_DIR := ../testsuite

# List of test suites
TEST_SUITES := \
	compatibility \
	diagnostic \
	functional \
	regression \
	smoke

# Default target: run all test suites
.PHONY: testsuite
testsuite: $(TEST_SUITES)

# Rule to run cargo nextest for each suite
$(TEST_SUITES):
	@echo "🔍 Running $@ tests in $(TESTSUITES_DIR)/$@ ..."
	@cd $(TESTSUITES_DIR)/$@ && cargo nextest run --no-fail-fast

# Individual targets
.PHONY: compatibility diagnostic functional regression smoke
.PHONY: all
all: check clean test testsuite build push

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
test: testlocal testsuite

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
TEST_SUITE_DIR := ../testsuite

# List of test suites
TEST_SUITES := \
	compatibility \
	diagnostic \
	functional \
	regression \
	smoke

.PHONY: testlocal
testlocal:
	cargo nextest run --all-targets --no-fail-fast --status-level fail --final-status-level fail


.PHONY: testsuite
testsuite: $(TEST_SUITES)

$(TEST_SUITES):
	@if [ -d "$(TEST_SUITE_DIR)/$@" ]; then \
		echo "🔍 Running $@ tests in $(TEST_SUITE_DIR)/$@ ..."; \
		cd $(TEST_SUITE_DIR)/$@ && cargo nextest run --no-fail-fast; \
	else \
		echo "⚠️ Skipping $@ – directory $(TEST_SUITE_DIR)/$@ not found"; \
	fi

# Individual targets
.PHONY: compatibility diagnostic functional regression smoke
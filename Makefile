
# Path to the test suites directory
TEST_SUITE_DIR := ../testsuite
TEST_CLIENT_DIR := ./pkg/client

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
	@for pkg in $$(cargo metadata --format-version 1 --no-deps | jq -r '.packages[].name' | grep '^reifydb-'); do \
		cargo clean -p $$pkg; \
	done

.PHONY: test
test: testlocal testsuite

.PHONY: test-full
test-full: testlocal testsuite testclient

.PHONY: build
build:
	cargo build --release

.PHONY: build-testcontainer
build-testcontainer:
	docker build . -f bin/testcontainer/Dockerfile -t reifydb/testcontainer

.PHONY: coverage
coverage:
	cargo tarpaulin -o html --all --output-dir target/coverage

.PHONY: push
push: check
	git push


# List of test suites
TEST_SUITES := \
	smoke \
	compatibility \
	diagnostic \
	functional \
	stress

.PHONY: testlocal
testlocal:
	cargo nextest run --all-targets --no-fail-fast --status-level fail --final-status-level fail


.PHONY: testsuite $(TEST_SUITES)
testsuite:
	$(MAKE) -j$(shell nproc) $(TEST_SUITES)

$(TEST_SUITES):
	@if [ -d "$(TEST_SUITE_DIR)/$@" ]; then \
		echo "🔍 Running $@ tests in $(TEST_SUITE_DIR)/$@ ..."; \
		cd $(TEST_SUITE_DIR)/$@ && cargo nextest run --no-fail-fast; \
	else \
		echo "⚠️ Skipping $@ – directory $(TEST_SUITE_DIR)/$@ not found"; \
	fi


TEST_CLIENTS := \
	typescript
	
.PHONY: testclient $(TEST_CLIENTS)
testclient:
	$(MAKE) build-testcontainer
	$(MAKE) -j$(shell nproc) $(TEST_CLIENTS)

$(TEST_CLIENTS):
	@if [ -d "$(TEST_CLIENT_DIR)/$@" ]; then \
		echo "🔍 Running $@ tests in $(TEST_CLIENT_DIR)/$@ ..."; \
		cd $(TEST_CLIENT_DIR)/$@ && $(MAKE) test; \
	else \
		echo "⚠️ Skipping $@ – directory $(TEST_CLIENT_DIR)/$@ not found"; \
	fi
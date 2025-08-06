# Makefile for ReifyDB Clients
.PHONY: help install build test test-unit test-integration test-coverage
.PHONY: install build test test-unit test-integration test-coverage

# Default target
.DEFAULT_GOAL := help

TEST_DIRS := client
PNPM := pnpm

# Colors for output
CYAN := \033[36m
GREEN := \033[32m
YELLOW := \033[33m
RED := \033[31m
RESET := \033[0m

help: ## Show this help message
	@echo "$(CYAN)ReifyDB Multi-Client Makefile$(RESET)"
	@echo ""
	@echo "$(GREEN)Available targets:$(RESET)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(CYAN)%-20s$(RESET) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Available client directories:$(RESET)"
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ]; then \
			echo "  $(GREEN)✓$(RESET) $$dir"; \
		else \
			echo "  $(RED)✗$(RESET) $$dir (not found)"; \
		fi; \
	done

# Helper function to check if directory exists and has package.json
define check_dir_and_package
	@if [ ! -d "$(1)" ]; then \
		echo "$(RED)Error: $(1) directory not found$(RESET)"; \
		exit 1; \
	fi
	@if [ ! -f "$(1)/package.json" ]; then \
		echo "$(RED)Error: $(1)/package.json not found$(RESET)"; \
		exit 1; \
	fi
endef

# Single directory targets (default to first directory in list)
CLIENT_DIR := $(word 1, $(TEST_DIRS))

install:
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ] && [ -f "$$dir/package.json" ]; then \
			echo "$(YELLOW)Installing dependencies in $$dir...$(RESET)"; \
			cd $$dir && $(PNPM) install && cd ..; \
		else \
			echo "$(RED)Skipping $$dir (not found or no package.json)$(RESET)"; \
		fi; \
	done

build:
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ] && [ -f "$$dir/package.json" ]; then \
			echo "$(YELLOW)Building project in $$dir...$(RESET)"; \
			cd $$dir && $(PNPM) build && cd ..; \
		else \
			echo "$(RED)Skipping $$dir (not found or no package.json)$(RESET)"; \
		fi; \
	done

test:
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ] && [ -f "$$dir/package.json" ]; then \
			echo "$(YELLOW)Running all tests in $$dir...$(RESET)"; \
			cd $$dir && $(PNPM) test && cd ..; \
		else \
			echo "$(RED)Skipping $$dir (not found or no package.json)$(RESET)"; \
		fi; \
	done

test-unit:
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ] && [ -f "$$dir/package.json" ]; then \
			echo "$(YELLOW)Running unit tests in $$dir...$(RESET)"; \
			cd $$dir && $(PNPM) test:unit && cd ..; \
		else \
			echo "$(RED)Skipping $$dir (not found or no package.json)$(RESET)"; \
		fi; \
	done

test-integration:
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ] && [ -f "$$dir/package.json" ]; then \
			echo "$(YELLOW)Running integration tests in $$dir...$(RESET)"; \
			cd $$dir && $(PNPM) test:integration && cd ..; \
		else \
			echo "$(RED)Skipping $$dir (not found or no package.json)$(RESET)"; \
		fi; \
	done

test-coverage:
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ] && [ -f "$$dir/package.json" ]; then \
			echo "$(YELLOW)Running tests with coverage in $$dir...$(RESET)"; \
			cd $$dir && $(PNPM) test:coverage && cd ..; \
		else \
			echo "$(RED)Skipping $$dir (not found or no package.json)$(RESET)"; \
		fi; \
	done

# Specific directory targets - dynamically generated
define DIRECTORY_TEMPLATE
install-$(1): ## Install dependencies for $(1)
	$(call check_dir_and_package,$(1))
	@echo "$(YELLOW)Installing dependencies in $(1)...$(RESET)"
	cd $(1) && $(PNPM) install

build-$(1): ## Build $(1)
	$(call check_dir_and_package,$(1))
	@echo "$(YELLOW)Building project in $(1)...$(RESET)"
	cd $(1) && $(PNPM) build

test-$(1): ## Run all tests for $(1)
	$(call check_dir_and_package,$(1))
	@echo "$(YELLOW)Running all tests in $(1)...$(RESET)"
	cd $(1) && $(PNPM) test

test-unit-$(1): ## Run unit tests for $(1)
	$(call check_dir_and_package,$(1))
	@echo "$(YELLOW)Running unit tests in $(1)...$(RESET)"
	cd $(1) && $(PNPM) test:unit

test-integration-$(1): ## Run integration tests for $(1)
	$(call check_dir_and_package,$(1))
	@echo "$(YELLOW)Running integration tests in $(1)...$(RESET)"
	cd $(1) && $(PNPM) test:integration

test-coverage-$(1): ## Run tests with coverage for $(1)
	$(call check_dir_and_package,$(1))
	@echo "$(YELLOW)Running tests with coverage in $(1)...$(RESET)"
	cd $(1) && $(PNPM) test:coverage
endef

# Generate targets for each test directory
$(foreach dir,$(TEST_DIRS),$(eval $(call DIRECTORY_TEMPLATE,$(dir))))

# Utility targets
list-dirs: ## List all configured test directories
	@echo "$(CYAN)Configured client directories:$(RESET)"
	@for dir in $(TEST_DIRS); do \
		if [ -d "$$dir" ]; then \
			if [ -f "$$dir/package.json" ]; then \
				name=$$(grep '"name"' "$$dir/package.json" | cut -d'"' -f4 2>/dev/null || echo "unknown"); \
				echo "  $(GREEN)✓$(RESET) $$dir ($$name)"; \
			else \
				echo "  $(YELLOW)?$(RESET) $$dir (no package.json)"; \
			fi; \
		else \
			echo "  $(RED)✗$(RESET) $$dir (not found)"; \
		fi; \
	done

check: ## Check status of all test directories
	@echo "$(CYAN)Client Directory Status:$(RESET)"
	@for dir in $(TEST_DIRS); do \
		echo -n "  $$dir: "; \
		if [ -d "$$dir" ]; then \
			if [ -f "$$dir/package.json" ]; then \
				if [ -d "$$dir/node_modules" ]; then \
					echo "$(GREEN)Ready$(RESET)"; \
				else \
					echo "$(YELLOW)Needs install$(RESET)"; \
				fi; \
			else \
				echo "$(YELLOW)No package.json$(RESET)"; \
			fi; \
		else \
			echo "$(RED)Not found$(RESET)"; \
		fi; \
	done

# Quick setup for all directories
setup: install build ## Install and build all client directories
	@echo "$(GREEN)All client directories setup completed$(RESET)"
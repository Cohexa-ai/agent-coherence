# Requires: pip install -e ".[langgraph,benchmark]"

.PHONY: benchmark benchmark-check cost-benchmark cost-benchmark-check tla-check help

benchmark:  ## Run all three LangGraph benchmarks and write latest.json
	python tools/run_benchmarks.py

benchmark-check:  ## Check latest.json drift against expected.json (skips benchmark run)
	python tools/benchmark_drift_check.py

cost-benchmark:  ## Run the change-rate × answer-sensitivity cost sweep and write cost_sweep.json
	python tools/run_cost_sweep.py

cost-benchmark-check:  ## Check cost_sweep.json drift against expected_cost.json (skips sweep run)
	python tools/cost_drift_check.py

TLA2TOOLS := formal/tla/lib/tla2tools.jar
TLC := java -XX:+UseParallelGC -cp $(TLA2TOOLS) tlc2.TLC -workers auto

tla-check:  ## Run TLC model checker on MESI and CrashRecovery specs
	@java -version 2>&1 | head -1 | grep -qE '"(1[7-9]|[2-9][0-9])\.' || { echo "ERROR: Java 17+ required for TLC model checker"; exit 1; }
	$(TLC) -config formal/tla/MESI_Standalone.cfg formal/tla/MESI_Standalone.tla
	$(TLC) -config formal/tla/CrashRecovery_CI.cfg formal/tla/CrashRecovery.tla

help:  ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

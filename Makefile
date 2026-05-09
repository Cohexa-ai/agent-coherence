# Requires: pip install -e ".[langgraph,benchmark]"

.PHONY: benchmark benchmark-check tla-check help

benchmark:  ## Run all three LangGraph benchmarks and write latest.json
	python tools/run_benchmarks.py

benchmark-check:  ## Check latest.json drift against expected.json (skips benchmark run)
	python tools/benchmark_drift_check.py

TLA2TOOLS := formal/tla/lib/tla2tools.jar
TLC := java -XX:+UseParallelGC -cp $(TLA2TOOLS) tlc2.TLC -workers auto

tla-check:  ## Run TLC model checker on MESI and CrashRecovery specs
	$(TLC) -config formal/tla/MESI_Standalone.cfg formal/tla/MESI_Standalone.tla
	$(TLC) -config formal/tla/CrashRecovery_CI.cfg formal/tla/CrashRecovery.tla

help:  ## List available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

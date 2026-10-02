.PHONY: test perf

# The performance of the working tree against the revision PERF_BASE
# (see test/performance/elock_perf_compare.escript):
#   make perf
#   make perf PERF_SCENARIOS="unique_terms hot_txn" PERF_CLIENTS="16 1024"
#
# PERF_SCENARIOS - the scenarios of test/performance/elock_perf.erl to
#                  run, all of them if empty
# PERF_CLIENTS   - the numbers of the clients to run every scenario at,
#                  one per scheduler and 64 per scheduler if empty
PERF_BASE ?= lazy_deadlock
PERF_SCENARIOS ?=
PERF_CLIENTS ?=

test:
	./rebar3 ct --spec test/functional/test.spec

perf:
	./rebar3 compile
	PERF_SCENARIOS="$(PERF_SCENARIOS)" PERF_CLIENTS="$(PERF_CLIENTS)" \
		escript test/performance/elock_perf_compare.escript $(PERF_BASE)

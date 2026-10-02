.PHONY: test perf

# The revision the working tree is compared to (see
# test/performance/elock_perf_compare.escript)
PERF_BASE ?= lazy_deadlock

test:
	./rebar3 ct --spec test/functional/test.spec

perf:
	./rebar3 compile
	escript test/performance/elock_perf_compare.escript $(PERF_BASE)

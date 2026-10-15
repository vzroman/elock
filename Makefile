.PHONY: compile xref dialyzer test performance_tests performance_report shell clean_logs clean_tests clean_build clean_all

compile:
	./rebar3 compile

xref:
	./rebar3 xref

dialyzer:
	./rebar3 dialyzer

test:
	./rebar3 ct --cover --spec test/functional/test.spec

performance_tests:
	./rebar3 ct --spec test/performance/test.spec

performance_report:
	cd performance_report && npm install
	cd performance_report && npm run build
	cd performance_report && sh -c '(sleep 1; xdg-open http://localhost:3000) & exec npm start'

shell:
	ERL_FLAGS="-args_file config/vm.args -config config/sys.config" ./rebar3 shell

clean_logs:
	rm -rf logs

clean_tests:
	rm -rf _build/test

clean_build:
	rm -rf _build
	rm -f rebar.lock

clean_all: clean_logs clean_build

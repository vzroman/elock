.PHONY: test performance_tests performance_report

test:
	./rebar3 ct --spec test/functional/test.spec

performance_tests:
	./rebar3 ct --spec test/performance/test.spec

performance_report:
	cd performance_report && npm install
	cd performance_report && npm run build
	cd performance_report && sh -c '(sleep 1; xdg-open http://localhost:3000) & exec npm start'

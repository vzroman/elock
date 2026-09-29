.PHONY: test

test:
	./rebar3 ct --spec test/functional/test.spec

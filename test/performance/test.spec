%% The performance tests of elock against mnesia and global, on
%% nodes in docker containers (see util/performance_nodes.erl):
%%
%%   make performance_tests
%%
%% The sizes and the nodes are in performance.config. Every point
%% writes a JSON file to performance_data in the priv_dir of the
%% suite (under _build/test/logs)

{define, 'PERFORMANCE_TEST', "./."}.

{config, "performance.config"}.

{suites, 'PERFORMANCE_TEST', [
  performance_transaction_SUITE
]}.

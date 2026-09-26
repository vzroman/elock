%% The functional tests of elock. Run everything:
%%
%%   ./rebar3 ct --spec test/functional/test.spec
%%
%% Run one suite (rebar3 3.20 ignores --suite next to --spec and runs
%% the whole spec, hence the --dir form):
%%
%%   ./rebar3 ct --dir test/functional --suite elock_graph_SUITE
%%
%% Logs: _build/test/logs

{define, 'FUNCTIONAL_TEST', "./."}.

{config, "functional.config"}.

%% Stage 1: the per-module suites. Stage 2: the single node
%% scenarios. Stage 3: the multi node scenarios on peer nodes (see
%% util/distributed_tests_utils.erl)
{suites, 'FUNCTIONAL_TEST', [
  elock_SUITE,
  elock_graph_SUITE,
  elock_manager_SUITE,
  elock_locking_SUITE,
  elock_deadlock_SUITE,
  elock_concurrency_SUITE,
  elock_multi_node_SUITE,
  elock_multi_node_concurrency_SUITE
]}.

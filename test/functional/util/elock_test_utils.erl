%%=================================================================
%%  Shared helpers of the elock functional suites:
%%  scopes, clients, observation of the locks and the managers,
%%  waiting with deadlines, message collectors and what the
%%  compiler says about the receives of a module.
%%
%%  Everything that waits has a deadline and fails naming what it
%%  waited for - a test must never hang.
%%=================================================================
-module(elock_test_utils).

%% Scopes
-export([
  start_scope/1, start_scope/2,
  stop_scope/1,
  finish_scope/1,
  wait_ready/2
]).

%% Clients
-export([
  client/0, client/1,
  call/2, call/3,
  cast/2,
  result/2,
  pending/1,
  stop/1,
  stop_clients/0
]).

%% Locks
-export([
  lock/4, lock/5,
  lock_async/5,
  lock_queued/5, lock_queued/6,
  settled/2,
  quiet/1,
  unlock/2,
  context/1
]).

%% Observation
-export([
  locks/1, locks/2,
  manager/2, manager/3,
  wait_manager/2, wait_manager/3,
  managers/0, managers/1,
  kill_managers/0, kill_managers/1
]).

%% Waiting
-export([
  wait_until/2,
  wait_idle/1, wait_idle/2,
  wait_dead/1,
  flush/0,
  no_message/1
]).

%% Collectors
-export([
  collector/0,
  collected/2,
  stop_collectors/0
]).

%% Compiler
-export([
  recv_opt_info/1
]).

%% Internal: the entry points of the processes spawned on other nodes
%% and the calls made there
-export([
  scope_holder/2,
  client_init/1,
  collector_init/1,
  managers_work/0
]).

-define(POLL, 10).
-define(DEADLINE, 5000).
-define(CONTEXT, '$elock_context$').
-define(HOLDER_SCOPE, '$elock_test_scope$').
-define(CALL, '$elock_test_call$').
-define(CLIENTS, '$elock_test_clients$').
-define(COLLECTORS, '$elock_test_collectors$').

%%=================================================================
%%  Scopes
%%=================================================================
%%-----------------------------------------------------------------
%%  A holder process calls elock:start_link(Scope) and sleeps. It
%%  returns once the table exists and the node is among the ready
%%  nodes of the scope
%%-----------------------------------------------------------------
start_scope(Scope)->
  start_scope(node(), Scope).

start_scope(Node, Scope)->
  Holder = spawn(Node, ?MODULE, scope_holder, [Scope, self()]),
  MonRef = erlang:monitor(process, Holder),
  receive
    {scope_started, Holder}->
      erlang:demonitor(MonRef, [flush]);
    {'DOWN', MonRef, process, Holder, Reason}->
      erlang:error({scope_start_failed, Node, Scope, Reason})
  after ?DEADLINE->
    erlang:error({scope_start_timeout, Node, Scope})
  end,
  wait_until(
    fun()->
      is_reference(rpc(Node, ets, whereis, [Scope]))
        andalso is_ready(Node, Scope)
    end,
    ?DEADLINE
  ),
  Holder.

scope_holder(Scope, Parent)->
  put(?HOLDER_SCOPE, Scope),
  {ok, _} = elock:start_link(Scope),
  Parent ! {scope_started, self()},
  timer:sleep(infinity).

%%-----------------------------------------------------------------
%%  Kill the holder: the scope process is linked to it and goes
%%  with it, the table and the pg scope go with the scope process
%%-----------------------------------------------------------------
stop_scope(Holder)->
  Node = node(Holder),
  Scope = holder_scope(Holder),
  exit(Holder, kill),
  wait_dead(Holder),
  case Scope of
    undefined->
      ok;
    _->
      PgScope = list_to_atom(atom_to_list(Scope) ++ "_$pg$"),
      wait_until(
        fun()->
          rpc(Node, ets, whereis, [Scope]) =:= undefined
            andalso rpc(Node, erlang, whereis, [PgScope]) =:= undefined
        end,
        ?DEADLINE
      )
  end,
  ok.

%%-----------------------------------------------------------------
%%  end_per_testcase helper. The scope of the holder must become
%%  idle (no lock entry, no manager on its node) within the
%%  deadline; then it is stopped and whatever manager is left is
%%  killed. The result is ok or {fail, Reason} to be returned by
%%  end_per_testcase - a crash of end_per_testcase does not fail
%%  the test case, only {fail, _} does
%%-----------------------------------------------------------------
finish_scope(Holder)->
  Node = node(Holder),
  Idle =
    case holder_scope(Holder) of
      undefined->
        % the scope is stopped already
        catch wait_until(fun()-> managers(Node) =:= [] end, ?DEADLINE);
      Scope->
        catch wait_idle(Node, Scope)
    end,
  stop_scope(Holder),
  kill_managers(Node),
  case Idle of
    ok->
      ok;
    Error->
      {fail, {scope_not_idle, Error}}
  end.

holder_scope(Holder)->
  case rpc(node(Holder), erlang, process_info, [Holder, dictionary]) of
    {dictionary, Dictionary}->
      proplists:get_value(?HOLDER_SCOPE, Dictionary);
    _->
      undefined
  end.

%%-----------------------------------------------------------------
%%  Every node of Nodes sees exactly Nodes as the ready nodes
%%-----------------------------------------------------------------
wait_ready(Scope, Nodes)->
  Expected = lists:sort(Nodes),
  wait_until(
    fun()->
      lists:all(
        fun(Node)->
          case catch rpc(Node, elock, ready_nodes, [Scope]) of
            Ready when is_list(Ready)->
              lists:sort(Ready) =:= Expected;
            _->
              false
          end
        end,
        Nodes
      )
    end,
    ?DEADLINE
  ).

is_ready(Node, Scope)->
  case catch rpc(Node, elock, ready_nodes, [Scope]) of
    Ready when is_list(Ready)->
      lists:member(Node, Ready);
    _->
      false
  end.

%%=================================================================
%%  Clients
%%
%%  A client is a process that runs closures on request. It is the
%%  lock owner: elock keeps the held locks in the process dictionary
%%  of the process that locks, hence lock and unlock of one logical
%%  client run inside the same client process. The client dies with
%%  the process that spawned it, so a failed test leaves no clients
%%=================================================================
client()->
  client(node()).

client(Node)->
  Client = spawn(Node, ?MODULE, client_init, [self()]),
  register_spawned(?CLIENTS, Client),
  Client.

client_init(Parent)->
  MonRef = erlang:monitor(process, Parent),
  client_loop(Parent, MonRef).

client_loop(Parent, MonRef)->
  receive
    {?CALL, From, Ref, Fun}->
      Result =
        try Fun()
        catch
          Class:Reason:Stack->
            {'EXIT', {Class, Reason, Stack}}
        end,
      From ! {Ref, Result},
      client_loop(Parent, MonRef);
    {'DOWN', MonRef, process, Parent, _Reason}->
      exit(parent_down)
  end.

%%-----------------------------------------------------------------
%%  Fun() runs inside the client. Exceptions come back as
%%  {'EXIT', {Class, Reason, Stack}}
%%-----------------------------------------------------------------
call(Client, Fun)->
  call(Client, Fun, ?DEADLINE).

call(Client, Fun, Timeout)->
  Ref = cast(Client, Fun),
  MonRef = erlang:monitor(process, Client),
  receive
    {Ref, Result}->
      erlang:demonitor(MonRef, [flush]),
      Result;
    {'DOWN', MonRef, process, Client, Reason}->
      erlang:error({client_down, Client, Reason})
  after Timeout->
    erlang:demonitor(MonRef, [flush]),
    erlang:error({call_timeout, Client})
  end.

%%-----------------------------------------------------------------
%%  Asynchronous call. The result comes to the caller as {Ref, Result}
%%  and is collected with result/2
%%-----------------------------------------------------------------
cast(Client, Fun)->
  Ref = make_ref(),
  Client ! {?CALL, self(), Ref, Fun},
  Ref.

result(Ref, Timeout)->
  receive
    {Ref, Result}->
      {ok, Result}
  after Timeout->
    timeout
  end.

%%-----------------------------------------------------------------
%%  No result yet. The mailbox is only looked at, not changed
%%-----------------------------------------------------------------
pending(Ref)->
  {messages, Messages} = process_info(self(), messages),
  not lists:keymember(Ref, 1, Messages).

stop(Client)->
  exit(Client, kill),
  wait_dead(Client).

%%-----------------------------------------------------------------
%%  Stop every client spawned by the calling process (for
%%  end_per_testcase)
%%-----------------------------------------------------------------
stop_clients()->
  stop_spawned(?CLIENTS).

%%=================================================================
%%  Locks
%%=================================================================
lock(Client, Scope, Term, Nodes)->
  call(Client, fun()-> elock:lock(Scope, Term, Nodes) end).

lock(Client, Scope, Term, Nodes, Options)->
  call(Client, fun()-> elock:lock(Scope, Term, Nodes, Options) end).

lock_async(Client, Scope, Term, Nodes, Options)->
  cast(Client, fun()-> elock:lock(Scope, Term, Nodes, Options) end).

%%-----------------------------------------------------------------
%%  An asynchronous lock request that has settled before the helper
%%  returns, so that the next request finds it in place: it stands
%%  in the queue of its manager and, if its client holds locks, it
%%  is in the wait-for graph and its probes have been handled. A
%%  request does not bring the held locks with it: the manager asks
%%  the client for them (#queued{}) and it is the answer of the
%%  client that joins the request to the graph and sends its
%%  probes. Hence three things are awaited, in this order - each
%%  one is what makes the next one conclusive:
%%  * taken - the entry shows the ticket the request took and the
%%    manager monitors the client, which it does once the request
%%    is in its queue (or granted). The manager registers the
%%    client after it has sent #queued{}, if it asks at all, hence
%%    the question is in the mailbox of the client by now
%%  * answered - the client is suspended in its receive again. A
%%    local send puts the message into the mailbox at once and a
%%    process with a message it waits for is not suspended, hence
%%    the client has taken the question and has sent its answer:
%%    the answer is in the mailbox of the manager. A client that is
%%    not asked (it holds nothing and names one node) is suspended
%%    from the start
%%  * quiet - the managers have nothing to do and have done nothing
%%    for a while (see quiet/1), hence the answer is handled, the
%%    request is in the graph, and its probes, the ones passed on
%%    from manager to manager included, and the verdicts they have
%%    brought are handled by every manager they got to
%%  or the verdict is in already, then there is nothing to look at:
%%  a request that closes a deadlock cycle loses at once and a
%%  dequeued request is demonitored.
%%
%%  On one node this is exact: everything is a local send, there is
%%  no moment when a message is in nobody's mailbox. The client
%%  must be new to the manager: one that holds the term already is
%%  monitored since its first request. The verdict is collected
%%  with result/2
%%-----------------------------------------------------------------
lock_queued(Client, Scope, Term, Nodes, Options)->
  lock_queued(node(), Client, Scope, Term, Nodes, Options).

%%-----------------------------------------------------------------
%%  The same for a request of a remote scope: Watch is the node, or
%%  the nodes, whose managers must have taken the request (the
%%  tables and the monitors are read there through rpc) - for a
%%  multi node request every node of it. The client and the managers
%%  of every node are looked at through rpc the same way, but
%%  across nodes this is not a guarantee: #queued{}, the answer and
%%  the probes travel through ecall, and a message on its way
%%  between two nodes is in nobody's mailbox. The checks only
%%  narrow the window there
%%-----------------------------------------------------------------
lock_queued(Watch, Client, Scope, Term, Nodes, Options) when is_atom(Watch)->
  lock_queued([Watch], Client, Scope, Term, Nodes, Options);
lock_queued(Watch, Client, Scope, Term, Nodes, Options)->
  Tickets = [ {Node, next_ticket(Node, Scope, Term)} || Node <- Watch ],
  Ref = lock_async(Client, Scope, Term, Nodes, Options),
  wait_until(
    fun()->
      case pending(Ref) of
        false->
          true;
        true->
          Taken =
            lists:foldl(
              fun
                ({Node, Ticket}, true)-> taken(Node, Client, Scope, Term, Ticket);
                (_, Problem)-> Problem
              end,
              true,
              Tickets
            ),
          case Taken of
            true-> settled(Client, Watch);
            Problem-> Problem
          end
      end
    end,
    ?DEADLINE
  ),
  Ref.

next_ticket(Node, Scope, Term)->
  case rpc(Node, ets, lookup, [Scope, Term]) of
    [{Term, _Manager, Last}]-> Last + 1;
    []-> 1
  end.

% The manager of the term on the node has taken the ticket and
% monitors the client, or what is in the way
taken(Node, Client, Scope, Term, Ticket)->
  case rpc(Node, ets, lookup, [Scope, Term]) of
    [{Term, Manager, Taken}] when is_pid(Manager), Taken >= Ticket->
      case rpc(Node, erlang, process_info, [Manager, monitors]) of
        {monitors, Monitors}->
          lists:member({process, Client}, Monitors) orelse {not_monitored, Node, Manager, Client};
        _->
          {manager_dead, Node, Manager}
      end;
    Entry->
      {not_taken, Node, Entry}
  end.

%%-----------------------------------------------------------------
%%  The request the client waits with has settled: the client is
%%  suspended in its receive - it has answered whatever it was
%%  asked - and after that the managers are quiet, i.e. they have
%%  handled the answer and what it set off. The order matters: the
%%  answer is in the mailbox of the manager only once the client is
%%  seen suspended. The caller sees to it that the manager has
%%  taken the request (see lock_queued/6). The managers looked at
%%  are those of the node of the client, of Nodes and of every
%%  node connected to this one. The result is true or what is in
%%  the way
%%-----------------------------------------------------------------
settled(Client, Nodes)->
  case rpc(node(Client), erlang, process_info, [Client, status]) of
    {status, waiting}->
      quiet(lists:usort([node(Client) | Nodes] ++ nodes()));
    Status->
      {client_not_suspended, Client, Status}
  end.

%%-----------------------------------------------------------------
%%  No manager of the nodes has anything to do, nor has it done
%%  anything lately: every one of them is suspended with an empty
%%  mailbox in two looks in a row and its reductions have not moved
%%  in between. One look is not enough. The managers are looked at
%%  one after another: a manager seen idle may get a probe passed
%%  on right after by a manager that is seen idle as well when its
%%  turn comes. The second look tells: a manager that has got a
%%  message since the first one has it in its mailbox, runs, or has
%%  run - its reductions have moved. If nothing moved, then between
%%  the two looks no manager ran and no mailbox of theirs had a
%%  message, so there was nobody to send one: the managers are at
%%  rest, all at the same moment. A manager that came or went in
%%  between is a change as well. The result is true or what is in
%%  the way
%%-----------------------------------------------------------------
quiet(Nodes)->
  Look = fun()-> [ {Node, managers_work(Node)} || Node <- Nodes ] end,
  First = Look(),
  case [ Busy || {_Node, Managers} <- First, Busy <- busy(Managers) ] of
    []->
      case Look() of
        First-> true;
        Second-> {managers_not_quiet, First, Second}
      end;
    Busy->
      {managers_busy, Busy}
  end.

% What the managers of the node are doing: [{Manager, Work}] or what
% is wrong with the node
managers_work(Node) when Node =:= node()->
  managers_work();
managers_work(Node)->
  rpc:call(Node, ?MODULE, managers_work, []).

% The managers are the ones the lock entries of the scopes of the
% node name, sorted: two looks at the same managers are equal terms.
% An entry without a pid is a manager that is starting, a dead
% manager has nothing to do
managers_work()->
  lists:sort([ {Manager, work(Manager)} || Scope <- scopes(), {_Term, Manager, _Ticket} <- entries(Scope) ]).

work(Manager) when is_pid(Manager)->
  process_info(Manager, [message_queue_len, status, reductions]);
work(_NotRegistered)->
  starting.

% The managers that are not at rest
busy(Managers) when is_list(Managers)->
  [ M || {_Manager, Work} = M <- Managers, not at_rest(Work) ];
busy(NodeProblem)->
  [NodeProblem].

at_rest(undefined)->
  true;
at_rest([{message_queue_len, 0}, {status, waiting}, {reductions, _}])->
  true;
at_rest(_Work)->
  false.

% The scopes of the node: the pg scope of a scope is registered as
% <Scope>_$pg$ (see elock:start_link/1)
scopes()->
  lists:filtermap(
    fun(Name)->
      case string:split(atom_to_list(Name), "_$pg$", trailing) of
        [Scope, ""]->
          {true, list_to_atom(Scope)};
        _->
          false
      end
    end,
    registered()
  ).

% The entries of the scope, none if its table is gone
entries(Scope)->
  try ets:tab2list(Scope)
  catch error:badarg-> []
  end.

unlock(Client, LockRef)->
  call(Client, fun()-> elock:unlock(LockRef) end).

%%-----------------------------------------------------------------
%%  The elock context of the client, read from its dictionary: the
%%  client may be blocked in elock:lock/4
%%-----------------------------------------------------------------
context(Client)->
  case rpc(node(Client), erlang, process_info, [Client, dictionary]) of
    {dictionary, Dictionary}->
      proplists:get_value(?CONTEXT, Dictionary, undefined);
    _->
      undefined
  end.

%%=================================================================
%%  Observation
%%=================================================================
locks(Scope)->
  ets:tab2list(Scope).

locks(Node, Scope)->
  rpc(Node, ets, tab2list, [Scope]).

manager(Scope, Term)->
  manager(node(), Scope, Term).

manager(Node, Scope, Term)->
  case rpc(Node, ets, lookup, [Scope, Term]) of
    [{Term, Manager, _LastTicket}] when is_pid(Manager)->
      Manager;
    _->
      undefined
  end.

%%-----------------------------------------------------------------
%%  The manager writes its pid into the entry after the client has
%%  got the lock - wait for it
%%-----------------------------------------------------------------
wait_manager(Scope, Term)->
  wait_manager(node(), Scope, Term).

wait_manager(Node, Scope, Term)->
  wait_until(fun()-> is_pid(manager(Node, Scope, Term)) end, ?DEADLINE),
  manager(Node, Scope, Term).

managers()->
  [ P || P <- erlang:processes(),
    process_info(P, current_function) =:= {current_function, {elock_manager, loop, 1}} ].

managers(Node)->
  rpc(Node, ?MODULE, managers, []).

kill_managers()->
  kill_managers(node()).

kill_managers(Node)->
  [ exit(P, kill) || P <- managers(Node) ],
  wait_until(fun()-> managers(Node) =:= [] end, ?DEADLINE).

%%=================================================================
%%  Waiting
%%=================================================================
%%-----------------------------------------------------------------
%%  Poll Fun every 10 ms until it returns true. At the deadline the
%%  error names the fun, its captured variables and the last value
%%-----------------------------------------------------------------
wait_until(Fun, Timeout)->
  Deadline = erlang:monotonic_time(millisecond) + Timeout,
  wait_until_loop(Fun, Timeout, Deadline).

wait_until_loop(Fun, Timeout, Deadline)->
  case Fun() of
    true->
      ok;
    Other->
      case erlang:monotonic_time(millisecond) >= Deadline of
        true->
          erlang:error({wait_until_timeout, #{
            timeout => Timeout,
            waited_for => describe(Fun),
            last_value => Other
          }});
        false->
          receive after ?POLL-> ok end,
          wait_until_loop(Fun, Timeout, Deadline)
      end
  end.

describe(Fun)->
  Info = erlang:fun_info(Fun),
  {
    proplists:get_value(module, Info),
    proplists:get_value(name, Info),
    proplists:get_value(env, Info)
  }.

%%-----------------------------------------------------------------
%%  No lock entry in the scope and no manager process on the node.
%%  The fun reports what is left, wait_until shows it at the deadline
%%-----------------------------------------------------------------
wait_idle(Scope)->
  wait_idle(node(), Scope).

wait_idle(Node, Scope)->
  wait_until(
    fun()->
      case {locks(Node, Scope), managers(Node)} of
        {[], []}->
          true;
        Left->
          {not_idle, Left}
      end
    end,
    ?DEADLINE
  ).

wait_dead(Pid)->
  MonRef = erlang:monitor(process, Pid),
  receive
    {'DOWN', MonRef, process, Pid, _Reason}->
      ok
  after ?DEADLINE->
    erlang:demonitor(MonRef, [flush]),
    erlang:error({wait_dead_timeout, Pid})
  end.

flush()->
  receive
    Message->
      [Message | flush()]
  after 0->
    []
  end.

no_message(Timeout)->
  receive
    Message->
      erlang:error({unexpected_message, Message})
  after Timeout->
    ok
  end.

%%=================================================================
%%  Collectors
%%
%%  A collector forwards every message it receives to the process
%%  that spawned it as {Collector, Message}. It poses as a manager,
%%  a client or a proxy in the unit tests of the modules. It dies
%%  with its parent
%%=================================================================
collector()->
  Collector = spawn(?MODULE, collector_init, [self()]),
  register_spawned(?COLLECTORS, Collector),
  Collector.

collector_init(Parent)->
  MonRef = erlang:monitor(process, Parent),
  collector_loop(Parent, MonRef).

collector_loop(Parent, MonRef)->
  receive
    {'DOWN', MonRef, process, Parent, _Reason}->
      exit(parent_down);
    Message->
      Parent ! {self(), Message},
      collector_loop(Parent, MonRef)
  end.

%%-----------------------------------------------------------------
%%  Exactly Count messages of the collector, in the order it got them
%%-----------------------------------------------------------------
collected(_Collector, 0)->
  [];
collected(Collector, Count)->
  receive
    {Collector, Message}->
      [Message | collected(Collector, Count - 1)]
  after ?DEADLINE->
    erlang:error({collected_timeout, Collector, Count, flush()})
  end.

%%-----------------------------------------------------------------
%%  Stop every collector spawned by the calling process (for
%%  end_per_testcase)
%%-----------------------------------------------------------------
stop_collectors()->
  stop_spawned(?COLLECTORS).

%%=================================================================
%%  Compiler
%%
%%  A receive that waits for a message carrying a fresh reference
%%  does not scan the messages that were in the mailbox before the
%%  reference was made - if the compiler can follow the reference
%%  from its creation to the receive. It follows a plain variable
%%  through the local calls only: a reference taken out of a record
%%  compiles and works, the receive just scans the whole mailbox.
%%  Nothing but the compiler tells the difference
%%=================================================================
%%-----------------------------------------------------------------
%%  What the compiler reports about the receives of the module (the
%%  recv_opt_info option), in the order of the source:
%%  [{{Function, Arity}, Info}], where Info is
%%  * reserved_receive_marker - a reference made here marks the
%%    mailbox position
%%  * passed_marker - and is passed on to a call
%%  * {used_receive_marker, {parameter, N}} - every clause of the
%%    receive matches the reference in the parameter N
%%  * unoptimized_selective_receive and the other complaints of
%%    beam_ssa_recv as they are
%%  The source is compiled once more the way the loaded module was
%%  (its include paths and macros), in memory: the loaded module is
%%  not touched
%%-----------------------------------------------------------------
recv_opt_info(Module)->
  Compile = Module:module_info(compile),
  Source = proplists:get_value(source, Compile),
  Options = [ Option || Option <- proplists:get_value(options, Compile, []),
    is_tuple(Option), lists:member(element(1, Option), [i, d]) ],
  {ok, Module, Binary, Warnings} = compile:file(Source, [recv_opt_info, return, binary, debug_info | Options]),
  {ok, {Module, [{abstract_code, {raw_abstract_v1, Forms}}]}} = beam_lib:chunks(Binary, [abstract_code]),
  Functions = [ {erl_anno:line(Anno), {Name, Arity}} || {function, Anno, Name, Arity, _Clauses} <- Forms ],
  [ {function_at(Location, Functions), recv_info(Info)}
    || {_File, FileWarnings} <- Warnings, {Location, beam_ssa_recv, Info} <- lists:sort(FileWarnings) ].

% The function the location belongs to: the last one that starts at
% or before its line
function_at({Line, _Column}, Functions)->
  function_at(Line, Functions);
function_at(Line, Functions)->
  {_Start, Function} = lists:last([ F || {Start, _} = F <- lists:sort(Functions), Start =< Line ]),
  Function.

% The creation site of a passed marker is an internal term of the compiler
recv_info({passed_marker, _CreatedBy})->
  passed_marker;
recv_info(Info)->
  Info.

%%=================================================================
%%  Utilities
%%=================================================================
%%-----------------------------------------------------------------
%%  The clients and the collectors a process spawns are listed in
%%  its dictionary, so that end_per_testcase can stop them all
%%-----------------------------------------------------------------
register_spawned(Key, Pid)->
  put(Key, [Pid | get_spawned(Key)]),
  ok.

get_spawned(Key)->
  case get(Key) of
    Pids when is_list(Pids)-> Pids;
    _-> []
  end.

stop_spawned(Key)->
  Pids = get_spawned(Key),
  erase(Key),
  [ stop(Pid) || Pid <- Pids ],
  ok.

rpc(Node, Module, Function, Args) when Node =:= node()->
  apply(Module, Function, Args);
rpc(Node, Module, Function, Args)->
  case rpc:call(Node, Module, Function, Args) of
    {badrpc, Reason}->
      erlang:error({badrpc, Node, Reason});
    Result->
      Result
  end.

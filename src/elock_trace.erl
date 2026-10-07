
%%=================================================================
%%  The trace of the lock calls, for the performance analysis.
%%
%%  The trace points (?TRACE of elock_trace.hrl) are compiled in
%%  only with the ELOCK_TRACE macro and write only between start/1
%%  and stop/0. An event is a row of a public ETS table of the node:
%%
%%      { Id, Step, Time, PID, Data }
%%
%%  The events stay in the memory of the node until dump/0, about
%%  200 bytes each: start/1 takes their limit, at the limit the
%%  trace stops by itself.
%%
%%  Id is the reference of the request for the steps of a lock call.
%%  Time is the OS system time in microseconds: the nodes of one
%%  host share the clock, so the rows of several nodes are joined by
%%  Id and compared by Time. PID is the process that has passed the
%%  step.
%%
%%  The steps of a lock call, in the order a request passes them.
%%  The client (elock_context):
%%    call          lock/4 has made the request. {Scope, Term, Nodes, IsShared}
%%    spawned       the workers of a request for several nodes are spawned
%%    w_start       a worker runs. Node
%%    w_done        the call of the worker to its node has returned. Node
%%    node_result   the client has the result of a node. Node
%%    queued_fwd    the client has #queued{} of a node and sends its held map. Node
%%    done          lock/4 returns. ok | {error, Reason}
%%    unlock        unlock/1 is called
%%  The proxy, the client or the worker on the node (elock_manager):
%%    w_enter       lock/1 runs on the node of the worker
%%    ticket        the ticket is taken. Ticket, 1: the lock was free
%%    no_manager    the entry has been recreated since the ticket, a new one is taken
%%    sent          the request is sent to the manager. Manager
%%    queued_rcv    the proxy has #queued{}
%%    verdict       the proxy has the verdict. {ok, Manager} | {error, Reason} | retry
%%  The manager (elock_manager), PID is the manager:
%%    m_init        started by the holder of ticket 1. {Scope, Term}
%%    m_request     a request is taken in by its ticket. Ticket
%%    m_postponed   a request is ahead of a missing ticket. {Ticket, Last}
%%    m_postpone_fired  the postpone timer has stepped over the missing tickets
%%    m_retry       the ticket of a request has been stepped over
%%    m_enqueue     the request waits. {Holders, QueueLength} | barging
%%    m_held        the held map of a waiting request goes to the graph. Size
%%    m_held_late   the held map of a request that waits no more
%%    m_grant       the request holds the lock
%%    m_deadlock    the verdict of the graph. delivered | late | upgrade
%%    m_timeout     the timeout of a waiting request
%%    m_dequeue     a waiting request leaves
%%    m_unlock      #unlock{} is received
%%    m_round       try_unlock/1 has found a new ticket
%%  The graph process (elock_graph), Id is the origin request:
%%    g_add         #add_edges{} is handled. {HeldSize, MailboxLength}
%%    g_probe       a hop is handled. {Locks, VisitedSize, MessageBytes, MailboxLength}
%%    g_walk        the walk has ended. origin | {Losers, VisitedSize, HopNodes}
%%    g_verdict     #deadlock{} goes to the manager of Id. origin | {closer, OriginRef}
%%    g_hop         a hop goes out. {Node, Locks, VisitedSize}
%%    g_remove      #remove_edges{} is handled
%%
%%  The reader is test/performance/util/performance_trace_report.erl
%%=================================================================
-module(elock_trace).
-moduledoc false.

%%=================================================================
%%	API
%%=================================================================
-export([
  start/1,
  stop/0,
  dump/0,
  event/3
]).

%%=================================================================
%%	The data of the trace points
%%=================================================================
-export([
  mailbox/0,
  walk/1
]).

%%=================================================================
%%	API
%%=================================================================
-define(LIMIT, {?MODULE, limit}).

%%-----------------------------------------------------------------
%%  The table outlives the caller and is never deleted: a process
%%  that has read the flag before stop/0 still has where to write.
%%  Limit is the most events the node keeps
%%-----------------------------------------------------------------
-spec start(pos_integer()) -> ok.
start(Limit)->
  case ets:whereis(?MODULE) of
    undefined->
      Caller = self(),
      Owner = spawn(fun()->
        ets:new(?MODULE, [
          named_table,
          public,
          duplicate_bag,
          {write_concurrency, auto}
        ]),
        Caller ! {?MODULE, self()},
        timer:sleep(infinity)
      end),
      receive
        {?MODULE, Owner}-> ok
      end;
    _Table->
      ets:delete_all_objects(?MODULE)
  end,
  persistent_term:put(?LIMIT, {atomics:new(1, []), Limit}),
  persistent_term:put(?MODULE, true),
  ok.

-spec stop() -> ok.
stop()->
  persistent_term:put(?MODULE, false),
  ok.

%%-----------------------------------------------------------------
%%  The events since start/1 as one binary, the list of the rows in
%%  the external term format: the reader may be on another node.
%%  Truncated: the trace has stopped at the limit
%%-----------------------------------------------------------------
-spec dump() -> {Truncated :: boolean(), binary()}.
dump()->
  {Counter, Limit} = persistent_term:get(?LIMIT),
  Events = ets:tab2list(?MODULE),
  ets:delete_all_objects(?MODULE),
  {atomics:get(Counter, 1) > Limit, term_to_binary(Events, [{compressed, 1}])}.

%%-----------------------------------------------------------------
%%  The event over the limit stops the trace of the node
%%-----------------------------------------------------------------
-spec event(atom(), term(), term()) -> ok.
event(Step, Id, Data)->
  {Counter, Limit} = persistent_term:get(?LIMIT),
  case atomics:add_get(Counter, 1, 1) of
    Count when Count =< Limit->
      Event = {Id, Step, os:system_time(microsecond), self(), Data},
      ets:insert(?MODULE, Event);
    _Over->
      stop()
  end,
  ok.

%%=================================================================
%%	The data of the trace points
%%=================================================================
-spec mailbox() -> non_neg_integer().
mailbox()->
  {message_queue_len, Length} = process_info(self(), message_queue_len),
  Length.

%%-----------------------------------------------------------------
%%  The result of elock_graph:walk/5 in numbers
%%-----------------------------------------------------------------
-spec walk({origin, term()} | {list(), map(), map()}) ->
  origin | {non_neg_integer(), non_neg_integer(), non_neg_integer()}.
walk({origin, _Lock})->
  origin;
walk({Losers, Visited, Hops})->
  {length(Losers), map_size(Visited), map_size(Hops)}.

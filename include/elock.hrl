
-ifndef(elock).
-define(elock,1).

%%-------------------------------------------------------------------------------
%% Types
%%-------------------------------------------------------------------------------
-type lock_key() :: {atom(), term(), node()}.
-type held_locks() :: #{lock_key() => pid()}.

-record(request,{
  % The ticket must stay first: postponed requests are sorted by it.
  queue :: pos_integer() | undefined,
  ref :: reference(),
  scope :: atom(),
  term :: term(),
  client :: pid(),
  proxy :: pid() | undefined,     % the client or a worker waiting for the verdict
  tag :: reference() | undefined, % the proxy's monitor, tags replies to the proxy
  shared :: boolean(),
  held_count :: non_neg_integer(), % held locks, the weight in a deadlock
  nodes :: nonempty_list(node()),
  timeout :: pos_integer() | undefined
}).

-record(unlock,{
  manager :: pid() | undefined,
  ref :: reference()
}).

% Manager -> client: the request waits, send the held map
-record(queued,{
  ref :: reference(),
  manager :: pid(),
  node :: node()
}).

% Client -> manager: the answer to #queued{} and every later grant
-record(add_held_locks,{
  ref :: reference(),
  held :: held_locks()
}).

% Graph -> manager, manager -> client: the request has lost
-record(deadlock,{
  ref :: reference(),
  winner :: lock_key() % the lock the winning request waits for
}).

% Manager -> graph of its node: the new holds of a waiting request
-record(add_edges,{
  lock :: lock_key(),           % the manager's lock
  ref :: reference(),
  weight :: non_neg_integer(),  % #request.held_count
  manager :: pid(),
  held :: held_locks()          % #add_held_locks.held as it came
}).

% Manager -> graph of its node: the request has stopped waiting
-record(remove_edges,{
  lock :: lock_key(),
  ref :: reference()
}).

% Graph -> graph of another node: expand these locks there. expand and
% visited are set for the hop, a launch walks without them
-record(deadlock_probe,{
  ref :: reference(),           % the origin request
  edge :: lock_key(),           % the lock the origin waits for
  manager :: pid(),             % the origin manager
  weight :: non_neg_integer(),  % the held count of the origin
  expand :: [lock_key()] | undefined, % the locks to expand on the receiving node
  visited :: #{lock_key() => true} | undefined % the locks this branch has expanded or scheduled
}).

%%-------------------------------------------------------------------------------
%% TRACING
%%-------------------------------------------------------------------------------
-include("elock_trace.hrl").

%%-------------------------------------------------------------------------------
%% LOGGING
%%-------------------------------------------------------------------------------

-ifndef(TEST).

-define(MFA_METADATA, #{
  mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
  line => ?LINE
}).

-define(LOGERROR(Text),          logger:error(Text, [], ?MFA_METADATA)).
-define(LOGERROR(Text,Params),   logger:error(Text, Params, ?MFA_METADATA)).
-define(LOGWARNING(Text),        logger:warning(Text, [], ?MFA_METADATA)).
-define(LOGWARNING(Text,Params), logger:warning(Text, Params, ?MFA_METADATA)).
-define(LOGINFO(Text),           logger:info(Text, [], ?MFA_METADATA)).
-define(LOGINFO(Text,Params),    logger:info(Text, Params, ?MFA_METADATA)).
-define(LOGDEBUG(Text),          logger:debug(Text, [], ?MFA_METADATA)).
-define(LOGDEBUG(Text,Params),   logger:debug(Text, Params, ?MFA_METADATA)).

-else.

-define(LOGERROR(Text),           ct:pal("error: " ++ Text)).
-define(LOGERROR(Text, Params),   ct:pal("error: " ++ Text, Params)).
-define(LOGWARNING(Text),         ct:pal("warning: " ++ Text)).
-define(LOGWARNING(Text, Params), ct:pal("warning: " ++ Text, Params)).
-define(LOGINFO(Text),            ct:pal("info: " ++ Text)).
-define(LOGINFO(Text, Params),    ct:pal("info: " ++ Text, Params)).
-define(LOGDEBUG(Text),           ct:pal("debug: " ++ Text)).
-define(LOGDEBUG(Text, Params),   ct:pal("debug: " ++ Text, Params)).

-endif.


-endif.


-ifndef(elock).
-define(elock,1).

%%-------------------------------------------------------------------------------
%% Types
%%-------------------------------------------------------------------------------
-type lock_key() :: {atom(), term(), node()}.
-type held_locks() :: #{lock_key() => pid()}.

-record(request,{
  % The ticket must stay first: postponed requests are sorted by it.
  ticket :: pos_integer() | undefined,
  ref :: reference(),
  scope :: atom(),
  term :: term(),
  client :: pid(),
  proxy :: pid() | undefined,     % the client or a worker waiting for the verdict
  tag :: reference() | undefined, % the proxy's monitor, tags replies to the proxy
  shared :: boolean(),
  timeout :: pos_integer() | undefined
}).

-record(unlock,{
  ref :: reference()
}).

% Manager -> proxy -> client: the request has joined the wait queue
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

% Walker -> request client; manager -> proxy: the request has lost
-record(deadlock,{
  ref :: reference(),
  winner :: lock_key() % the lock the winning request waits for
}).

% Client graph worker -> update call on the waiting node: new holds
-record(add_edges,{
  lock :: lock_key(),           % the manager's lock
  ref :: reference(),
  birth :: non_neg_integer(),   % priority fixed for the life of the context
  client :: pid(),              % the request client receiving verdicts
  holds :: held_locks()         % the context's holds or a later grant
}).

% Client graph worker -> removal cast on the waiting node: stopped waiting
-record(remove_edges,{
  lock :: lock_key(),
  ref :: reference()
}).

% Walker -> probe cast on another node: expand these locks there.
% expand and visited are set for the hop, a launch walks without them
-record(deadlock_probe,{
  ref :: reference(),           % the origin request
  edge :: lock_key(),           % the lock the origin waits for
  client :: pid(),              % the origin client receiving verdicts
  birth :: non_neg_integer(),   % the origin context's fixed priority
  expand :: [lock_key()] | undefined, % the locks to expand on the receiving node
  visited :: #{lock_key() => true} | undefined % the locks this branch has expanded or scheduled
}).

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

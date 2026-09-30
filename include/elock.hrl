
-ifndef(elock).
-define(elock,1).

%%-------------------------------------------------------------------------------
%% Types
%%-------------------------------------------------------------------------------
-record(request,{
  queue,        % the ticket. Must stay the first field: the manager keeps
                % the postponed requests in an ordset sorted by it
  ref,
  scope,
  term,
  client,
  proxy,        % the process that waits for the verdict: the client or a worker
  tag,          % the proxy's monitor on the manager, tags every reply to the proxy
  shared,
  held_count,   % the number of locks the client holds, the weight in a deadlock
  nodes,
  timeout
}).

-record(unlock,{
  manager,
  ref
}).

% Manager -> client: the request waits, send the held map
-record(queued,{
  ref,
  manager,
  node
}).

% Client -> manager: the answer to #queued{} and every later grant
-record(add_held_locks,{
  ref,
  held      % #{ {Scope, Term, Node} => Manager }
}).

% To the client, or to the origin manager as the answer to its probe
-record(deadlock,{
  ref,
  winner    % {Scope, Term, Node} - the lock the winning request waits for
}).

-record(deadlock_probe,{
  ref,      % the origin request
  edge,     % {Scope, Term, Node} - the lock the origin waits for
  manager,  % the origin manager
  weight,   % the held count of the origin
  sent_to   % #{ ManagerPID => true } - the managers that have got the probe
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

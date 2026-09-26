
-ifndef(elock).
-define(elock,1).

%%-------------------------------------------------------------------------------
%% Types
%%-------------------------------------------------------------------------------
-record(request,{
  queue,
  ref,
  scope,
  term,
  client,
  proxy,
  shared,
  held,
  nodes,
  timeout
}).

-record(unlock,{
  manager,
  ref
}).

-record(queued,{
  ref,
  manager,
  node
}).

-record(add_held_locks,{
  ref,
  held
}).

% The deadlock verdict: to the client, and to the origin manager in reply to its probe (see elock_graph)
-record(deadlock,{
  ref
}).

-record(deadlock_probe,{
  ref,      % the origin request
  edge,     % {Term, Node} - the lock the origin waits for
  manager,  % the origin manager, the verdict is sent back to it
  weight,   % the held count of the origin - the lighter loses, the coin settles a tie
  sent_to   % #{ ManagerPID => true } - managers this probe has already been sent to
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


-ifndef(elock).
-define(elock,1).

%%-------------------------------------------------------------------------------
%% Types
%%-------------------------------------------------------------------------------
-define(lock(T),{lock,T}).
-define(queue(R,Q),{queue,R,Q}).

-define(holder(H,T),{holder,H,T}).
-define(wait(T),{wait,T}).

%%-------------------------------------------------------------------------------
%% LOGGING
%%-------------------------------------------------------------------------------

-ifndef(TEST).

-define(MFA_METADATA, #{
  mfa => {?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY},
  line => ?LINE
}).

-define(PREFIX, "\r\n"++pid_to_list(self()) ++": "++ atom_to_list(?MODULE) ++":" ++ atom_to_list(?FUNCTION_NAME)++":"++integer_to_list(?LINE)).

-define(LOGERROR(Text),          logger:error(Text, [], ?MFA_METADATA)).
-define(LOGERROR(Text,Params),   logger:error(Text, Params, ?MFA_METADATA)).
-define(LOGWARNING(Text),        logger:warning(Text, [], ?MFA_METADATA)).
-define(LOGWARNING(Text,Params), logger:warning(Text, Params, ?MFA_METADATA)).
-define(LOGINFO(Text),           io:format(?PREFIX ++ Text ++"\r\n")).
-define(LOGINFO(Text,Params),    io:format(?PREFIX ++Text++"\r\n", Params)).
-define(LOGDEBUG(Text),          io:format(?PREFIX ++Text++"\r\n")).
-define(LOGDEBUG(Text,Params),   io:format(?PREFIX ++Text"\r\n", Params)).

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

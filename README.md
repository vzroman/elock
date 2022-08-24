# elock
Erlang library for distributed term locks.

I tried to keep API as simple as possible.

I appreciate any pull requests for bug fixing, tests or extending the functionality.

API
-----

    elock:start_link( SomeUniqueAtom )
    
    Call it from an OTP supervisor as a permanent worker. Example:
    
    MyLockServer = #{
        id=>'$mylocks',
        start=>{elock,start_link,[ '$mylocks' ]},
        restart=>permanent,
        shutdown=> <It's up to you>
        type=>worker,
        modules=>[elock]
    },

    The process registers itself as '$mylocks', be carefull.
    
    Ok now you are ready for local locks. If you need distributed locks do the same
    at your other nodes.
    
    If you need one more heap of lock start add another one to a supervisor. 
    
    If you want to lock any erlang term call:
    
    {ok,Unlock} | {error,timeout} =  elock:lock(Locks, Term, IsShared, Nodes, Timeout )

    Locks is your '$mylocks'
    Term is any erlang term
    Timeout is Milliseconds or infinity
    
    Others are more interesting:

    set IsShared = true if it is enough for you to be sure that the term is locked may be
    even not by you. 
    
    Nodes is where else you want to lock the Term. If the Nodes is [] the Term
    will be locked only locally

    When you need to unlock the Term call Unlock() from returned to you {ok,Unlock}.

    Avoid setting lock on the term you'v already locked, you will get a deadlock.

    that's it.
    
    
    
BUILD
-----
    Add it as a dependency to your application and you are ready (I use rebar3)
    {deps, [
        ......
        {elock, {git, "git@github.com:vzroman/elock.git", {branch, "main"}}}
    ]}.

TODO
-----
    Tests!!!
    
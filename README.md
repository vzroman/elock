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
    
    Ok now you are ready for local locks. If you need distributed locks do the same
    at your other nodes.
    
    If you need one more heap of lock add another one to a supervisor with a 
    different name. 
    
    If you want to lock any erlang term call:
    
    {ok,Unlock} | {error,timeout} | {error, deadlock} 
        =  elock:lock(Locks, Term, IsShared, Timeout )

    Distributed locks are almost the same:

    {ok,Unlock} | {error,timeout} | {error, deadlock} 
        =  elock:lock(Locks, Term, IsShared, Timeout, Nodes )

    Locks is your '$mylocks'
    Term is any erlang term
    IsShared = true | false. Set it true if it is enough for you to be sure 
                that the term is locked may be even not by you and nobody 
                has not shared lock on it.
    Timeout is Milliseconds or infinity
    Nodes is a list of nodes where you want to lock the Term

    When you need to unlock the Term call Unlock() from returned to you {ok,Unlock}.

    Deadlocks are detected automatically. If a requested lock leads to a deadlock 
    then one of the requests will get:
        {error, deadlock}
    Most cases deadlock will get a process which has less locked terms.
    If you already have locked a Term and try to lock it with a different IsShared
    type then the previous lock is automatically released and the new request
    is queued in the end.

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
    
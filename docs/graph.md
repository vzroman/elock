The idea is to optimize deadlock detection for project without substantial performnce impact on other paths. 
The idea is to use a dedicted process (elock_graph) that starts with the application. The graph is ets based. A manager just casts to it with add_edges and remove_edges. The graph may cast #deadlock to managers.  The graph handles all the edges and transmits the edges that cover other nodes to participating neigbours (eventual consistency). 

It's a brainstorming step. No code changes. Check the idea thoroughly. What are potential pitfalls?


The idea is to optimize deadlock detection for project without substantial performnce impact on other paths. 

The idea is based on 'pool' branch. Each manager keeps it's graph in ets. elock_graph transforms the ets, does all the checks against it and only sends #deadlock to original manager or itself and forwards the probe to other managers when needed. So graph call doesn't change the state but ets only.


It's a brainstorming step. No code changes. Check the idea thoroughly. What are potential pitfalls?

----------------------
ets (bag) record:
{ 
    {Scope, Node, Term} = Hold,
    {{Scope, Node, Term} = Wait, Manager|RemoteGraph, Ref}
}

add_edges(
    Wait = {Scope, Node, Term}, 
    Hold = [{Scope, Node, Term}], 
    Manager, 
    Ref
)->
    [begin
        Holder = 
            if 
                is_local(Hold) -> 
                    Manager; 
                true ->
                     
                    {?MODULE, Node},
            end,        
        ets:
    end],


ets:insert(
    Graph, 
    [{Hold, {Wait, if is_local(Hold)-> Manager; true -> RemoteGraph}}]
)


case ets:lookup(Graph, Wait) of
    [] -> ignore;
    Holders ->
        [] 



remove_edges({Scope, Term, Node}, [{Scope, Node, Term}], Manager, Ref)


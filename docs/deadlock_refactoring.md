The idea is to use elock_managers themselves as deadlock detectors:
the client passes held_locks in the request as:
#{
    {Term,Node} => ManagerPID
}
the managers handles a new field 'holds' as:
#{
    {Term, Node} => #{
        Ref => [{Term,Node}]
    }
}
#client also gets a new field 'holds' = [{Term,Node}] from the lates client's request

when a request is received and goes into the waiting queue its 'held_locks' are merged into 'holds'. 
then the manager sends:
#waiting{
    ref = Ref,
    term = {Term, Node}
    manager = ManagerPID,
    holds = [{Term, Node}] % keys of 'held_locks' from the original request, just to weigh locks on deadlock detection
    sent_to = #{ManagerPID1 => true, ManagerPID2 => true}  % all the alreday notified managers
}
to every manager from 'holds' after the merge.
On receiving #waiting{} a manager checks  if #waiting.term is in its 'holds'. If yes the manager weighs 'client_locks' against 'holds' locks of each holding client.
The smallest loses. If the smallest is the #waiting{} then the managers sends #deadlock{ ref = Ref } back and the propogation stops. If the smallest is one of the manager's clients then it gets #deadlock{} but the propogation continues. 
The propogation: the manger resends the #waiting to every manager among it's 'holds' excluding 'sent_to'. All the new managers are merged into 'sent_to'.


weighing, tie break:
Tie = lists:sort([Ref1, Ref2]),
Winner = lists:nth(erlang:phash2(Tie, 2) + 1, Tie)
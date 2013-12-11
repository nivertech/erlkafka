-module(erlkafka_profile).

-compile([export_all]).

from_file(Topic, FN, Count) ->
    StartTime = now(),
    {ok, File} = file:open(FN, [read, binary]),
    LineCount = round(Count / 100.0),
    {_, _, Lines} = lists:foldl(fun next_line/2, {FN, File, []}, lists:seq(1, LineCount)),
    file:close(File),
    [[gen_server:call(ballermann:pid(producer_pool), {add, Topic, L})||L<-Lines]||_<-lists:seq(1, 100)],
    Sync = fun(Pid) -> gen_server:call(Pid, sync) end,
    ballermann:apply_within(producer_pool, {erlang, list_to_atom, ["sync"]}, Sync),
    Elapsed = timer:now_diff(now(), StartTime) / 1000000,
    io:format("\n~p seconds for ~p messages: ~p ops\n", [Elapsed, Count, Count / Elapsed]).

next_line(_, {FN, F, Acc}) ->
  case io:get_line(F, "") of
      eof ->
        file:close(F),
        {ok, F2} = file:open(FN, [read, binary]),
        {FN, F2, [io:get_line(F2, "") | Acc]};
      Data ->
        {FN, F, [Data | Acc]}
  end.



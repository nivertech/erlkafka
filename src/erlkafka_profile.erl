-module(erlkafka_profile).

-compile([export_all, {inline, [{add_and_wait, 2}]}]).

from_file(Topics, FN, Count) ->
    from_file(Topics, FN, Count, undefined).

from_file(Topics, FN, Count, Rate) ->
    {ok, File} = file:open(FN, [read, binary]),
    Series = 10,
    LineCount = round(Count / Series * 1.0),
    {_, _, Lines} = lists:foldl(fun next_line/2, {FN, File, []}, lists:seq(1, LineCount)),
    file:close(File),
    StartTime = now(),
    case Rate of
        undefined ->
            [[gen_server:call(ballermann:pid(producer_pool), {add, one_of(Topics), L})||L<-Lines]||_<-lists:seq(1, Series)];
        _ ->
            TimePerMsg = 1.0 / Rate * 1000000.0, %micros
            [lists:foldl(fun add_and_wait/2, {Topics, TimePerMsg, 0, os:timestamp()}, Lines)||_<-lists:seq(0, (Series - 1))]
    end,
    Sync = fun(Pid) -> gen_server:call(Pid, sync) end,
    ballermann:apply_within(producer_pool, {erlang, list_to_atom, ["sync"]}, Sync),
    Elapsed = timer:now_diff(now(), StartTime) / 1000000,
    io:format("\n~p seconds for ~p messages: ~p ops\n", [Elapsed, Count, Count / Elapsed]).

add_and_wait(L, {Topics, TimePerMsg, N, SeriesStartTime}) ->
    case N rem 1000 =:= 0 of
        true ->
            SeriesTimeElapse = timer:now_diff(os:timestamp(), SeriesStartTime),
            case (TimePerMsg * N * 1.0) - SeriesTimeElapse * 1.0 of
                TTS when TTS > 0.0 ->
                    timer:sleep(round(TTS/1000.0));
                _ -> noop
            end;
        false -> noop
    end,
    gen_server:call(ballermann:pid(producer_pool), {add, one_of(Topics), L}),
    {Topics, TimePerMsg, N + 1, SeriesStartTime}.


next_line(_, {FN, F, Acc}) ->
  case io:get_line(F, "") of
      eof ->
        file:close(F),
        {ok, F2} = file:open(FN, [read, binary]),
        {FN, F2, [io:get_line(F2, "") | Acc]};
      Data ->
        {FN, F, [Data | Acc]}
  end.

one_of(List) ->
    lists:nth(random:uniform(length(List)), List).

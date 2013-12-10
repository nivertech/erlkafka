%%%-------------------------------------------------------------------
%%% File     : erlkafka_app.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------

-module(erlkafka_app).
-author("Milind Parikh <milindparikh@gmail.com> [milindparikh.com]").
-behaviour(application).

-export([
    start/0,
    start/2,
    stop/1]).


start() ->
    start(normal, []).

start(normal, _Args) ->
    erlkafka_root_sup:start_link(1).


stop() ->
    Res = application:stop(erlkafka_app),
    application:stop(ezk),
    application:stop(uuid),
    application:stop(crypto),
    Res.

stop(_State) ->
    ok.



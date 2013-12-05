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
    ensure_started(crypto),
    %ensure_started(uuid),
    %%conditionally_start(ezk, enable_kafka_autodiscovery),
    %% this is misleading - should investigate why it's linking
    %%
    %% CONSIDER:
    %% application:start(erlkafka_app).
    erlkafka_root_sup:start_link(1).


stop() ->
    Res = application:stop(erlkafka_app),
    application:stop(ezk),
    application:stop(uuid),
    application:stop(crypto),
    Res.

stop(_State) ->
    ok.


%% Internal functions
ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

conditionally_start(AppName, EnvVar) ->
    case application:get_env(EnvVar) of
        true ->
            ensure_started(AppName);
        _ -> %% could be false, or undefined
            ok
    end.

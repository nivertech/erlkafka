%%%-------------------------------------------------------------------
%%% File     : erlkafka_root_sup.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
-module(erlkafka_root_sup).
-author("Milind Parikh <milindparikh@gmail.com> [http://www.milindparikh.com]").
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(_Params) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    RestartStrategy = {one_for_one, 1, 60*60}, % You get a second chance!
    Children = [
     {erlkafka_server_sup,
      {erlkafka_server_sup, start_link,[]},
      permanent,
      infinity,
      supervisor,
      [erlkafka_server_sup]},
     {erlkafka_stream_consumer_sup,
      {erlkafka_stream_consumer_sup, start_link,[]},
      permanent,
      infinity,
      supervisor,
      [erlkafka_stream_consumer_sup]}
    ],
    {ok, {RestartStrategy, Children}}.


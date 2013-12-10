-module(erlkafka_producer_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

-export([init/1]).

start_link(ProducerCount) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, ProducerCount).

init(ProducerCount) ->
    ProducerSpec = fun(I) ->
        {list_to_atom("erlkafka_producer" ++ integer_to_list(I)),
        {erlkafka_producer, start_link, []},
        permanent, brutal_kill, worker, [erlkafka_producer]}
    end,
    RestartStrategy = {one_for_one, 1000, 10},
    {ok, {RestartStrategy, [ProducerSpec(I) || I <- lists:seq(1, ProducerCount)]}}.


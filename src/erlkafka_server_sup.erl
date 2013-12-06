%%%-------------------------------------------------------------------
%%% File     : erlkafka_server_sup.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
-module(erlkafka_server_sup).
-author("Milind Parikh <milindparikh@gmail.com> [http://www.milindparikh.com]").
-behaviour(supervisor).

-export([start_link/1, start_link/0,
         get_ids/0,
         get_random_broker_instance_from_pool/1
        ]).
-export([init/1]).

-define(DEFAULT_POOL_COUNT, 5).


%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

start_link() ->
    start_link(erlkafka_protocol:get_dynamic_list_of_brokers()).

start_link(Params) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Params]).

get_random_broker_instance_from_pool(Broker) ->
    BrokerPoolCount = param(broker_pool_count, ?DEFAULT_POOL_COUNT),
    Pids = get_ids(),
    BrokerInstance = Broker*BrokerPoolCount + random:uniform(BrokerPoolCount),
    lists:filter(
            fun({Child, Id}) ->
                Id =:= BrokerInstance andalso is_pid(Child)
            end,
            Pids).

get_ids() ->
    [{Child, Id}
     || {Id, Child, _Type, _Modules} <- supervisor:which_children(?MODULE),
        Child /= undefined, Id /= 0].

%%%-------------------------------------------------------------------
%%%                         SUPERVISOR CB FUNCTIONS
%%%-------------------------------------------------------------------
init([Params]) ->
    BrokerPoolCount = param(broker_pool_count, ?DEFAULT_POOL_COUNT),
    RestartStrategy = {one_for_one, 10, 60*60}, % allowing 10 crashes per hour
    Children = lists:flatten(
        lists:map(
            fun({Broker, {Host, Port}}) ->
                io:format("starting with ~p\n", [{Broker, {Host, Port}}]),
                lists:map(
                    fun(X) ->
                        {Broker*BrokerPoolCount + X,
                         {erlkafka_server, start_link, [[Host, Port]]},
                         transient,
                         brutal_kill,
                         worker,
                         [erlkafka_server]}
                    end,
                    lists:seq(1, BrokerPoolCount))
            end,
            Params
        )
    ),
    {ok, {RestartStrategy, Children}}.


%%%-------------------------------------------------------------------
%%%                         INTERNAL  FUNCTIONS
%%%-------------------------------------------------------------------
param(Name, Default)->
    case application:get_env(erlkafka, Name) of
        {ok, Value} -> Value;
        _-> Default
    end.


%%%-------------------------------------------------------------------
%%% File     : erlkafka.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
-module(erlkafka).
-author('Milind Parikh <milindparikh@gmail.com>').
-include("erlkafka.hrl").

-export([start/0, get_kafka_stream_consumer/4, uuid/0]).

%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

start() ->
    ensure_started(crypto),
    application:start(crypto),
    application:start(sasl),
    application:load(ezk),
    application:set_env(ezk, default_servers, [{"test-kafka-one.local", 2181, 30000, 10000}, {"test-kafka-two.local", 2181, 30000, 10000}, {"test-kafka-three.local", 2181, 30000, 10000}]),
    %application:set_env(ezk, default_servers, [{"localhost", 2181, 30000, 10000}]),
    application:start(ezk),
    application:start(erlkafka),
    ballermann:balance(erlkafka_producer_sup, producer_pool).

%% Internal functions
ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.

uuid() ->
    uuid:to_string(uuid:uuid4()).

get_kafka_stream_consumer(Broker, Topic, Partition, Offset) ->
    {Mega, Sec, Micro} = now(),
    random:seed(Mega, Sec, Micro),
    UuidKSR = uuid(),
    UuidKSC = uuid(),
    {ok, KsrPid} = supervisor:start_child(
         erlkafka_stream_consumer_sup,
         {UuidKSR,
          {erlkafka_sequential_reader,start_link,[[Broker,Topic,Partition,Offset]]},
          temporary,   % never restart
          brutal_kill,
          worker,
          [erlkafka_sequential_reader]}
     ),
     {ok, KscPid} = supervisor:start_child(
         erlkafka_stream_consumer_sup,
         {UuidKSC,
          {erlkafka_stream_consumer, start_link, [KsrPid]},
          temporary,   % never restart
          brutal_kill,
          worker,
          [erlkafka_stream_consumer]}
     ),
     {erlkafka_stream_consumer:get_stream_function(),
      erlkafka_stream_consumer:get_terminate_function(),
      KscPid}.

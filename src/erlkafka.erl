%%%-------------------------------------------------------------------
%%% File     : erlkafka.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
-module(erlkafka).
-author('Milind Parikh <milindparikh@gmail.com>').
-include("erlkafka.hrl").

-export([get_kafka_stream_consumer/4, uuid/0]).
-import(uuid).

%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------
uuid() ->
    uuid:to_string(uuid:get_v4()).

get_kafka_stream_consumer(Broker, Topic, Partition, Offset) -> 
    {A1, A2, A3} = now(),
    random:seed(A1, A2, A3),
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

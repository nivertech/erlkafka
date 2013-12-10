%%%-------------------------------------------------------------------
%%% File     : erlkafka_protocol.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------
-module(erlkafka_protocol).
-author('Milind Parikh <milindparikh@gmail.com>').

%% Initial philosophy is derived from
%%     https://github.com/wooga/kafka-erlang.git
%% The kafka protocol is tested against kafka 0.7.1
%% It requires ezk (https://github.com/infinipool/ezk.git) for dynamic discovery

-compile([export_all]).

-define(RQ_TYPE_PRODUCE, 0).
-define(RQ_TYPE_FETCH, 1).
-define(RQ_TYPE_MULTIFETCH, 2).
-define(RQ_TYPE_MULTIPRODUCE, 3).
-define(RQ_TYPE_OFFSETS, 4).

%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

%%%-------------------------------------------------------------------
%%%                         API/FETCH FUNCTIONS
%%%-------------------------------------------------------------------


%%  @doc The default fetch request; which assumes a default partition of 0
-spec fetch_request(Topic::binary(),
                    Offset::integer(),
                    MaxSize::integer()) -> binary().
fetch_request(Topic, Offset, MaxSize) ->
    fetch_request(Topic, Offset,0, MaxSize).

%%  @doc The fetch request with partition also passed in
-spec fetch_request(Topic::binary(),
                    Offset::integer(),
                    Partition::integer(),
                    MaxSize::integer()) -> binary().
fetch_request(Topic, Offset,Partition, MaxSize) ->
    TopicSize = byte_size(Topic),
    RequestSize = 2 + 2 + TopicSize + 4 + 8 + 4,
    <<RequestSize:32/integer,
      ?RQ_TYPE_FETCH:16/integer,
      TopicSize:16/integer,
      Topic/binary,
      Partition:32/integer,
      Offset:64/integer,
      MaxSize:32/integer>>.

%%  @doc The multi-fetch request with partition also passed in
%%       TopicPartitionOffset is {Topic, Partition, Offset, Maxsize}
%%       [{"test", 0, 0, 100}, {"test2", 0,0, 200}]
-spec multi_fetch_request(TopicPartitionOffsets::list()) -> binary().
multi_fetch_request(TopicPartitionOffsets) ->
    TPOsSize = size_multi_fetch_tpos(TopicPartitionOffsets),
    TopicPartitionCount = length(TopicPartitionOffsets),
    RequestLength = 2 + 2 + TPOsSize,
    RequestHeader = <<RequestLength:32/integer,
                      ?RQ_TYPE_MULTIFETCH:16/integer,
                      TopicPartitionCount:16/integer>>,
    RequestBody = lists:foldr(
        fun({Topic, Partition, Offset, MaxSize}, Acc) ->
            TopicLength = byte_size(Topic),
            <<TopicLength:16/integer,
              Topic/binary,
              Partition:32/integer,
              Offset:64/integer,
              MaxSize:32/integer,
              Acc/binary >>
        end,
        <<"">>,
        TopicPartitionOffsets),
    <<RequestHeader/binary, RequestBody/binary>>.

%% @doc parse the fetched messages
-spec parse_messages(Bs::binary()) -> {[binary()], non_neg_integer()}.
parse_messages(Bs) ->
    parse_messages(Bs, [], 0).

%%%-------------------------------------------------------------------
%%%                         API/PRODUCE FUNCTIONS
%%%-------------------------------------------------------------------

%%  @doc The default produce request with the only default partition.
-spec produce_request(Topic::binary(), Messages::list(binary())) -> binary().
produce_request(Topic,  Messages) ->
    produce_request(Topic, 0, 1, 0, Messages).

%%  @doc The default produce request.
%%

-spec produce_request(Topic::binary(),
                      Partition::integer(),
                      Messages::list(binary())) -> binary().
produce_request(Topic, Partition,  Messages) ->
    produce_request(Topic, Partition, 1, 0, Messages).

%%  @doc The  produce request with passed in Magic and Compression
-spec produce_request(Topic::binary(),
                      Partition::integer(),
                      Magic::integer(),
                      Compression::integer(),
                      Messages::list()) -> binary().
produce_request(Topic, Partition, Magic, Compression, Messages) ->
    MessagesLength = size_of_produce_messages(Messages),
    TopicSize = byte_size(Topic),
    RequestSize = 2 + 2 + TopicSize + 4 + 4 + MessagesLength,
    ProducedMessages = lists:foldr(
        fun(X, A) ->
            KafkaMessage = produce_message(X, Magic, Compression),
            <<KafkaMessage/binary, A/binary>>
        end,
        <<"">>,
        Messages),
    <<RequestSize:32/integer,
      ?RQ_TYPE_PRODUCE:16/integer,
      TopicSize:16/integer,
      Topic/binary,
      Partition:32/integer,
      MessagesLength:32/integer,
      ProducedMessages/binary>>.

%%  @doc The multi-produce request with partition also passed in
%%       [{<<"topic1">>,  0, [{Magic, Compression, <<"hi">>}, {Magic, Compression, <<"second hihi">>}]},
%%       [{<<"topic2">>,  0, [{Magic, Compression, <<"hi2">>}, {Magic, Compression, <<"second hihi2">>}]},

-spec multi_produce_request(TopicPartitionMessages::list()) -> binary().
multi_produce_request(TopicPartitionMessages) ->
    TPMSize = size_multi_produce_tpms(TopicPartitionMessages),
    RequestLength = 2+2+TPMSize,
    TopicPartitionCount = length(TopicPartitionMessages),
    RequestHeader = <<RequestLength:32/integer,
                      ?RQ_TYPE_MULTIPRODUCE:16/integer,
                      TopicPartitionCount:16/integer>>,
    RequestBody = lists:foldr(
        fun({Topic, Partition, Messages},Acc1) ->
            TopicLength = byte_size(Topic),
            {MessagesLength, MessagesBin} =
            lists:foldr(
                fun({Magic, Compression, MsgBin}, {Count, Bin}) ->
                    KafkaMessage=produce_message(MsgBin,Magic, Compression),
                    {byte_size(KafkaMessage) + Count, <<KafkaMessage/binary, Bin/binary>>}
                end,
                {0, <<"">>},
                Messages),
            <<TopicLength:16/integer,
              Topic/binary,
              Partition:32/integer,
              MessagesLength:32/integer,
              MessagesBin/binary,
              Acc1/binary>>
        end,
        <<"">>,
        TopicPartitionMessages),
    <<RequestHeader/binary, RequestBody/binary>>.

%%%-------------------------------------------------------------------
%%%                         API/OFFSETS FUNCTIONS
%%%-------------------------------------------------------------------

%% @doc The offset request with given time
-spec offset_request(Topic::binary(),
                     Partition::integer(),
                     Time::integer(),
                     MaxNumberOfOffsets::integer()) -> binary().
offset_request(Topic, Partition, Time, MaxNumberOfOffsets) ->
    TopicSize = byte_size(Topic),
    RequestLength = 2+2+TopicSize+4+8+4,
    <<RequestLength:32/integer,
      ?RQ_TYPE_OFFSETS:16/integer,
      TopicSize:16/integer,
      Topic/binary,
      Partition:32/integer,
      Time:64/integer,
      MaxNumberOfOffsets:32/integer>>.


%% @doc Parsing the results of the offset request
-spec parse_offsets(binary()) -> {[byte()]}.
parse_offsets(<<NumOffsets:32/integer, Ds/binary>>) ->
    parse_offsets(Ds, [], NumOffsets).

%%%-------------------------------------------------------------------
%%%                         API/BROKER FUNCTIONS
%%%-------------------------------------------------------------------

%% @ If enable_kafka_autodiscovery is enabled under application erlkafka
%%   Then looks for zookeeper based broker registry
%%   If not, then looks under a static definition of kafka_brokers
get_list_of_brokers() ->
    case application:get_env(erlkafka, enable_kafka_autodiscovery) of
        undefined -> [];
        {ok, false} ->
            case application:get_env(erlkafka, kafka_brokers) of
                   undefined -> [];
                   {ok, X} -> X
            end;
        {ok, true} ->
            get_dynamic_list_of_brokers()
    end.

%% @ This is to get all possible broker-partition combinations hosting
%%   a specific topic. Currently only implemented through the
%%   auto discovery in zookeeper (and therefore requires ezk).
get_list_of_broker_partitions(Topic) ->
    get_dynamic_list_of_broker_partitions(Topic).

%%%-------------------------------------------------------------------
%%%                         INTERNAL FUNCTIONS
%%%-------------------------------------------------------------------
get_dynamic_list_of_broker_partitions(Topic) ->
    DynList = lists:flatten(
        lists:foldr(
            fun({Broker, Partitions}, Acc1) ->
                [lists:foldr(fun(Partition,Acc2) -> [{Broker,Partition}|Acc2] end,
                             [],
                             Partitions)|Acc1]
            end,
            [],
            lists:foldr(fun({BrokerId, NumPartitions}, Acc3) ->
                            [{BrokerId, lists:seq(0, NumPartitions )} |Acc3]
                        end,
                        [],
                        lists:foldr(fun({BrokerId, _, _ }, Acc4) ->
                                        [{BrokerId,
                                          get_num_partitions_topic_broker(Topic, BrokerId)
                                         } | Acc4]
                                    end,
                                    [],
                                    get_list_of_brokers())
                        )
        )
    ),
    DynList.

get_num_partitions_topic_broker(Topic, Broker) ->
    NewTopic = binary_to_list(Topic),
    {ok, Conn} = ezk:start_connection(),
    NumPartitions = case ezk:get(Conn, get_path_for_broker_topics()++NewTopic++"/" ++ integer_to_list(Broker)) of
        {ok, {X, _}} -> list_to_integer(binary_to_list(X));
        {error, no_dir} -> 0
    end,
    ezk:end_connection(Conn, ""),
    NumPartitions.

get_dynamic_list_of_brokers() ->
    {ok, Conn} = ezk:start_connection(),
    {ok, RawListBrokers} = ezk:ls(Conn, "/brokers/ids"),

    ParseBroker = fun(Id, {ok, {Json, _}}) ->
      {Data} = jiffy:decode(Json),
      {list_to_integer(binary_to_list(Id)), {binary_to_list(proplists:get_value(<<"host">>, Data)), proplists:get_value(<<"port">>, Data)}}
    end,
    Brokers = orddict:from_list([ParseBroker(Id, ezk:get(Conn, "/brokers/ids/" ++ Id)) || Id <- RawListBrokers]),

    ezk:end_connection(Conn, ""),
    Brokers.

get_path_for_broker_ids() ->
    case application:get_env(erlkafka, kafka_prefix) of
        undefined -> "/brokers/ids";
        {ok, KafkaPrefix} -> KafkaPrefix++"/brokers/ids"
    end.

get_path_for_broker_topics() ->
    case application:get_env(erlkafka, kafka_prefix) of
         undefined -> "/brokers/topics";
         {ok, KafkaPrefix} -> KafkaPrefix++"/brokers/topics"
    end.

produce_message(X, Magic, Compression) ->
    MessageLength = 1+1+4+byte_size(X),
    CheckSum = erlang:crc32(X),
    <<MessageLength:32/integer,
      Magic:8/integer,
      Compression:8/integer,
      CheckSum:32/integer,
      X/binary>>.


producer_request(ClientId, Ack, Timeout, TopicsAndPartitionsAndPayloads) ->
    ApiKey         = 0,
    ApiVersion     = 0,
    CorrelationId  = 0,
    ClientIdEnc    = string_primitive(ClientId),
    Payload        = producer_request(Ack, Timeout, TopicsAndPartitionsAndPayloads),
    RequestPayload = << ApiKey:16/integer, ApiVersion:16/integer, CorrelationId:32/integer, ClientIdEnc/binary, Payload/binary >>,
    RequestLength  = byte_size(RequestPayload),
    << RequestLength:32/integer, RequestPayload/binary >>.

% Payloads = [{Topic, [{Partition, Payloads}]
producer_request(Ack, Timeout, TopicsAndPartitionsAndPayloads) ->
    TopicProducerRequests =
    [topic_producer_request(Topic, PartitionsAndPayloads) || {Topic, PartitionsAndPayloads} <- TopicsAndPartitionsAndPayloads],
    Payload = array_primitive(TopicProducerRequests),
    << Ack:16/integer, Timeout:32/integer, Payload/binary >>.

topic_producer_request(Topic, PartitionsAndPayloads) ->
    PartitionProducerRequests =
    [partition_producer_request(Partition, Payload) || {Partition, Payload} <- PartitionsAndPayloads],
    Payload = array_primitive(PartitionProducerRequests),
    TopicEnc = string_primitive(Topic),
    << TopicEnc/binary, Payload/binary >>.

partition_producer_request(Partition, Payloads) ->
    MessageSet = message_set(Payloads),
    Size       = byte_size(MessageSet),
    << Partition:32/integer, Size:32/integer, MessageSet/binary >>.

message_set([]) ->
    <<>>;
message_set([Payload | PayloadsRest]) ->
    Message          = message(Payload),
    Size             = byte_size(Message),
    Offset           = 0, % set by the server
    MessageSet       = << Offset:64/integer, Size:32/integer, Message/binary >>,
    MessagesSetRest =  message_set(PayloadsRest),
    << MessagesSetRest/binary, MessageSet/binary >>.

message(Data) ->
    Magic       = 0,
    Compression = 0,
    Key         = bytes_primitive(<<>>),
    Payload     = bytes_primitive(Data),
    MsgBody     =
    << Magic:8/integer,
      Compression:8/integer,
      Key/binary,
      Payload/binary >>,
    CheckSum    = erlang:crc32(MsgBody),
    << CheckSum:32/integer, MsgBody/binary >>.

bytes_primitive(<<>>) ->
    << -1:32/signed-integer >>;
bytes_primitive(Data) ->
    Size = byte_size(Data),
    << Size:32/signed-integer, Data/binary >>.

skim_bytes_primitive(<< -1:32/signed-integer, Rest/binary >>) ->
    {<<>>, Rest};
skim_bytes_primitive(<< Length:32/signed-integer, Data:Length/binary, Rest/binary >>) ->
    {Data, Rest}.

string_primitive(<<>>) ->
    << -1:16/signed-integer >>;
string_primitive(Data) ->
    Size = byte_size(Data),
    << Size:16/signed-integer, Data/binary >>.

skim_string_primitive(<< -1:16/signed-integer, Rest/binary >>) ->
    {<<>>, Rest};
skim_string_primitive(<< Length:16/signed-integer, Data:Length/binary, Rest/binary >>) ->
    {Data, Rest}.

array_primitive(Payloads) ->
    Count   = length(Payloads),
    Payload = bin_join(Payloads),
    << Count:32/integer, Payload/binary >>.

skim_array_primitive(SkimFun, << Count:32/integer, Rest/binary >>) ->
    FoldFun = fun(_, {Acc, Bin}) ->
            {Skim, R} = SkimFun(Bin),
            {Acc ++ [Skim], R}
    end,
    lists:foldl(FoldFun, {[], Rest}, lists:seq(1, Count)).

bin_join([]) ->
    <<>>;
bin_join([Next|Rest]) ->
    RestBin = bin_join(Rest),
    << Next/bitstring, RestBin/bitstring >>.


parse_produce_response(<< CorrelationId:32/integer, Payload/binary >>) ->
    ParseTopic   = fun(TopicBin) ->
        {Topic, PartitionErrorOffsetsBin} = skim_string_primitive(TopicBin),
        ParsePartitionErrorOffset =
        fun(<<Partition:32/integer, ErrorCode:16/integer, Offset:64/integer, R/binary >>) ->
            {{Partition, error_name(ErrorCode), Offset}, R}
        end,
        {PEOs, Rest} = skim_array_primitive(ParsePartitionErrorOffset, PartitionErrorOffsetsBin),
        {{Topic, PEOs}, Rest}
    end,

    {Data, Rest} = skim_array_primitive(ParseTopic, Payload),
    {{CorrelationId, Data}, Rest}.

error_name(0)   -> undefined;
error_name(-1)  -> unknown_error;
error_name(1)   -> offset_out_of_range_error;
error_name(2)   -> invalid_message_error;
error_name(3)   -> unknown_topic_or_partition_error;
error_name(4)   -> invalid_message_size_error;
error_name(5)   -> leader_not_available_error;
error_name(6)   -> not_leader_for_partition_error;
error_name(7)   -> request_timeout_error;
error_name(8)   -> broker_not_available_error;
error_name(9)   -> replica_not_available_error;
error_name(10)  -> message_size_too_large_error;
error_name(11)  -> stale_controller_epoch_error;
error_name(12)  -> offset_metadata_too_large_error.


size_multi_fetch_tpos (TPOs) ->
    lists:foldl(fun({Topic, _, _, _},A) ->
                    2 + byte_size(Topic) + 4 + 8 + 4 + A
                end,
                0,
                TPOs).

size_multi_produce_tpms(TopicPartitionMessages) ->
    lists:foldl(fun({Topic, _, Messages},Acc1) ->
                    2+byte_size(Topic) +  4+4 +
                    lists:foldl(fun({_Magic, _Compression, X}, Acc2) ->
                                    4+1+1+4+byte_size(X) + Acc2
                                end,
                                0,
                                Messages)
                    + Acc1
                end,
                0,
                TopicPartitionMessages).

size_of_produce_messages(Messages) ->
    lists:foldl(fun (X, Size) ->
                    Size + 4 + 1 + 1 + 4 + byte_size(X)
                end,
                0,
                Messages).

parse_offsets(<<"">>, Offsets, _) ->
    {lists:reverse(Offsets)};
parse_offsets(_, Offsets, 0) ->
    {lists:reverse(Offsets)};
parse_offsets(<<Offset:8/integer, Rest/binary>>, Offsets, NumOffsets) ->
    parse_offsets(Rest, [Offset|Offsets], NumOffsets - 1).

parse_messages(<<>>, Acc, Size) ->
    {lists:reverse(Acc), Size};
parse_messages(<<L:32/integer, _/binary>> = B, Acc, Size) when byte_size(B) >= L + 4->
    MsgLengthOfPayload = L -1 -1 -4 ,
    <<_:32/integer, _M:8/integer, _C:8/integer, _Check:32/integer,
      Msg:MsgLengthOfPayload/binary,
      Rest/bitstring>> = B,
    parse_messages(Rest, [Msg | Acc], Size + L + 4);
parse_messages(_B, Acc, Size) ->
    {lists:reverse(Acc), Size}.

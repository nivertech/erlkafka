-module(erlkafka_producer).

-behaviour(gen_server).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).


%               [[{{<<"test">>,0},0}]]   [{<<"test">>,[0,1]}]     [{{<<"test">>, 0}, [<<"msg1">>, <<"msg2">>]}]
-record(state, {leaders_by_topic_partitions, partitions_by_topic, buffer, buffer_size, buffer_limit}).

-export([add/2, send/4]).

%% Public API
start_link() ->
    gen_server:start_link(?MODULE, [], []).

add(Topic, Message) ->
    gen_server:call(ballermann:pid(producer_pool), {add, Topic, Message}).


%% Callbacks

init([]) ->
    random:seed(now()),
    BufferLimit = 2000 + random:uniform(100),
    io:format("BufferLimit ~p\n", [BufferLimit]),
    StateNew = get_leader_by_topic_partitions(#state{
            buffer                      = dict:new(),
            buffer_size                 = 0,
            buffer_limit                = BufferLimit}),
    {ok, StateNew}.

handle_call({add, Topic, Message}, _From,
            State = #state{ partitions_by_topic = PartitionsByTopic, buffer = Buffer, buffer_size = BufferSize }) ->

    Partition     = any_partition(Topic, PartitionsByTopic),
    BufferNew     = dict:append({Topic, Partition}, Message, Buffer),
    BufferSizeNew = BufferSize + 1,
    StateNew      = State#state{ buffer = BufferNew, buffer_size = BufferSizeNew },
    StateSend     = maybe_send(StateNew),
    {reply, ok, StateSend};

handle_call(sync, _From, State) ->
    % HACK ALERT!
    StateSend    = maybe_send(State#state{ buffer_size = 1000000 }),
    {reply, ok, StateSend}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

get_leader_by_topic_partitions(State) ->
    {ok, Conn} = ezk:start_connection(),
    {ok, Topics} =  ezk:ls(Conn, "/brokers/topics"),

    Partitions = fun(Topic) -> {ok, Pt} = ezk:ls(Conn, "/brokers/topics/" ++ binary_to_list(Topic) ++ "/partitions"), Pt end,

    PartitionLeader = fun(Topic, Partition) ->
      {ok, {TopicState, _}} = ezk:get(Conn, "/brokers/topics/" ++ binary_to_list(Topic) ++ "/partitions/" ++ binary_to_list(Partition) ++ "/state"),
      {Dict} = jiffy:decode(TopicState),
      orddict:fetch(<<"leader">>, Dict)
    end,

    PartitionLeaders        =
        orddict:from_list([{T, orddict:from_list([ {list_to_integer(binary_to_list(P)), PartitionLeader(T, P)} || P <- Partitions(T)])} || T <- Topics]),
    LeadersByTopicPartition =
        orddict:from_list(lists:flatten([[{{T, P}, L}||{P, L} <- Ps]|| {T, Ps} <- PartitionLeaders])),
    PartitionsByTopic       =
        orddict:from_list(lists:foldl(fun({T, PLs}, Acc) -> [{T, [P||{P,_L}<-PLs]}|Acc] end, [], PartitionLeaders)),
    State#state{ leaders_by_topic_partitions = LeadersByTopicPartition, partitions_by_topic = PartitionsByTopic}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

maybe_send(State = #state{ buffer_size = BufferSize, buffer_limit = BufferLimit }) when BufferSize < BufferLimit ->
    State;
maybe_send(State = #state{ leaders_by_topic_partitions = LeadersByTopicPartition, buffer = Buffer }) ->
    FoldFun = fun({{T, P}, Msgs}, Acc) ->
        Broker = orddict:fetch({T, P}, LeadersByTopicPartition),
        BrokerBucket = case orddict:find(Broker, Acc) of
            {ok, BB} -> BB;
            error    -> []
        end,
        TopicBucket = case orddict:find(T, BrokerBucket) of
            {ok, TB} -> TB;
            error    -> []
        end,
        TopicBucketNew  = orddict:store(P, Msgs, TopicBucket),
        BrokerBucketNew = orddict:store(T, TopicBucketNew, BrokerBucket),
        orddict:store(Broker, BrokerBucketNew, Acc)
    end,
    SendBuffer = lists:foldl(FoldFun, orddict:new(), dict:to_list(Buffer)),
    Ref = make_ref(),
    [spawn(?MODULE, send, [Broker, Data, self(), Ref]) || {Broker, Data} <- SendBuffer],
    Receive = fun(_, Failure) ->
            receive
                {Ref, ok} ->
                    io:format(".", []),
                    Failure;
                {Ref, {error, Error}} ->
                    io:format("Got Error ~p.", [Error]),
                    true
            after
                10000 ->
                    io:format("Timeout.", []),
                    true
            end
    end,
    % we are not detecting what data got lost, just that we have failures
    Failure = lists:foldl(Receive, false, SendBuffer),
    io:format("*", []),
    % [send(Broker, Data) || {Broker, Data} <- SendBuffer],
    StateReset = State#state{ buffer = dict:new(), buffer_size = 0 },
    case Failure of
        false -> StateReset;
        true  -> get_leader_by_topic_partitions(StateReset)
    end.

send(Broker, Data, From, Ref) ->
    [{Server, _}] = erlkafka_server_sup:get_random_broker_instance_from_pool(Broker),
    %io:format("Sending to Broker ~p :~p\n", [Broker, Data]),
    ProduceRequest = erlkafka_protocol:producer_request(<<"iId">>, 1, 3000, Data),
    ReplyFromServer = gen_server:call(Server, {produce, ProduceRequest}),
    Reply = case ReplyFromServer of
        {error, closed} ->
            {error, closed};
        {_CorrelationId, TopicPartionErrors} ->
            PartitionErrorWithoutError = fun
                ({_, undefined, _}) -> true;
                (_)                 -> false
            end,
            CheckTopic = fun({Topic, PartitionErrors}) ->
                case lists:all(PartitionErrorWithoutError, PartitionErrors) of
                    true ->
                        true;
                    false ->
                        io:format("Error for topic ~p: ~p\n", [Topic, PartitionErrors]),
                        {error, {Topic, PartitionErrors}}
                end
            end,
            MaybeErrors = lists:map(CheckTopic, TopicPartionErrors),
            case lists:all(fun(V) -> V =:= true end, MaybeErrors) of
                true  -> ok;
                false -> {error, MaybeErrors}
            end;
        _ ->
            io:format("Unexpected Reply: ~p\n", [ReplyFromServer]),
            {error, {unexpected_message, ReplyFromServer}}
    end,
    From ! {Ref, Reply}.


any_partition(Topic, PartitionsByTopic) ->
    Partitions = orddict:fetch(Topic, PartitionsByTopic),
    one_of(Partitions).

one_of(List) ->
    lists:nth(random:uniform(length(List)), List).

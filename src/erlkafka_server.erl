%%%-------------------------------------------------------------------
%%% File     : erlkafka_server.erl
%%% Author   : Milind Parikh <milindparikh@gmail.com>
%%%-------------------------------------------------------------------

-module(erlkafka_server).
-author('Milind Parikh <milindparikh@gmail.com>').
-behaviour(gen_server).
-include("erlkafka.hrl").

-export([start_link/0, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {socket, maxsize = ?MAX_MSG_SIZE}).

%%%-------------------------------------------------------------------
%%%                         API FUNCTIONS
%%%-------------------------------------------------------------------

start_link() ->
    start_link(['127.0.0.1', 9092]).

start_link([Host, Port]) ->
   gen_server:start_link( ?MODULE, [Host, Port], []).

%%%-------------------------------------------------------------------
%%%                         GEN_SERVER CB FUNCTIONS
%%%-------------------------------------------------------------------

init([Host, Port]) ->
    io:format("Starting server with ~p.\n", [{Host, Port}]),
    {ok, Socket} = gen_tcp:connect(Host, Port,
                                   [binary, {active, false}, {packet, raw}]),
    {ok, #state{socket=Socket}, 0}.

handle_call({produce, Req}, _From, State) ->
    % TimeBefore = now(),
    Reply =
    case gen_tcp:send(State#state.socket, Req) of
        {error, closed} -> {error, closed};
        ok              ->
            % io:format("SendTime: ~p\n", [timer:now_diff(now(), TimeBefore) / 1000.0]),
            % TimeBeforeRec = now(),
            case gen_tcp:recv(State#state.socket, 4) of
                {error, closed} -> {error, closed};
                {ok, << Length:32/integer >>} ->
                    case gen_tcp:recv(State#state.socket, Length) of
                        {error, closed} -> {error, closed};
                        {ok, ReplyBin}  ->
                            %io:format("RecTime: ~p\n", [timer:now_diff(now(), TimeBeforeRec) / 1000.0]),
                            %io:format("ReqTime: ~p\n", [timer:now_diff(now(), TimeBefore) / 1000.0]),
                            erlkafka_protocol:parse_produce_response(ReplyBin)
                    end
            end
        end,
    {reply, Reply, State}.

handle_cast(stop_link, State) ->
    {stop, normal, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

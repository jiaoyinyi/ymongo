%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动cursor进程
%%% 实现参考https://github.com/comtihon/mongodb-erlang
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_cursor).
-author("jiaoyinyi").

-behaviour(gen_server).

-export([
    next/1, next/2
    , next_batch/1, next_batch/2
    , rest/1, rest/2
    , take/2, take/3
    , foldl/4, foldl/5
    , map/3
]).
-export([start_link/6, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-include("mongo.hrl").

-spec next(pid()) -> bson:document() | {error, term()}.
next(Cursor) ->
    next(Cursor, ?MONGO_CURSOR_DEF_TIMEOUT).

-spec next(pid(), timeout()) -> bson:document() | {error, term()}.
next(Cursor, Timeout) ->
    call(Cursor, {next, Timeout}, Timeout).

-spec next_batch(pid()) -> [bson:document()] | {error, term()}.
next_batch(Cursor) ->
    next_batch(Cursor, ?MONGO_CURSOR_DEF_TIMEOUT).

-spec next_batch(pid(), timeout()) -> [bson:document()] | {error, term()}.
next_batch(Cursor, Timeout) ->
    call(Cursor, {next_batch, Timeout}, Timeout).

-spec rest(pid()) -> [bson:document()] | {error, term()}.
rest(Cursor) ->
    rest(Cursor, ?MONGO_CURSOR_DEF_TIMEOUT).

-spec rest(pid(), timeout()) -> [bson:document()] | {error, term()}.
rest(Cursor, Timeout) ->
    call(Cursor, {rest, infinity, Timeout}, Timeout).

-spec take(pid(), non_neg_integer()) -> [bson:document()] | {error, term()}.
take(Cursor, Limit) ->
    take(Cursor, Limit, ?MONGO_CURSOR_DEF_TIMEOUT).

-spec take(pid(), non_neg_integer(), timeout()) -> [bson:document()] | {error, term()}.
take(Cursor, Limit, Timeout) ->
    call(Cursor, {rest, Limit, Timeout}, Timeout).

-spec foldl(pid(), fun((bson:document(), term()) -> term()), term(), non_neg_integer() | infinity) -> term().
foldl(Cursor, Fun, Acc, Max) ->
    foldl(Cursor, Fun, Acc, Max, ?MONGO_CURSOR_DEF_TIMEOUT).

-spec foldl(pid(), fun((bson:document(), term()) -> term()), term(), non_neg_integer() | infinity, timeout()) -> term().
foldl(_Cursor, _Fun, Acc, Max, _Timeout) when Max =< 0 ->
    Acc;
foldl(Cursor, Fun, Acc, Max, Timeout) ->
    case next(Cursor, Timeout) of
        {error, _Reason} ->
            Acc;
        Doc ->
            NewMax =
                case Max of
                    infinity -> infinity;
                    _ -> Max - 1
                end,
            foldl(Cursor, Fun, Fun(Doc, Acc), NewMax, Timeout)
    end.

-spec map(fun((bson:document()) -> term()), pid(), non_neg_integer()) -> [term()].
map(Fun, Cursor, Max) ->
    lists:reverse(foldl(fun(Doc, Acc) -> [Fun(Doc) | Acc] end, [], Cursor, Max)).

-spec stop(pid()) -> ok | {error, term()}.
stop(Cursor) ->
    call(Cursor, stop, ?MONGO_CURSOR_DEF_TIMEOUT).

%% 同步调用
call(Cursor, Request, Timeout) ->
    try
        gen_server:call(Cursor, Request, Timeout)
    catch
        _:Reason ->
            case Reason of
                {noproc, _} -> {error, empty};
                {timeout, _} -> {error, timeout};
                {normal, _} -> {error, exit};
                _ -> {error, Reason}
            end
    end.

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Conn, Db, Coll, CursorId, BatchSize, Batch) ->
    gen_server:start_link(?MODULE, [self(), Conn, Db, Coll, CursorId, BatchSize, Batch], []).

init([Owner, Conn, Db, Coll, CursorId, BatchSize, Batch]) ->
    erlang:monitor(process, Owner),
    Cursor = #mongo_cursor{
        conn = Conn
        , db = Db
        , coll = Coll
        , cursor_id = CursorId
        , batch_size = BatchSize
        , batch = Batch
    },
    {ok, Cursor}.

handle_call({next, Timeout}, _From, State) ->
    case next_i(State, Timeout) of
        {Reply, #mongo_cursor{cursor_id = 0, batch = []} = UpdatedState} ->
            {stop, normal, Reply, UpdatedState};
        {Reply, UpdatedState} ->
            {reply, Reply, UpdatedState}
    end;
handle_call({rest, Limit, Timeout}, _From, State) ->
    case rest_i(State, Limit, Timeout) of
        {Reply, #mongo_cursor{cursor_id = 0} = UpdatedState} ->
            {stop, normal, Reply, UpdatedState};
        {Reply, UpdatedState} ->
            {reply, Reply, UpdatedState}
    end;
handle_call({next_batch, Timeout}, _From, State = #mongo_cursor{batch_size = Limit}) ->
    case rest_i(State, Limit, Timeout) of
        {Reply, #mongo_cursor{cursor_id = 0} = UpdatedState} ->
            {stop, normal, Reply, UpdatedState};
        {Reply, UpdatedState} ->
            {reply, Reply, UpdatedState}
    end;
%% 同步关闭
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, not_achieve}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', _Ref, process, _Process, _Reason}, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #mongo_cursor{conn = Conn, db = Db, coll = Coll, cursor_id = CursorId}) ->
    catch CursorId =/= 0 andalso mongo_man:kill_cursor(Conn, Db, Coll, CursorId),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

next_i(#mongo_cursor{batch = [Doc | Rest]} = State, _Timeout) ->
    {Doc, State#mongo_cursor{batch = Rest}};
next_i(#mongo_cursor{batch = [], cursor_id = 0} = State, _Timeout) ->
    {{error, empty}, State};
next_i(#mongo_cursor{batch = [], conn = Conn, db = Db, coll = Coll, cursor_id = CursorId, batch_size = BatchSize} = State, Timeout) ->
    case mongo_man:get_more(Conn, Db, Coll, CursorId, BatchSize, Timeout) of
        {error, Reason} ->
            {{error, Reason}, State#mongo_cursor{cursor_id = 0}};
        {NewCursorId, NewBatch} ->
            next_i(State#mongo_cursor{cursor_id = NewCursorId, batch = NewBatch}, Timeout)
    end.

%% @private
rest_i(State, infinity, Timeout) ->
    rest_i(State, -1, Timeout);
rest_i(State, Limit, Timeout) when is_integer(Limit) ->
    {Docs, UpdatedState} = rest_i(State, [], Limit, Timeout),
    {lists:reverse(Docs), UpdatedState}.

%% @private
rest_i(State, Acc, 0, _Timeout) ->
    {Acc, State};
rest_i(State, Acc, Limit, Timeout) ->
    case next_i(State, Timeout) of
        {{error, _Reason}, UpdatedState} ->
            {Acc, UpdatedState};
        {Doc, UpdatedState} ->
            rest_i(UpdatedState, [Doc | Acc], Limit - 1, Timeout)
    end.
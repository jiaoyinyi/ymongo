%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动指令
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_man).
-author("jiaoyinyi").

%% API
-export([
    connect/1
    , close/1
    , count/5
    , find_one/6
    , find_many/6
    , get_more/6
    , kill_cursor/4
    , insert_one/5
    , insert_many/5
    , update_one/7
    , update_many/7
    , delete_one/6
    , delete_many/6
    , create_index/4
    , drop_index/4
    , command/3
    , database_command/4
]).
-export([
    pack_command/1
    , pack_reply/1
]).

-include("mongo.hrl").

-define(MONGO_MERGE_CMD(Cmd, Opts), case maps:size(Opts) =:= 0 of true -> Cmd; _ -> Cmd ++ maps:to_list(Opts) end).
-define(MONGO_MERGE_OPT(Opts, AddOpts), case maps:size(AddOpts) =:= 0 of true -> Opts; _ -> maps:merge(AddOpts, Opts) end).

%% @doc 建议所有指令在该模块实现

%% @doc 启动连接器
%% Opts :: #{
%% host => inet:socket_address() | inet:hostname()
%% port => inet:port_number()
%% ssl => boolean() %% use ssl connect or not
%% ssl_opts => [ssl:tls_client_option()]
%% database => binary()
%% login => binary() %% login name
%% password => binary() %% login password
%% auth_source => binary() %% auth from which database
%% }
-spec connect(map()) -> {ok, pid()} | {error, term()}.
connect(Opts) ->
    mongo_conn:start_link(Opts).

%% @doc 关闭连接器
-spec close(map()) -> ok | {error, term()}.
close(ConnPid) ->
    mongo_conn:stop(ConnPid).

%% @doc 获取数量
-spec count(pid(), collection(), query(), map(), timeout()) -> {ok, integer()} | {error, term()}.
count(Conn, Coll, Query, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"count">>, Coll}, {<<"query">>, Query}], Opts),
    case command(Conn, Cmd, Timeout) of
        {ok, #{<<"n">> := Count}} ->
            {ok, Count};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc 查找单个数据
-spec find_one(pid(), collection(), filter(), projector(), map(), timeout()) -> {ok, document()} | {error, term()}.
find_one(Conn, Coll, Filter, Projector, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"find">>, Coll}, {<<"filter">>, Filter}, {<<"projection">>, Projector}, {<<"limit">>, 1}], Opts),
    case command(Conn, Cmd, Timeout) of
        {ok, #{<<"cursor">> := #{<<"firstBatch">> := []}}} ->
            {error, not_found};
        {ok, #{<<"cursor">> := #{<<"firstBatch">> := [Doc]}}} ->
            {ok, Doc};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc 查找
-spec find_many(pid(), collection(), filter(), projector(), map(), timeout()) -> {ok, pid()} | {error, term()}.
find_many(Conn, Coll, Filter, Projector, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"find">>, Coll}, {<<"filter">>, Filter}, {<<"projection">>, Projector}], Opts),
    case command(Conn, Cmd, Timeout) of
        {ok, #{<<"cursor">> := #{<<"firstBatch">> := []}}} ->
            {error, not_found};
        {ok, #{<<"cursor">> := #{<<"id">> := CursorId, <<"firstBatch">> := Batch, <<"ns">> := NS}}} ->
            [Db, _] = binary:split(NS, <<".">>),
            BatchSize = maps:get(<<"batchSize">>, Opts, 50),
            mongo_cursor:start_link(Conn, Db, Coll, CursorId, BatchSize, Batch);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc 获取更多查找数据（正常只提供给cursor使用）
-spec get_more(pid(), database(), collection(), integer(), integer(), timeout()) -> {integer(), documents()} | {error, term()}.
get_more(Conn, Db, Coll, CursorId, BatchSize, Timeout) ->
    Cmd = [{<<"getMore">>, CursorId}, {<<"collection">>, Coll}, {<<"batchSize">>, BatchSize}],
    case database_command(Conn, Db, Cmd, Timeout) of
        {ok, #{<<"cursor">> := #{<<"id">> := NewCursorId, <<"nextBatch">> := Batch}}} ->
            {NewCursorId, Batch};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc 关闭cursor
-spec kill_cursor(pid(), database(), collection(), integer()) -> {ok, document()} | {error, term()}.
kill_cursor(Conn, Db, Coll, CursorId) ->
    Cmd = [{<<"killCursors">>, Coll}, {<<"cursors">>, [CursorId]}],
    database_command(Conn, Db, Cmd, ?MONGO_DEF_TIMEOUT).

%% @doc 插入单个数据
-spec insert_one(pid(), collection(), document(), map(), timeout()) -> {ok, document()} | {error, term()}.
insert_one(Conn, Coll, Doc, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"insert">>, Coll}, {<<"documents">>, [Doc]}], Opts),
    command(Conn, Cmd, Timeout).

%% @doc 插入多个数据
-spec insert_many(pid(), collection(), documents(), map(), timeout()) -> {ok, document()} | {error, term()}.
insert_many(Conn, Coll, Docs, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"insert">>, Coll}, {<<"documents">>, Docs}], Opts),
    command(Conn, Cmd, Timeout).

%% @doc 更新单个数据
-spec update_one(pid(), collection(), filter(), update(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
update_one(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"update">>, Coll}, {<<"updates">>, [?MONGO_MERGE_OPT(#{<<"q">> => Filter, <<"u">> => Update, <<"multi">> => false}, UpdateOpts)]}], Opts),
    command(Conn, Cmd, Timeout).

%% @doc 更新多个数据
-spec update_many(pid(), collection(), filter(), update(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
update_many(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"update">>, Coll}, {<<"updates">>, [?MONGO_MERGE_OPT(#{<<"q">> => Filter, <<"u">> => Update, <<"multi">> => true}, UpdateOpts)]}], Opts),
    command(Conn, Cmd, Timeout).

%% @doc 删除单个数据
-spec delete_one(pid(), collection(), filter(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
delete_one(Conn, Coll, Filter, DeleteOpts, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"delete">>, Coll}, {<<"deletes">>, [?MONGO_MERGE_OPT(#{<<"q">> => Filter, <<"limit">> => 1}, DeleteOpts)]}], Opts),
    command(Conn, Cmd, Timeout).

%% @doc 删除多个数据
-spec delete_many(pid(), collection(), filter(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
delete_many(Conn, Coll, Filter, DeleteOpts, Opts, Timeout) ->
    Cmd = ?MONGO_MERGE_CMD([{<<"delete">>, Coll}, {<<"deletes">>, [?MONGO_MERGE_OPT(#{<<"q">> => Filter, <<"limit">> => 0}, DeleteOpts)]}], Opts),
    command(Conn, Cmd, Timeout).

%% @doc 创建索引
-spec create_index(pid(), collection(), indexes(), timeout()) -> {ok, document()} | {error, term()}.
create_index(Conn, Coll, Indexes, Timeout) ->
    Cmd = [{<<"createIndexes">>, Coll}, {<<"indexes">>, mongo_util:ensure_list(Indexes)}],
    command(Conn, Cmd, Timeout).

%% @doc 移除索引
-spec drop_index(pid(), collection(), index_name(), timeout()) -> {ok, document()} | {error, term()}.
drop_index(Conn, Coll, Index, Timeout) ->
    Cmd = [{<<"dropIndexes">>, Coll}, {<<"index">>, Index}],
    command(Conn, Cmd, Timeout).

%% @doc 其他指令
-spec command(pid(), proplists:proplist(), timeout()) -> {ok, document()} | {error, term()}.
command(Conn, Command, Timeout) ->
    database_command(Conn, undefined, Command, Timeout).

%% @doc 其他指定数据库指令
-spec database_command(pid(), database(), proplists:proplist(), timeout()) -> {ok, document()} | {error, term()}.
database_command(Conn, Db, Command, Timeout) ->
    Req = #mongo_command{db = Db, cmd = Command},
    mongo_conn:request_api(Conn, Req, Timeout).

%% @doc 打包指令
-spec pack_command(#mongo_command{}) -> bson:document().
pack_command(#mongo_command{db = Db, cmd = Command}) ->
    Command ++ [{<<"$db">>, Db}].

%% @doc 打包回复信息
-spec pack_reply(map()) -> any().
pack_reply(Res) ->
    case maps:get(<<"ok">>, Res) == 1 of
        true ->
            {ok, maps:remove(<<"ok">>, Res)};
        _ ->
            pack_reply_err(Res)
    end.

pack_reply_err(Res) ->
    case Res of
        #{<<"code">> := Code, <<"errmsg">> := ErrMsg} ->
            {error, {fail, {Code, ErrMsg}}};
        _ ->
            {error, {fail, Res}}
    end.


%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动API
%%% @end
%%% Created : 26. 6月 2024 00:00
%%%-------------------------------------------------------------------
-module(mongo_api).
-author("jiaoyinyi").

%% API
-export([
    connect/1
    , close/1
    , count/3, count/4, count/5
    , find_one/3, find_one/4, find_one/5, find_one/6
    , find_many/3, find_many/4, find_many/5, find_many/6
    , insert_one/3, insert_one/4, insert_one/5
    , insert_many/3, insert_many/4, insert_many/5
    , update_one/4, update_one/5, update_one/6, update_one/7
    , update_many/4, update_many/5, update_many/6, update_many/7
    , delete_one/3, delete_one/4, delete_one/5, delete_one/6
    , delete_many/3, delete_many/4, delete_many/5, delete_many/6
    , create_index/3, create_index/4
    , drop_index/3, drop_index/4
    , command/2, command/3
    , database_command/3, database_command/4
]).

-include("mongo.hrl").

%% @doc 启动连接器
-spec connect(map()) -> {ok, pid()} | {error, term()}.
connect(Opts) ->
    mongo_man:connect(Opts).

%% @doc 关闭连接器
-spec close(map()) -> ok | {error, term()}.
close(ConnPid) ->
    mongo_man:close(ConnPid).

%% @doc 获取数量
-spec count(pid(), collection(), query()) -> {ok, integer()} | {error, term()}.
count(Conn, Coll, Query) ->
    count(Conn, Coll, Query, #{}, ?MONGO_DEF_TIMEOUT).
-spec count(pid(), collection(), query(), map() | timeout()) -> {ok, integer()} | {error, term()}.
count(Conn, Coll, Query, Opts) when is_map(Opts) ->
    count(Conn, Coll, Query, Opts, ?MONGO_DEF_TIMEOUT);
count(Conn, Coll, Query, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    count(Conn, Coll, Query, #{}, Timeout).
-spec count(pid(), collection(), query(), map(), timeout()) -> {ok, integer()} | {error, term()}.
count(Conn, Coll, Query, Opts, Timeout) ->
    mongo_man:count(Conn, Coll, Query, Opts, Timeout).

%% @doc 查找单个数据
-spec find_one(pid(), collection(), filter()) -> {ok, document()} | {error, term()}.
find_one(Conn, Coll, Filter) ->
    find_one(Conn, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_one(pid(), collection(), filter(), projector()) -> {ok, document()} | {error, term()}.
find_one(Conn, Coll, Filter, Projector) ->
    find_one(Conn, Coll, Filter, Projector, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_one(pid(), collection(), filter(), projector(), map() | timeout()) -> {ok, document()} | {error, term()}.
find_one(Conn, Coll, Filter, Projector, Opts) when is_map(Opts) ->
    find_one(Conn, Coll, Filter, Projector, Opts, ?MONGO_DEF_TIMEOUT);
find_one(Conn, Coll, Filter, Projector, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    find_one(Conn, Coll, Filter, Projector, #{}, Timeout).
-spec find_one(pid(), collection(), filter(), projector(), map(), timeout()) -> {ok, document()} | {error, term()}.
find_one(Conn, Coll, Filter, Projector, Opts, Timeout) ->
    mongo_man:find_one(Conn, Coll, Filter, Projector, Opts, Timeout).

%% @doc 查找
-spec find_many(pid(), collection(), filter()) -> {ok, pid()} | {error, term()}.
find_many(Conn, Coll, Filter) ->
    find_many(Conn, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_many(pid(), collection(), filter(), projector()) -> {ok, pid()} | {error, term()}.
find_many(Conn, Coll, Filter, Projector) ->
    find_many(Conn, Coll, Filter, Projector, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_many(pid(), collection(), filter(), projector(), map() | timeout()) -> {ok, pid()} | {error, term()}.
find_many(Conn, Coll, Filter, Projector, Opts) when is_map(Opts) ->
    find_many(Conn, Coll, Filter, Projector, Opts, ?MONGO_DEF_TIMEOUT);
find_many(Conn, Coll, Filter, Projector, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    find_many(Conn, Coll, Filter, Projector, #{}, Timeout).
-spec find_many(pid(), collection(), filter(), projector(), map(), timeout()) -> {ok, pid()} | {error, term()}.
find_many(Conn, Coll, Filter, Projector, Opts, Timeout) ->
    mongo_man:find_many(Conn, Coll, Filter, Projector, Opts, Timeout).

%% @doc 插入单个数据
-spec insert_one(pid(), collection(), document()) -> {ok, document()} | {error, term()}.
insert_one(Conn, Coll, Doc) ->
    insert_one(Conn, Coll, Doc, #{}, ?MONGO_DEF_TIMEOUT).
-spec insert_one(pid(), collection(), document(), map() | timeout()) -> {ok, document()} | {error, term()}.
insert_one(Conn, Coll, Doc, Opts) when is_map(Opts) ->
    insert_one(Conn, Coll, Doc, Opts, ?MONGO_DEF_TIMEOUT);
insert_one(Conn, Coll, Doc, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    insert_one(Conn, Coll, Doc, #{}, Timeout).
-spec insert_one(pid(), collection(), document(), map(), timeout()) -> {ok, document()} | {error, term()}.
insert_one(Conn, Coll, Doc, Opts, Timeout) ->
    mongo_man:insert_one(Conn, Coll, Doc, Opts, Timeout).

%% @doc 插入多个数据
-spec insert_many(pid(), collection(), documents()) -> {ok, document()} | {error, term()}.
insert_many(Conn, Coll, Docs) ->
    insert_many(Conn, Coll, Docs, #{}, ?MONGO_DEF_TIMEOUT).
-spec insert_many(pid(), collection(), documents(), map() | timeout()) -> {ok, document()} | {error, term()}.
insert_many(Conn, Coll, Docs, Opts) when is_map(Opts) ->
    insert_many(Conn, Coll, Docs, Opts, ?MONGO_DEF_TIMEOUT);
insert_many(Conn, Coll, Docs, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    insert_many(Conn, Coll, Docs, #{}, Timeout).
-spec insert_many(pid(), collection(), documents(), map(), timeout()) -> {ok, document()} | {error, term()}.
insert_many(Conn, Coll, Docs, Opts, Timeout) ->
    mongo_man:insert_many(Conn, Coll, Docs, Opts, Timeout).

%% @doc 更新单个数据
-spec update_one(pid(), collection(), filter(), update()) -> {ok, document()} | {error, term()}.
update_one(Conn, Coll, Filter, Update) ->
    update_one(Conn, Coll, Filter, Update, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec update_one(pid(), collection(), filter(), update(), timeout()) -> {ok, document()} | {error, term()}.
update_one(Conn, Coll, Filter, Update, Timeout) ->
    update_one(Conn, Coll, Filter, Update, #{}, #{}, Timeout).
-spec update_one(pid(), collection(), filter(), update(), map(), map()) -> {ok, document()} | {error, term()}.
update_one(Conn, Coll, Filter, Update, UpdateOpts, Opts) ->
    update_one(Conn, Coll, Filter, Update, UpdateOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec update_one(pid(), collection(), filter(), update(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
update_one(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout) ->
    mongo_man:update_one(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout).

%% @doc 更新多个数据
-spec update_many(pid(), collection(), filter(), update()) -> {ok, document()} | {error, term()}.
update_many(Conn, Coll, Filter, Update) ->
    update_many(Conn, Coll, Filter, Update, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec update_many(pid(), collection(), filter(), update(), timeout()) -> {ok, document()} | {error, term()}.
update_many(Conn, Coll, Filter, Update, Timeout) ->
    update_many(Conn, Coll, Filter, Update, #{}, #{}, Timeout).
-spec update_many(pid(), collection(), filter(), update(), map(), map()) -> {ok, document()} | {error, term()}.
update_many(Conn, Coll, Filter, Update, UpdateOpts, Opts) ->
    update_many(Conn, Coll, Filter, Update, UpdateOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec update_many(pid(), collection(), filter(), update(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
update_many(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout) ->
    mongo_man:update_many(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout).

%% @doc 删除单个数据
-spec delete_one(pid(), collection(), filter()) -> {ok, document()} | {error, term()}.
delete_one(Conn, Coll, Filter) ->
    delete_one(Conn, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec delete_one(pid(), collection(), filter(), timeout()) -> {ok, document()} | {error, term()}.
delete_one(Conn, Coll, Filter, Timeout) ->
    delete_one(Conn, Coll, Filter, #{}, #{}, Timeout).
-spec delete_one(pid(), collection(), filter(), map(), map()) -> {ok, document()} | {error, term()}.
delete_one(Conn, Coll, Filter, DeleteOpts, Opts) ->
    delete_one(Conn, Coll, Filter, DeleteOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec delete_one(pid(), collection(), filter(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
delete_one(Conn, Coll, Filter, DeleteOpts, Opts, Timeout) ->
    mongo_man:delete_one(Conn, Coll, Filter, DeleteOpts, Opts, Timeout).

%% @doc 删除多个数据
-spec delete_many(pid(), collection(), filter()) -> {ok, document()} | {error, term()}.
delete_many(Conn, Coll, Filter) ->
    delete_many(Conn, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec delete_many(pid(), collection(), filter(), timeout()) -> {ok, document()} | {error, term()}.
delete_many(Conn, Coll, Filter, Timeout) ->
    delete_many(Conn, Coll, Filter, #{}, #{}, Timeout).
-spec delete_many(pid(), collection(), filter(), map(), map()) -> {ok, document()} | {error, term()}.
delete_many(Conn, Coll, Filter, DeleteOpts, Opts) ->
    delete_many(Conn, Coll, Filter, DeleteOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec delete_many(pid(), collection(), filter(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
delete_many(Conn, Coll, Filter, DeleteOpts, Opts, Timeout) ->
    mongo_man:delete_many(Conn, Coll, Filter, DeleteOpts, Opts, Timeout).

%% @doc 创建索引
-spec create_index(pid(), collection(), indexes()) -> {ok, document()} | {error, term()}.
create_index(Conn, Coll, Indexes) ->
    create_index(Conn, Coll, Indexes, ?MONGO_DEF_TIMEOUT).
-spec create_index(pid(), collection(), indexes(), timeout()) -> {ok, document()} | {error, term()}.
create_index(Conn, Coll, Indexes, Timeout) ->
    mongo_man:create_index(Conn, Coll, Indexes, Timeout).

%% @doc 移除索引
-spec drop_index(pid(), collection(), index_name()) -> {ok, document()} | {error, term()}.
drop_index(Conn, Coll, Index) ->
    drop_index(Conn, Coll, Index, ?MONGO_DEF_TIMEOUT).
-spec drop_index(pid(), collection(), index_name(), timeout()) -> {ok, document()} | {error, term()}.
drop_index(Conn, Coll, Index, Timeout) ->
    mongo_man:drop_index(Conn, Coll, Index, Timeout).

%% @doc 其他指令
-spec command(pid(), proplists:proplist()) -> {ok, document()} | {error, term()}.
command(Conn, Command) ->
    command(Conn, Command, ?MONGO_DEF_TIMEOUT).
-spec command(pid(), proplists:proplist(), timeout()) -> {ok, document()} | {error, term()}.
command(Conn, Command, Timeout) ->
    mongo_man:command(Conn, Command, Timeout).

%% @doc 其他数据库指令
-spec database_command(pid(), database(), proplists:proplist()) -> {ok, document()} | {error, term()}.
database_command(Conn, Db, Command) ->
    database_command(Conn, Db, Command, ?MONGO_DEF_TIMEOUT).
-spec database_command(pid(), database(), proplists:proplist(), timeout()) -> {ok, document()} | {error, term()}.
database_command(Conn, Db, Command, Timeout) ->
    mongo_man:database_command(Conn, Db, Command, Timeout).
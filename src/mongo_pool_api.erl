%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动池化API
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_pool_api).
-author("jiaoyinyi").

%% API
-export([
    start_pool/3, stop_pool/1
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

%% @doc 新增连接池
-spec start_pool(atom(), map(), map()) -> {ok, pid()} | {error, term()}.
start_pool(PoolName, PoolArgs, DbArgs) ->
    NewPoolArgs = maps:merge(PoolArgs, #{name => {local, PoolName}, worker_module => mongo_conn}),
    poolboy:start_link(maps:to_list(NewPoolArgs), DbArgs).

%% @doc 关闭连接池
-spec stop_pool(atom()) -> ok.
stop_pool(PoolName) ->
    poolboy:stop(PoolName).

%% @doc 获取数量
-spec count(pid(), collection(), query()) -> {ok, integer()} | {error, term()}.
count(Conn, Coll, Query) ->
    count(Conn, Coll, Query, #{}, ?MONGO_DEF_TIMEOUT).
-spec count(pid(), collection(), query(), map() | timeout()) -> {ok, integer()} | {error, term()}.
count(Conn, Coll, Query, Opts) when is_map(Opts) ->
    count(Conn, Coll, Query, Opts, ?MONGO_DEF_TIMEOUT);
count(Conn, Coll, Query, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    count(Conn, Coll, Query, #{}, Timeout).
-spec count(pid(), collection(), query(), map(),  timeout()) -> {ok, integer()} | {error, term()}.
count(Pool, Coll, Query, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:count(Conn, Coll, Query, Opts, Timeout)
        end, Timeout
    ).

%% @doc 查找单个数据
-spec find_one(pid(), collection(), filter()) -> {ok, document()} | {error, term()}.
find_one(Pool, Coll, Filter) ->
    find_one(Pool, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_one(pid(), collection(), filter(), projector()) -> {ok, document()} | {error, term()}.
find_one(Pool, Coll, Filter, Projector) ->
    find_one(Pool, Coll, Filter, Projector, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_one(pid(), collection(), filter(), projector(), map() | timeout()) -> {ok, document()} | {error, term()}.
find_one(Pool, Coll, Filter, Projector, Opts) when is_map(Opts) ->
    find_one(Pool, Coll, Filter, Projector, Opts, ?MONGO_DEF_TIMEOUT);
find_one(Pool, Coll, Filter, Projector, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    find_one(Pool, Coll, Filter, Projector, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_one(pid(), collection(), filter(), projector(), map(), timeout()) -> {ok, document()} | {error, term()}.
find_one(Pool, Coll, Filter, Projector, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:find_one(Conn, Coll, Filter, Projector, Opts, Timeout)
        end, Timeout
    ).

%% @doc 查找
-spec find_many(pid(), collection(), filter()) -> {ok, pid()} | {error, term()}.
find_many(Pool, Coll, Filter) ->
    find_many(Pool, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_many(pid(), collection(), filter(), projector()) -> {ok, pid()} | {error, term()}.
find_many(Pool, Coll, Filter, Projector) ->
    find_many(Pool, Coll, Filter, Projector, #{}, ?MONGO_DEF_TIMEOUT).
-spec find_many(pid(), collection(), filter(), projector(), map() | timeout()) -> {ok, pid()} | {error, term()}.
find_many(Pool, Coll, Filter, Projector, Opts) when is_map(Opts) ->
    find_many(Pool, Coll, Filter, Projector, Opts, ?MONGO_DEF_TIMEOUT);
find_many(Pool, Coll, Filter, Projector, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    find_many(Pool, Coll, Filter, Projector, #{}, Timeout).
-spec find_many(pid(), collection(), filter(), projector(), map(), timeout()) -> {ok, pid()} | {error, term()}.
find_many(Pool, Coll, Filter, Projector, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:find_many(Conn, Coll, Filter, Projector, Opts, Timeout)
        end, Timeout
    ).

%% @doc 插入单个数据
-spec insert_one(pid(), collection(), document()) -> {ok, document()} | {error, term()}.
insert_one(Pool, Coll, Doc) ->
    insert_one(Pool, Coll, Doc, #{}, ?MONGO_DEF_TIMEOUT).
-spec insert_one(pid(), collection(), document(), map() | timeout()) -> {ok, document()} | {error, term()}.
insert_one(Pool, Coll, Doc, Opts) when is_map(Opts) ->
    insert_one(Pool, Coll, Doc, Opts, ?MONGO_DEF_TIMEOUT);
insert_one(Pool, Coll, Doc, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    insert_one(Pool, Coll, Doc, #{}, Timeout).
-spec insert_one(pid(), collection(), document(), map(), timeout()) -> {ok, document()} | {error, term()}.
insert_one(Pool, Coll, Doc, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:insert_one(Conn, Coll, Doc, Opts, Timeout)
        end, Timeout
    ).

%% @doc 插入多个数据
-spec insert_many(pid(), collection(), documents()) -> {ok, document()} | {error, term()}.
insert_many(Pool, Coll, Docs) ->
    insert_many(Pool, Coll, Docs, #{}, ?MONGO_DEF_TIMEOUT).
-spec insert_many(pid(), collection(), documents(), map() | timeout()) -> {ok, document()} | {error, term()}.
insert_many(Pool, Coll, Docs, Opts) when is_map(Opts) ->
    insert_many(Pool, Coll, Docs, Opts, ?MONGO_DEF_TIMEOUT);
insert_many(Pool, Coll, Docs, Timeout) when ?MONGO_IS_TIMEOUT(Timeout) ->
    insert_many(Pool, Coll, Docs, #{}, Timeout).
-spec insert_many(pid(), collection(), documents(), map(), timeout()) -> {ok, document()} | {error, term()}.
insert_many(Pool, Coll, Docs, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:insert_many(Conn, Coll, Docs, Opts, Timeout)
        end, Timeout
    ).

%% @doc 更新单个数据
-spec update_one(pid(), collection(), filter(), update()) -> {ok, document()} | {error, term()}.
update_one(Pool, Coll, Filter, Update) ->
    update_one(Pool, Coll, Filter, Update, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec update_one(pid(), collection(), filter(), update(), timeout()) -> {ok, document()} | {error, term()}.
update_one(Pool, Coll, Filter, Update, Timeout) ->
    update_one(Pool, Coll, Filter, Update, #{}, #{}, Timeout).
-spec update_one(pid(), collection(), filter(), update(), map(), map()) -> {ok, document()} | {error, term()}.
update_one(Pool, Coll, Filter, Update, UpdateOpts, Opts) ->
    update_one(Pool, Coll, Filter, Update, UpdateOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec update_one(pid(), collection(), filter(), update(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
update_one(Pool, Coll, Filter, Update, UpdateOpts, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:update_one(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout)
        end, Timeout
    ).

%% @doc 更新多个数据
-spec update_many(pid(), collection(), filter(), update()) -> {ok, document()} | {error, term()}.
update_many(Pool, Coll, Filter, Update) ->
    update_many(Pool, Coll, Filter, Update, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec update_many(pid(), collection(), filter(), update(), timeout()) -> {ok, document()} | {error, term()}.
update_many(Pool, Coll, Filter, Update, Timeout) ->
    update_many(Pool, Coll, Filter, Update, #{}, #{}, Timeout).
-spec update_many(pid(), collection(), filter(), update(), map(), map()) -> {ok, document()} | {error, term()}.
update_many(Pool, Coll, Filter, Update, UpdateOpts, Opts) ->
    update_many(Pool, Coll, Filter, Update, UpdateOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec update_many(pid(), collection(), filter(), update(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
update_many(Pool, Coll, Filter, Update, UpdateOpts, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:update_many(Conn, Coll, Filter, Update, UpdateOpts, Opts, Timeout)
        end, Timeout
    ).

%% @doc 删除单个数据
-spec delete_one(pid(), collection(), filter()) -> {ok, document()} | {error, term()}.
delete_one(Pool, Coll, Filter) ->
    delete_one(Pool, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec delete_one(pid(), collection(), filter(), timeout()) -> {ok, document()} | {error, term()}.
delete_one(Pool, Coll, Filter, Timeout) ->
    delete_one(Pool, Coll, Filter, #{}, #{}, Timeout).
-spec delete_one(pid(), collection(), filter(), map(), map()) -> {ok, document()} | {error, term()}.
delete_one(Pool, Coll, Filter, DeleteOpts, Opts) ->
    delete_one(Pool, Coll, Filter, DeleteOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec delete_one(pid(), collection(), filter(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
delete_one(Pool, Coll, Filter, DeleteOpts, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:delete_one(Conn, Coll, Filter, DeleteOpts, Opts, Timeout)
        end, Timeout
    ).

%% @doc 删除多个数据
-spec delete_many(pid(), collection(), filter()) -> {ok, document()} | {error, term()}.
delete_many(Pool, Coll, Filter) ->
    delete_many(Pool, Coll, Filter, #{}, #{}, ?MONGO_DEF_TIMEOUT).
-spec delete_many(pid(), collection(), filter(), timeout()) -> {ok, document()} | {error, term()}.
delete_many(Pool, Coll, Filter, Timeout) ->
    delete_many(Pool, Coll, Filter, #{}, #{}, Timeout).
-spec delete_many(pid(), collection(), filter(), map(), map()) -> {ok, document()} | {error, term()}.
delete_many(Pool, Coll, Filter, DeleteOpts, Opts) ->
    delete_many(Pool, Coll, Filter, DeleteOpts, Opts, ?MONGO_DEF_TIMEOUT).
-spec delete_many(pid(), collection(), filter(), map(), map(), timeout()) -> {ok, document()} | {error, term()}.
delete_many(Pool, Coll, Filter, DeleteOpts, Opts, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:delete_many(Conn, Coll, Filter, DeleteOpts, Opts, Timeout)
        end, Timeout
    ).

%% @doc 创建索引
-spec create_index(pid(), collection(), indexes()) -> {ok, document()} | {error, term()}.
create_index(Pool, Coll, Indexes) ->
    create_index(Pool, Coll, Indexes, ?MONGO_DEF_TIMEOUT).
-spec create_index(pid(), collection(), indexes(), timeout()) -> {ok, document()} | {error, term()}.
create_index(Pool, Coll, Indexes, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:create_index(Conn, Coll, Indexes, Timeout)
        end, Timeout
    ).

%% @doc 移除索引
-spec drop_index(pid(), collection(), index_name()) -> {ok, document()} | {error, term()}.
drop_index(Pool, Coll, Index) ->
    drop_index(Pool, Coll, Index, ?MONGO_DEF_TIMEOUT).
-spec drop_index(pid(), collection(), index_name(), timeout()) -> {ok, document()} | {error, term()}.
drop_index(Pool, Coll, Index, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:drop_index(Conn, Coll, Index, Timeout)
        end, Timeout
    ).

%% @doc 其他指令
-spec command(pid(), proplists:proplist()) -> {ok, document()} | {error, term()}.
command(Pool, Command) ->
    command(Pool, Command, ?MONGO_DEF_TIMEOUT).
-spec command(pid(), proplists:proplist(), timeout()) -> {ok, document()} | {error, term()}.
command(Pool, Command, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:command(Conn, Command, Timeout)
        end, Timeout
    ).

%% @doc 其他指定数据库指令
-spec database_command(pid(), database(), proplists:proplist()) -> {ok, document()} | {error, term()}.
database_command(Pool, Db, Command) ->
    database_command(Pool, Db, Command, ?MONGO_DEF_TIMEOUT).
-spec database_command(pid(), database(), proplists:proplist(), timeout()) -> {ok, document()} | {error, term()}.
database_command(Pool, Db, Command, Timeout) ->
    pool_transaction(Pool,
        fun(Conn) ->
            mongo_api:database_command(Conn, Db, Command, Timeout)
        end, Timeout
    ).

pool_transaction(Pool, Fun, Timeout) ->
    poolboy:transaction(Pool, Fun, Timeout).
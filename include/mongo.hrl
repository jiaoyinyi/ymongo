%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动头文件
%%% @end
%%% Created : 26. 6月 2024 17:21
%%%-------------------------------------------------------------------

-ifndef(MONGO_HRL).
-define(MONGO_HRL, 1).

-type database() :: binary().
-type collection() :: binary().
-type document() :: map().
-type documents() :: [document()].
-type query() :: document().
-type filter() :: document().
-type projector() :: document().
-type update() :: document().
-type indexes() :: documents().
-type index_name() :: binary().

%% 操作码
-define(MONGO_OP_MSG, 2013).           %% 使用标准格式发送消息。用于客户端请求和数据库回复

%% 消息体类型
-define(MONGO_SECTION_KIND_0, 0).      %% 消息体
-define(MONGO_SECTION_KIND_1, 1).      %% 文档序列

%% mongo 指令标签
-define(MONGO_COMMAND_FIND_ONE, find_one).
-define(MONGO_COMMAND_FIND_MANY, find).
-define(MONGO_COMMAND_GET_MORE, get_more).
-define(MONGO_COMMAND_KILL_CURSORS, kill_cursors).
-define(MONGO_COMMAND_COUNT, count).
-define(MONGO_COMMAND_INSERT_ONE, insert_one).
-define(MONGO_COMMAND_INSERT_MANY, insert_many).
-define(MONGO_COMMAND_UPDATE_ONE, update_one).
-define(MONGO_COMMAND_UPDATE_MANY, update_many).
-define(MONGO_COMMAND_DELETE_ONE, delete_one).
-define(MONGO_COMMAND_DELETE_MANY, delete_many).
-define(MONGO_COMMAND_CREATE_INDEX, create_index).
-define(MONGO_COMMAND_DROP_INDEX, drop_index).
-define(MONGO_COMMAND_COMMAND, command).

%% 判断是否为写指令
-define(MONGO_IS_WRITE_COMMAND(Type),
    (Type =:= ?MONGO_COMMAND_INSERT_ONE orelse Type =:= ?MONGO_COMMAND_INSERT_MANY orelse Type =:= ?MONGO_COMMAND_UPDATE_ONE
    orelse Type =:= ?MONGO_COMMAND_UPDATE_MANY orelse Type =:= ?MONGO_COMMAND_DELETE_ONE orelse Type =:= ?MONGO_COMMAND_DELETE_MANY)
).

-define(MONGO_DEF_TIMEOUT, 8000).
-define(MONGO_CURSOR_DEF_TIMEOUT, infinity).
-define(MONGO_IS_TIMEOUT(Timeout), (is_integer(Timeout) andalso Timeout > 0 orelse Timeout =:= infinity)).

-ifdef(MONGO_DEBUG).
-define(MONGO_DFORMAT(Format, Args), io:format("[MD]" ++ Format ++ "~n", Args)).
-define(MONGO_DFORMAT(Format), ?MONGO_DFORMAT("~ts", [Format])).
-else.
-define(MONGO_DFORMAT(Format, Args), ok).
-define(MONGO_DFORMAT(Format), ok).
-endif.

%% @doc 连接进程状态
-record(mongo_conn, {
    socket                         :: gen_tcp:socket() | ssl:sslsocket()
    , net_mod                      :: gen_tcp | ssl
    , reqs                         :: map()
    , db                           :: database()
    , buffer                       :: binary()
    , gc_ref                       :: reference()
}).

%% @doc 请求等待返回状态
-record(mongo_req_state, {
    req_id                         :: integer()
    , from                         :: gen_server:from()
    , type                         :: atom()
}).

%% @doc cursor进程状态
-record(mongo_cursor, {
    conn                            :: pid()
    , db                            :: database()
    , coll                          :: collection()
    , cursor_id                     :: integer()
    , batch_size                    :: integer()
    , batch                         :: documents()
}).

%% mongo api封装
-record(mongo_command, {
    db                              :: database()
    , coll                          :: collection()
    , type                          :: atom()
    , cmd                           :: map() | tuple()
}).

-endif.


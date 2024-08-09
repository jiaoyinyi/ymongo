Erlang的简单mongodb驱动，基于OP_MSG协议实现。仅支持mongodb版本>=2.7

驱动主要分两个接口模块，mongo_api和mongo_pool_api。

### mongo_api
直接使用驱动连接进程的驱动接口

#### 建立连接
```
%% Opts :: #{
%%     host => inet:socket_address() | inet:hostname()
%%     port => inet:port_number()
%%     ssl => boolean()                                    %% use ssl connect or not
%%     ssl_opts => [ssl:tls_client_option()]
%%     database => binary()
%%     login => binary()                                   %% login username
%%     password => binary()                                %% login password
%%     auth_source => binary()                             %% auth from where database
%% }

Opts = #{
    host => "127.0.0.1", port => 27017, database => <<"test_1">>, 
    login => <<"test">>, password => <<"test">>, auth_source => <<"admin">>
},
{ok, Conn} = mongo_api:connect(Opts).
```

#### 断开连接
```
ok = mongo_api:close(Conn).
```

#### 创建索引
```
Indexes = #{<<"key">> => #{<<"rid">> => 1, <<"srv_id">> => 1}, <<"name">> => <<"role_id">>, <<"unique">> => true},
{ok, _} = mongo_api:create_index(Conn, <<"role">>, Indexes).
```

#### 删除索引
```
{ok, _} = mongo_api:drop_index(Conn, <<"role">>, <<"role_id">>).
```

#### 增删改查
```
%% insert
Doc = #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
{ok, #{<<"n">> := 1}} = mongo_api:insert_one(Conn, <<"role">>, Doc).

Docs = [#{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}],
{ok, #{<<"n">> := 2}} = mongo_api:insert_many(Conn, <<"role">>, Docs).

%% find
{ok, #{<<"rid">> := 1, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>}} 
    = mongo_api:find_one(Conn, <<"role">>, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{}).

{ok, Cursor} = mongo_api:find_many(Conn, <<"role">>, #{<<"name">> => <<"Mark">>}, #{}).
[#{<<"rid">> := 1, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>},
 #{<<"rid">> := 2, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>}]
  = mongo_cursor:rest(Cursor).

%% count
{ok, Count} = mongo_api:count(Conn, <<"role">>, #{}).

%% update
{ok, #{<<"n">> := 1}} = mongo_api:update_one(Conn, <<"role">>, #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>}, #{<<"$set">> => #{<<"name">> => <<"Test">>}}).
{ok, #{<<"n">> := 2}} = mongo_api:update_many(Conn, <<"role">>, #{<<"name">> => <<"Mark">>}, #{<<"$set">> => #{<<"name">> => <<"Test">>}}).

%% delete
{ok, #{<<"n">> := 1}} = mongo_api:delete_one(Conn, <<"role">>, #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>}).
{ok, #{<<"n">> := 2}} = mongo_api:delete_many(Conn, <<"role">>, #{}).
```

#### 其他操作
```
{ok, Doc} = mongo_api:command(Conn, Command).
{ok, Doc} = mongo_api:database_command(Conn, Db, Command).
```

### mongo_pool_api
具有简单连接池的驱动接口

#### 创建连接池
```
{ok, PoolPid} = mongo_pool_api:start_pool(PoolName, PoolArgs, DbArgs).
```

#### 移除连接池
```
ok = mongo_pool_api:stop_pool(PoolName).
```

#### 创建索引
```
Indexs = #{<<"key">> => #{<<"rid">> => 1, <<"srv_id">> => 1}, <<"name">> => <<"role_id">>, <<"unique">> => true},
{ok, _} = mongo_pool_api:create_index(PoolName, <<"role">>, Indexes).
```

#### 删除索引
```
{ok, _} = mongo_pool_api:drop_index(PoolName, <<"role">>, <<"role_id">>).
```

#### 增删改查
```
%% insert
Doc = #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
{ok, #{<<"n">> := 1}} = mongo_pool_api:insert_one(PoolName, <<"role">>, Doc).

Docs = [#{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}],
{ok, #{<<"n">> := 2}} = mongo_pool_api:insert_many(PoolName, <<"role">>, Docs).

%% find
{ok, #{<<"rid">> := 1, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>}} 
    = mongo_pool_api:find_one(PoolName, <<"role">>, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{}).

{ok, Cursor} = mongo_pool_api:find_many(PoolName, <<"role">>, #{<<"name">> => <<"Mark">>}, #{}).
[#{<<"rid">> := 1, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>},
 #{<<"rid">> := 2, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>}]
  = mongo_cursor:rest(Cursor).

%% count
{ok, Count} = mongo_pool_api:count(PoolName, <<"role">>, #{}).

%% update
{ok, #{<<"n">> := 1}} = mongo_pool_api:update_one(PoolName, <<"role">>, #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>}, #{<<"name">> => <<"Test">>}).
{ok, #{<<"n">> := 2}} = mongo_pool_api:update_many(PoolName, <<"role">>, #{<<"name">> => <<"Mark">>}, #{<<"name">> => <<"Test">>}).

%% delete
{ok, #{<<"n">> := 1}} = mongo_pool_api:delete_one(PoolName, <<"role">>, #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>}).
{ok, #{<<"n">> := 2}} = mongo_pool_api:delete_many(PoolName, <<"role">>, #{}).
```

#### 其他操作
```
{ok, Doc} = mongo_pool_api:command(PoolName, Command).
{ok, Doc} = mongo_pool_api:database_command(PoolName, Db, Command).
```

### 扩展
详细指令可在mongodb手册查询 https://www.mongodb.com/docs/manual/reference/command/
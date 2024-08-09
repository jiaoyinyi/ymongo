%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_test).
-author("jiaoyinyi").

%% API
-export([
    run/1, run/2
    , proc_loop/1
]).

run(N) ->
    run(N, 1000).
run(N, Times) ->
    Opts = #{
        database => <<"test_1">>,
        login => <<"test">>,
        password => <<"test">>,
        auth_source => <<"admin">>
    },
    {ok, _} = mongo_pool_api:start_pool(?MODULE, #{size => 50}, Opts),
    {ok, _} = mongo_pool_api:create_index(?MODULE, <<"role">>, [#{<<"key">> => #{<<"rid">> => 1, <<"srv_id">> => 1}, <<"name">> => <<"role_id">>, <<"unique">> => true}]),
    Fun =
        fun() ->
            Map = start_procs(N, Times),
            wait_procs(Map)
        end,
    eprof:profile(Fun),
    eprof:stop_profiling(),
    eprof:log("eprof.txt"),
    eprof:analyze(total),
    mongo_pool_api:delete_many(?MODULE, <<"role">>, #{}),
    mongo_pool_api:stop_pool(?MODULE),
    ok.

start_procs(N, Times) ->
    do_start_procs(N, Times, #{}).
do_start_procs(N, _Times, Map) when N =< 0 ->
    Map;
do_start_procs(N, Times, Map) ->
    {Pid, Ref} =
        erlang:spawn_monitor(
            fun() ->
                proc_loop(Times)
            end
        ),
    do_start_procs(N - 1, Times, maps:put(Pid, Ref, Map)).

proc_loop(Times) when Times =< 0 ->
    quit;
proc_loop(Times) ->
    Ms = rand:uniform(10),
    timer:sleep(Ms),
    Idx = rand:uniform(5),
    do_proc_loop(Idx),
    ?MODULE:proc_loop(Times - 1).

do_proc_loop(1) -> %% 插入数据
    Doc = #{
        <<"rid">> => rand:uniform(1000000)
        , <<"srv_id">> => <<"test_1">>
        , <<"lev">> => rand:uniform(100)
        , <<"exp">> => rand:uniform(100000000000)
        , <<"power">> => rand:uniform(100000000000)
    },
    mongo_pool_api:insert_one(?MODULE, <<"role">>, Doc);
do_proc_loop(2) -> %% 查下单条数据
    RoleId = #{<<"rid">> => rand:uniform(1000000), <<"srv_id">> => <<"test_1">>},
    mongo_pool_api:find_one(?MODULE, <<"role">>, RoleId);
do_proc_loop(3) -> %% 范围查询多条数据
    case mongo_pool_api:find_many(?MODULE, <<"role">>, #{<<"lev">> => rand:uniform(1000)}) of
        {ok, Cursor} ->
            mongo_cursor:rest(Cursor);
        Ret ->
            Ret
    end;
do_proc_loop(4) -> %% 更新
    Doc = #{
        <<"rid">> => rand:uniform(1000000)
        , <<"srv_id">> => <<"test_1">>
        , <<"lev">> => rand:uniform(100)
        , <<"exp">> => rand:uniform(100000000000)
        , <<"power">> => rand:uniform(100000000000)
    },
    mongo_pool_api:update_one(?MODULE, <<"role">>, #{}, Doc, #{<<"upsert">> => true}, #{});
do_proc_loop(5) -> %% 删除
    RoleId = #{<<"rid">> => rand:uniform(1000000), <<"srv_id">> => <<"test_1">>},
    mongo_pool_api:delete_one(?MODULE, <<"role">>, RoleId).

wait_procs(Map) ->
    maps:foreach(
        fun(Pid, Ref) ->
            receive
                {'DOWN', Ref, process, Pid, _Reason} ->
                    continue
            end
        end, Map
    ).
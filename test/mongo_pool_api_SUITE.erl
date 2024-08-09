%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_pool_api_SUITE).
-author("jiaoyinyi").

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-compile(export_all).

all() ->
    [
        ensure_index_test,
        count_test,
        find_one_test,
        find_many_test,
        upsert_and_update_test
    ].

init_per_suite(Config) ->
    Opts = #{
        database => <<"test_1">>,
        login => <<"test">>,
        password => <<"test">>,
        auth_source => <<"admin">>
    },
    {ok, _Pid} = mongo_pool_api:start_pool(test_mongo, #{size => 10}, Opts), %% 测试期间，start_pool不要link，测试底层会换进程导致进程挂掉
    [{pool_name, test_mongo} | Config].

end_per_suite(Config) ->
    PoolName = ?config(pool_name, Config),
    ok = mongo_pool_api:stop_pool(PoolName),
    ok.

init_per_testcase(Case, Config) ->
    [{collection, collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
    PoolName = ?config(pool_name, Config),
    Collection = ?config(collection, Config),
    mongo_pool_api:delete_many(PoolName, Collection, #{}).

ensure_index_test(Config) ->
    PoolName = ?config(pool_name, Config),
    Collection = ?config(collection, Config),
    {ok, _} = mongo_pool_api:create_index(PoolName, Collection, [#{<<"key">> => #{<<"rid">> => 1, <<"srv_id">> => 1}, <<"name">> => <<"role_id">>, <<"unique">> => true}]),
    {ok, _} = mongo_pool_api:drop_index(PoolName, Collection, <<"role_id">>),
    Config.

count_test(Config) ->
    PoolName = ?config(pool_name, Config),
    Collection = ?config(collection, Config),
    {ok, 0} = mongo_pool_api:count(PoolName, Collection, #{}),
    {ok, #{<<"n">> := 1}} = mongo_pool_api:insert_one(PoolName, Collection, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>}),
    {ok, #{<<"n">> := 2}} = mongo_pool_api:insert_many(PoolName, Collection, [
        #{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}
    ]),
    {ok, N} = mongo_pool_api:count(PoolName, Collection, #{}),
    ?assertEqual(3, N),
    Config.

find_one_test(Config) ->
    PoolName = ?config(pool_name, Config),
    Collection = ?config(collection, Config),
    {error, not_found} = mongo_pool_api:find_one(PoolName, Collection, #{}, #{<<"name">> => true}),
    {ok, #{<<"n">> := 3}} = mongo_pool_api:insert_many(PoolName, Collection, [
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}
    ]),
    {ok, #{<<"name">> := <<"Mark">>}} = mongo_pool_api:find_one(PoolName, Collection, #{}, #{<<"name">> => true}),
    {error, not_found} = mongo_pool_api:find_one(PoolName, Collection, #{<<"name">> => <<"Batman">>}, #{<<"name">> => true}),
    Config.

find_many_test(Config) ->
    PoolName = ?config(pool_name, Config),
    Collection = ?config(collection, Config),
    {error, not_found} = mongo_pool_api:find_many(PoolName, Collection, #{}, #{<<"name">> => true}),
    {ok, #{<<"n">> := 3}} = mongo_pool_api:insert_many(PoolName, Collection, [
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}
    ]),
    {error, not_found} = mongo_pool_api:find_many(PoolName, Collection, #{<<"name">> => <<"Batman">>}, #{<<"name">> => true}),
    {ok, Cursor} =
        mongo_pool_api:find_many(PoolName, Collection, #{<<"srv_id">> => <<"test_1">>}, #{<<"name">> => true}),
    [
        #{<<"name">> := <<"Mark">>},
        #{<<"name">> := <<"Mark">>},
        #{<<"name">> := <<"Mary">>}
    ] = mongo_cursor:rest(Cursor),
    Config.

upsert_and_update_test(Config) ->
    PoolName = ?config(pool_name, Config),
    Collection = ?config(collection, Config),
    {ok, #{<<"n">> := 1}} = mongo_pool_api:update_one(PoolName, Collection, #{},
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"upsert">> => true}, #{}),
    {ok, Cursor} = mongo_pool_api:find_many(PoolName, Collection, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{}),
    [#{<<"rid">> := 1, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>}] = mongo_cursor:rest(Cursor),

    %update existent fields
    Command = #{
        <<"quantity">> => 500,
        <<"details">> => #{<<"model">> => "14Q3"},
        <<"tags">> => ["coats", "outerwear", "clothing"]
    },
    {ok, _} = mongo_pool_api:update_many(PoolName, Collection,
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{<<"$set">> => mongo_util:flatten_map(Command)}),

    {ok, Cursor1} = mongo_pool_api:find_many(PoolName, Collection, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{}),

    [#{
        <<"rid">> := 1,
        <<"srv_id">> := <<"test_1">>,
        <<"name">> := <<"Mark">>,
        <<"quantity">> := 500,
        <<"details">> := #{<<"model">> := "14Q3"},
        <<"tags">> := ["coats", "outerwear", "clothing"]
    }] = mongo_cursor:rest(Cursor1),
    Config.

collection(Case) ->
    Now = now_to_seconds(os:timestamp()),
    <<(atom_to_binary(?MODULE, utf8))/binary, $-,
        (atom_to_binary(Case, utf8))/binary, $-,
        (list_to_binary(integer_to_list(Now)))/binary>>.

now_to_seconds({Mega, Sec, _}) ->
    (Mega * 1000000) + Sec.
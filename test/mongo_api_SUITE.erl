%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_api_SUITE).
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
    [{database, <<"test_1">>} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Opts = #{
        database => ?config(database, Config),
        login => <<"test">>,
        password => <<"test">>,
        auth_source => <<"admin">>
    },
    {ok, Pid} = mongo_api:connect(Opts),
    [{connection, Pid}, {collection, collection(Case)} | Config].

end_per_testcase(_Case, Config) ->
    Connection = ?config(connection, Config),
    Collection = ?config(collection, Config),
    mongo_api:delete_many(Connection, Collection, #{}),
    mongo_api:close(Connection).

ensure_index_test(Config) ->
    Pid = ?config(connection, Config),
    Collection = ?config(collection, Config),
    {ok, _} = mongo_api:create_index(Pid, Collection, [#{<<"key">> => #{<<"rid">> => 1, <<"srv_id">> => 1}, <<"name">> => <<"role_id">>, <<"unique">> => true}]),
    {ok, _} = mongo_api:drop_index(Pid, Collection, <<"role_id">>),
    Config.

count_test(Config) ->
    Collection = ?config(collection, Config),
    Pid = ?config(connection, Config),
    {ok, 0} = mongo_api:count(Pid, Collection, #{}),
    {ok, #{<<"n">> := 1}} = mongo_api:insert_one(Pid, Collection, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>}),
    {ok, #{<<"n">> := 2}} = mongo_api:insert_many(Pid, Collection, [
        #{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}
    ]),
    {ok, N} = mongo_api:count(Pid, Collection, #{}),
    ?assertEqual(3, N),
    Config.

find_one_test(Config) ->
    Collection = ?config(collection, Config),
    Pid = ?config(connection, Config),
    {error, not_found} = mongo_api:find_one(Pid, Collection, #{}, #{<<"name">> => true}),
    {ok, #{<<"n">> := 3}} = mongo_api:insert_many(Pid, Collection, [
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}
    ]),
    {ok, #{<<"name">> := <<"Mark">>}} = mongo_api:find_one(Pid, Collection, #{}, #{<<"name">> => true}),
    {error, not_found} = mongo_api:find_one(Pid, Collection, #{<<"name">> => <<"Batman">>}, #{<<"name">> => true}),
    Config.

find_many_test(Config) ->
    Collection = ?config(collection, Config),
    Pid = ?config(connection, Config),
    {error, not_found} = mongo_api:find_many(Pid, Collection, #{}, #{<<"name">> => true}),
    {ok, #{<<"n">> := 3}} = mongo_api:insert_many(Pid, Collection, [
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 2, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"rid">> => 3, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mary">>}
    ]),
    {error, not_found} = mongo_api:find_many(Pid, Collection, #{<<"name">> => <<"Batman">>}, #{<<"name">> => true}),
    {ok, Cursor} =
        mongo_api:find_many(Pid, Collection, #{<<"srv_id">> => <<"test_1">>}, #{<<"name">> => true}),
    [
        #{<<"name">> := <<"Mark">>},
        #{<<"name">> := <<"Mark">>},
        #{<<"name">> := <<"Mary">>}
    ] = mongo_cursor:rest(Cursor),
    Config.

upsert_and_update_test(Config) ->
    Collection = ?config(collection, Config),
    Pid = ?config(connection, Config),
    {ok, #{<<"n">> := 1}} = mongo_api:update_one(Pid, Collection, #{},
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>, <<"name">> => <<"Mark">>},
        #{<<"upsert">> => true}, #{}),
    {ok, Cursor} =
        mongo_api:find_many(Pid, Collection, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{}),
    [#{<<"rid">> := 1, <<"srv_id">> := <<"test_1">>, <<"name">> := <<"Mark">>}] = mongo_cursor:rest(Cursor),

    %update existent fields
    Command = #{
        <<"quantity">> => 500,
        <<"details">> => #{<<"model">> => "14Q3"},
        <<"tags">> => ["coats", "outerwear", "clothing"]
    },
    {ok, _} = mongo_api:update_many(Pid, Collection,
        #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{<<"$set">> => mongo_util:flatten_map(Command)}),

    {ok, Cursor1} = mongo_api:find_many(Pid, Collection, #{<<"rid">> => 1, <<"srv_id">> => <<"test_1">>}, #{}),

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
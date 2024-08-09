%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 杂项库
%%% @end
%%% Created : 26. 6月 2024 00:00
%%%-------------------------------------------------------------------
-module(mongo_util).
-author("jiaoyinyi").

%% API
-export([
    ensure_list/1
    , flatten_map/1
]).

%% @doc 确保为列表
-spec ensure_list(term()) -> list().
ensure_list(List) when is_list(List) ->
    List;
ensure_list(I) ->
    [I].

%% Flattens map, add dot notation to all nested objects
-spec flatten_map(map()) -> map().
flatten_map(Map) ->
    flatten(<<>>, Map, #{}).

%% @private
flatten(Key, Map, Acc) when is_map(Map) ->
    maps:fold(fun(K, V, Res) -> flatten(<<(append_dot(Key))/binary, K/binary>>, V, Res) end, Acc, Map);
flatten(Key, Value, Acc) ->
    maps:put(Key, Value, Acc).

%% @private
append_dot(<<>>) -> <<>>;
append_dot(Key) when is_atom(Key) -> append_dot(atom_to_binary(Key, utf8));
append_dot(Key) when is_integer(Key) -> append_dot(integer_to_binary(Key));
append_dot(Key) when is_float(Key) -> append_dot(float_to_binary(Key));
append_dot(Key) when is_list(Key) -> append_dot(list_to_binary(Key));
append_dot(Key) -> <<Key/binary, <<".">>/binary>>.

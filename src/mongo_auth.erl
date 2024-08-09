%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动认证
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_auth).
-author("jiaoyinyi").

%% API
-export([
    auth/4
]).

-include("mongo.hrl").

-define(RANDOM_LENGTH, 24).
-define(GS2_HEADER, <<"n,,">>).

%% 认证
-spec auth(pid(), database(), binary(), binary()) -> boolean() | {error, term()}.
auth(Conn, Database, Login, Password) -> %% mongodb需要大于2.7
    try
        scram_sha_1_auth(Conn, Database, Login, Password)
    catch _:Reason:Stacktrace ->
        {error, {Reason, Stacktrace}}
    end.

%% @private
-spec scram_sha_1_auth(port(), binary(), binary(), binary()) -> boolean().
scram_sha_1_auth(Conn, Database, Login, Password) ->
    scram_first_step(Conn, Database, Login, Password).

scram_first_step(Conn, Database, Login, Password) ->
    RandomBString = priv_random_nonce(?RANDOM_LENGTH),
    FirstMessage = priv_compose_first_message(Login, RandomBString),
    Message = <<?GS2_HEADER/binary, FirstMessage/binary>>,
    Cmd = [{<<"saslStart">>, 1}, {<<"mechanism">>, <<"SCRAM-SHA-1">>}, {<<"payload">>, {data, binary, Message}}, {<<"autoAuthorize">>, 1}],
    {ok, Res} = mongo_api:database_command(Conn, Database, Cmd),
    ConversationId = maps:get(<<"conversationId">>, Res, {}),
    Payload = maps:get(<<"payload">>, Res),
    scram_second_step(Conn, Database, Login, Password, Payload, ConversationId, RandomBString, FirstMessage).
priv_random_nonce(TextLength) ->
    ByteLength = trunc(TextLength / 4 * 3),
    RandBytes = crypto:strong_rand_bytes(ByteLength),
    base64:encode(RandBytes).
priv_compose_first_message(Login, RandomBString) ->
    UserName = <<<<"n=">>/binary, (priv_encode_name(Login))/binary>>,
    Nonce = <<<<"r=">>/binary, RandomBString/binary>>,
    <<UserName/binary, <<",">>/binary, Nonce/binary>>.
priv_encode_name(Name) ->
    Comma = re:replace(Name, <<"=">>, <<"=3D">>, [{return, binary}]),
    re:replace(Comma, <<",">>, <<"=2C">>, [{return, binary}]).

scram_second_step(Conn, Database, Login, Password, {data, binary, Decoded}, ConversationId, RandomBString, FirstMessage) ->
    {Signature, ClientFinalMessage} = priv_compose_second_message(Decoded, Login, Password, RandomBString, FirstMessage),
    Cmd = [{<<"saslContinue">>, 1}, {<<"conversationId">>, ConversationId}, {<<"payload">>, {data, binary, ClientFinalMessage}}],
    {ok, Res} = mongo_api:database_command(Conn, Database, Cmd),
    scram_third_step(base64:encode(Signature), Res, ConversationId, Conn, Database).
priv_compose_second_message(Payload, Login, Password, RandomBString, FirstMessage) ->
    ParamList = priv_parse_response(Payload),
    R = priv_get_value(<<"r">>, ParamList),
    Nonce = <<<<"r=">>/binary, R/binary>>,
    {0, ?RANDOM_LENGTH} = binary:match(R, [RandomBString], []),
    S = priv_get_value(<<"s">>, ParamList),
    I = binary_to_integer(priv_get_value(<<"i">>, ParamList)),
    SaltedPassword = priv_salt_pwd(priv_pw_hash(Login, Password), base64:decode(S), I),
    ChannelBinding = <<<<"c=">>/binary, (base64:encode(?GS2_HEADER))/binary>>,
    ClientFinalMessageWithoutProof = <<ChannelBinding/binary, <<",">>/binary, Nonce/binary>>,
    AuthMessage = <<FirstMessage/binary, <<",">>/binary, Payload/binary, <<",">>/binary, ClientFinalMessageWithoutProof/binary>>,
    ServerSignature = priv_generate_sig(SaltedPassword, AuthMessage),
    Proof = priv_generate_proof(SaltedPassword, AuthMessage),
    {ServerSignature, <<ClientFinalMessageWithoutProof/binary, <<",">>/binary, Proof/binary>>}.
priv_pw_hash(Username, Password) ->
    Bin = crypto:hash(md5, [Username, <<":mongo:">>, Password]),
    HexStr = lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]),
    unicode:characters_to_binary(HexStr, utf8).
priv_salt_pwd(Password, Salt, Iterations) ->
    {ok, Key} = pbkdf2:pbkdf2(sha, Password, Salt, Iterations, 20),
    Key.
priv_generate_proof(SaltedPassword, AuthMessage) ->
    ClientKey = priv_hmac(SaltedPassword, <<"Client Key">>),
    StoredKey = crypto:hash(sha, ClientKey),
    Signature = priv_hmac(StoredKey, AuthMessage),
    ClientProof = priv_xorKeys(ClientKey, Signature, <<>>),
    <<<<"p=">>/binary, (base64:encode(ClientProof))/binary>>.
priv_generate_sig(SaltedPassword, AuthMessage) ->
    ServerKey = priv_hmac(SaltedPassword, "Server Key"),
    priv_hmac(ServerKey, AuthMessage).
priv_hmac(One, Two) ->
    crypto:mac(hmac, sha, One, Two).
priv_xorKeys(<<>>, _, Res) -> Res;
priv_xorKeys(<<FA, RestA/binary>>, <<FB, RestB/binary>>, Res) ->
    priv_xorKeys(RestA, RestB, <<Res/binary, <<(FA bxor FB)>>/binary>>).

scram_third_step(ServerSignature, Response, ConversationId, Conn, Database) ->
    {data, binary, Payload} = maps:get(<<"payload">>, Response),
    Done = maps:get(<<"done">>, Response, false),
    ParamList = priv_parse_response(Payload),
    ServerSignature = priv_get_value(<<"v">>, ParamList),
    scram_forth_step(Done, ConversationId, Conn, Database).

scram_forth_step(true, _, _, _) -> true;
scram_forth_step(false, ConversationId, Conn, Database) ->
    Cmd = [{<<"saslContinue">>, 1}, {<<"conversationId">>, ConversationId}, {<<"payload">>, {data, binary, <<>>}}],
    {ok, Res} = mongo_api:database_command(Conn, Database, Cmd),
    true = maps:get(<<"done">>, Res, false).

priv_parse_response(Responce) ->
    ParamList = binary:split(Responce, <<",">>, [global]),
    lists:map(
        fun(Param) ->
            [K, V] = binary:split(Param, <<"=">>),
            {K, V}
        end, ParamList).

priv_get_value(Key, List) ->
    priv_get_value(Key, List, undefined).
priv_get_value(Key, List, Default) ->
    case lists:keyfind(Key, 1, List) of
        {_, Value} -> Value;
        false -> Default
    end.
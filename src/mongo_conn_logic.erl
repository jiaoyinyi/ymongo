%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动连接器逻辑
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_conn_logic).
-author("jiaoyinyi").

%% API
-export([
    connect/1
    , request/3
    , decode_responses/1
    , response/2
    , need_hibernate/2
]).

-include("mongo.hrl").

-define(MAX_REQ_ID, 2147483647).

%% @doc 建立连接
-spec connect(map()) -> {ok, gen_tcp:socket() | ssl:sslsocket()} | {error, inet:posix()}.
connect(Opts) ->
    Host = maps:get(host, Opts, "127.0.0.1"),
    Port = maps:get(port, Opts, 27017),
    SSL = maps:get(ssl, Opts, false),
    SSLOpts = maps:get(ssl_opts, Opts, []),
    do_connect(Host, Port, SSL, SSLOpts).

do_connect(Host, Port, true, SSLOpts) ->
    {ok, _} = application:ensure_all_started(ssl),
    ssl:connect(Host, Port, [binary, {active, true}, {packet, raw}] ++ SSLOpts);
do_connect(Host, Port, false, _) ->
    gen_tcp:connect(Host, Port, [binary, {active, true}, {packet, raw}]).

%% @doc 处理请求
-spec request(#mongo_command{}, term(), #mongo_conn{}) -> #mongo_conn{}.
request(Req0 = #mongo_command{db = Db0, type = Type}, From, Conn = #mongo_conn{db = Db, net_mod = NetMod, socket = Socket, reqs = Reqs}) ->
    Req =
        case Db0 =:= undefined of
            true -> Req0#mongo_command{db = Db};
            _ -> Req0
        end,
    Command = mongo_man:pack_command(Req),
    ReqId = get_req_id(),
    case mongo_protocol:try_pack_req(ReqId, Command) of %% 兼容外部输入指令异常，导致打包失败异常导致进程挂掉（正常应由外部限制输入）
        {ok, Bin} ->
            ?MONGO_DFORMAT("发送请求，请求ID：~w，请求数据：~w，指令：~w，之前是否有该指令：~w", [ReqId, Req, Command, maps:is_key(ReqId, Reqs)]),
            ok = NetMod:send(Socket, Bin),
            ReqState = #mongo_req_state{req_id = ReqId, from = From, type = Type},
            NewReqs = maps:put(ReqId, ReqState, Reqs),
            NewConn0 = Conn#mongo_conn{reqs = NewReqs},
            NewConn = need_hibernate(byte_size(Bin), NewConn0),
            NewConn;
        {error, Reason} ->
            gen_server:reply(From, {error, Reason}),
            Conn
    end.

%% @doc 解包响应
-spec decode_responses(binary()) -> {list(), binary()}.
decode_responses(Data) ->
    decode_responses(Data, []).
decode_responses(<<Length:32/signed-little, Data/binary>>, Acc) when byte_size(Data) >= (Length - 4) ->
    ResLength = Length - 4,
    <<ResData:ResLength/binary, RestData/binary>> = Data,
    Res = mongo_protocol:unpack_res(ResData),
    decode_responses(RestData, [Res | Acc]);
decode_responses(Data, Acc) ->
    {lists:reverse(Acc), Data}.

%% @doc 响应处理
-spec response(list(), map()) -> map().
response([], Reqs) ->
    Reqs;
response([{ResId, Res} | Ress], Reqs) ->
    case maps:find(ResId, Reqs) of
        {ok, ReqState = #mongo_req_state{from = From}} ->
            Reply = mongo_man:pack_reply(Res, ReqState),
            ?MONGO_DFORMAT("响应请求，响应ID：~w，响应数据：~w，请求状态：~w，响应：~w", [ResId, Res, ReqState, Reply]),
            gen_server:reply(From, Reply),
            response(Ress, maps:remove(ResId, Reqs));
        error ->
            response(Ress, Reqs)
    end.

%% @doc 判断是否需要进行垃圾回收
-spec need_hibernate(integer(), #mongo_conn{}) -> #mongo_conn{}.
need_hibernate(BSize, Conn) when BSize < 64 ->
    Conn;
need_hibernate(_BSize, Conn = #mongo_conn{gc_ref = undefined}) ->
    Ref = erlang:send_after(1000, self(), hibernate),
    Conn#mongo_conn{gc_ref = Ref};
need_hibernate(_BSize, Conn) ->
    Conn.

%% @doc 获取请求ID
get_req_id() ->
    ReqId =
        case get('@req_id') of
            undefined -> 1;
            ReqId0 -> ReqId0
        end,
    NextReqId =
        case ReqId >= ?MAX_REQ_ID of
            true -> 1;
            _ -> ReqId + 1
        end,
    put('@req_id', NextReqId),
    ReqId.
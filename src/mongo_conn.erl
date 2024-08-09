%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动连接进程
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_conn).
-author("jiaoyinyi").

-behaviour(gen_server).

-export([
    request_api/2, request_api/3
]).
-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-include("mongo.hrl").

%% @doc api请求
-spec request_api(pid(), term()) -> {error, term()} | any().
request_api(Conn, Request) ->
    request_api(Conn, Request, ?MONGO_DEF_TIMEOUT).
-spec request_api(pid(), term(), timeout()) -> {error, term()} | any().
request_api(Conn, Request, Timeout) ->
    try
        gen_server:call(Conn, {api, Request}, Timeout)
    catch
        _:Reason ->
            case Reason of
                {timeout, _} -> {error, timeout};
                {noproc, _} -> {error, noproc};
                {normal, _} -> {error, exit};
                _ -> {error, Reason}
            end
    end.

%% @doc 关闭连接器
-spec stop(pid()) -> ok | {error, term()}.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Opts) ->
    case gen_server:start_link(?MODULE, [Opts], []) of
        {ok, Conn} ->
            Login = maps:get(login, Opts, undefined),
            Password = maps:get(password, Opts, undefined),
            case Login =/= undefined andalso Password =/= undefined of
                true ->
                    AuthSource = maps:get(auth_source, Opts, <<"admin">>),
                    case mongo_auth:auth(Conn, AuthSource, Login, Password) of
                        true ->
                            {ok, Conn};
                        _ ->
                            mongo_conn:stop(Conn),
                            {error, auth_fail}
                    end;
                false ->
                    {ok, Conn}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

init([Opts]) ->
    case mongo_conn_logic:connect(Opts) of
        {ok, Socket} ->
            Conn = #mongo_conn{
                socket = Socket
                , net_mod = get_opt_net_mod(Opts)
                , reqs = #{}
                , db = get_opt_database(Opts)
                , buffer = <<>>
            },
            ?MONGO_DFORMAT("创建连接进程成功，状态：~w", [Conn]),
            {ok, Conn};
        {error, Reason} ->
            {stop, Reason}
    end.

%% 处理api请求
handle_call({api, Request}, From, State) ->
    NewState = mongo_conn_logic:request(Request, From, State),
    {noreply, NewState};

%% 主动关闭
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, {error, not_achieve}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

%% socket处理
handle_info({Net, _Socket, Bin}, Conn = #mongo_conn{buffer = Buffer, reqs = Reqs}) when Net =:= tcp; Net =:= ssl ->
    NewBuffer0 = <<Buffer/binary, Bin/binary>>,
    {Ress, NewBuffer} = mongo_conn_logic:decode_responses(NewBuffer0),
    NewReqs = mongo_conn_logic:response(Ress, Reqs),
    NewConn0 = Conn#mongo_conn{buffer = NewBuffer, reqs = NewReqs},
    NewConn = mongo_conn_logic:need_hibernate(byte_size(NewBuffer0) - byte_size(NewBuffer), NewConn0),
    {noreply, NewConn};
handle_info({NetClose, _Socket}, State) when NetClose =:= tcp_closed; NetClose =:= ssl_closed ->
    {stop, socket_closed, State};
handle_info({NetErr, _Socket, Reason}, State) when NetErr =:= tcp_error; NetErr =:= ssl_error ->
    {stop, Reason, State};
%% 休眠垃圾回收
handle_info(hibernate, Conn) ->
    NewConn = Conn#mongo_conn{gc_ref = undefined},
    {noreply, NewConn, hibernate};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _Conn = #mongo_conn{socket = Socket, net_mod = NetMod}) ->
    catch NetMod:close(Socket),
    ?MONGO_DFORMAT("关闭连接进程成功，~w", [_Conn]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc 获取参数数据库
get_opt_database(Opts) ->
    maps:get(database, Opts, <<"test">>).

%% @doc 获取参数网络模块
get_opt_net_mod(Opts) ->
    case maps:get(ssl, Opts, false) of
        true -> ssl;
        false -> gen_tcp
    end.
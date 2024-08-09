%%%-------------------------------------------------------------------
%%% @author jiaoyinyi
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% 驱动协议
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_protocol).
-author("jiaoyinyi").

%% API
-export([
    try_pack_req/2
    , pack_req/2
    , unpack_res/1
]).

-include("mongo.hrl").

%% @doc 尝试打包请求
-spec try_pack_req(integer(), document() | documents()) -> {ok, binary()} | {error, term()}.
try_pack_req(ReqId, Doc) ->
    try
        Bin = mongo_protocol:pack_req(ReqId, Doc),
        {ok, Bin}
    catch
        _:Reason:Stacktrace ->
            {error, {pack_req_err, Reason, Stacktrace}}
    end.

%% @doc 打包请求
-spec pack_req(integer(), document() | documents()) -> binary().
pack_req(ReqId, Doc) ->
    Data = <<
        %% header
        ReqId:32/little-signed                %% requestID
        , 0:32/little-signed                  %% responseTo
        , ?MONGO_OP_MSG:32/little-signed      %% opcode
        %% body
        , 0:32/little-unsigned                %% flagbits
        , (pack_sections(Doc))/binary         %% sections
    >>,
    <<(byte_size(Data) + 4):32/little-signed, Data/binary>>.

pack_sections(Doc) when is_map(Doc) ->
    {ok, Bson} = nbson:encode(Doc),
    <<?MONGO_SECTION_KIND_0:8/little-unsigned, Bson/binary>>;
pack_sections(Doc = [{_, _} | _]) ->
    {ok, Bson} = nbson:encode(Doc),
    <<?MONGO_SECTION_KIND_0:8/little-unsigned, Bson/binary>>;
pack_sections(Docs) when is_list(Docs) ->
    CStr = to_cstring(<<"documents"/utf8>>),
    SectionsBin = docs_to_bsons(Docs),
    Data = <<CStr/binary, SectionsBin/binary>>,
    <<?MONGO_SECTION_KIND_1:8/little-unsigned, (byte_size(Data) + 4):32/signed-little, Data/binary>>.

%% @doc 解包响应
-spec unpack_res(binary()) -> {integer(), document()}.
unpack_res(Data) ->
    <<
        %% header
        _ReqId:32/little-signed               %% requestID
        , ResId:32/little-signed              %% responseTo
        , _OpCode:32/little-signed            %% opcode
        %% body
        , _FlagBits:32/little-unsigned        %% flagbits
        , _SectionKind:8/little-unsigned
        , SectionData/binary                  %% sections
    >> = Data,
    {ok, ResDoc} = nbson:decode(SectionData),
%%    ?MONGO_DFORMAT("解包响应，ReqId：~w，ResId：~w，OpCode：~w，FlagBits：~w，SectionKind：~w", [_ReqId, ResId, _OpCode, _FlagBits, _SectionKind]),
    {ResId, ResDoc}.

to_cstring(UBin) ->
    <<UBin/binary, 0:8>>.

docs_to_bsons(Docs) ->
    docs_to_bsons(Docs, <<>>).
docs_to_bsons([Doc | Docs], Acc) ->
    {ok, Bson} = nbson:encode(Doc),
    docs_to_bsons(Docs, <<Acc/binary, Bson/binary>>);
docs_to_bsons([], Acc) ->
    Acc.
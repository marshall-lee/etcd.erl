-module(etcd_sets).

-export([add/4, add/5, del/4, ismember/4, members/3]).

-include("include/etcd_types.hrl").

%% @spec (Url, Key, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = ok | {error, any()}
%% @end
-spec add(url(), key(), value(), pos_timeout()) -> ok | {error, any()}.
add(Url, Key, Value, Timeout) ->
    {ok, Exists} = exists(Url, Key, Timeout),
    case Exists of
        not_exists -> etcd:set(Url, get_head_key(Key), "1", Timeout);
        exists     -> ok
    end,
    MemberKey = io_lib:format("~s/~s", [Key, hash(Value)]),
    Result = etcd:set(Url, MemberKey, Value, Timeout),
    case Result of
        {ok, {set, _, _, _, _, _, _, _}} -> ok;
        _                                -> {error, Result}
    end.

%% @spec (Url, Key, Value, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = ok | {error, any()}
%% @end
-spec add(url(), key(), value(), pos_integer(), pos_timeout()) -> ok | {error, any()}.
add(Url, Key, Value, TTL, Timeout) ->
    {ok, Exists} = exists(Url, Key, Timeout),
    case Exists of
        not_exists -> etcd:set(Url, get_head_key(Key), "1", Timeout);
        exists     -> ok
    end,
    MemberKey = io_lib:format("~s/~s", [Key, hash(Value)]),
    Result = etcd:set(Url, MemberKey, Value, TTL, Timeout),
    case Result of
        {ok, {set, _, _, _, _, _, _, _}} -> ok;
        _                                -> {error, Result}
    end.

%% @spec (Url, Key, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = ok | {error, any()}
%% @end
-spec del(url(), key(), value(), pos_timeout()) -> ok | {error, any()}.
del(Url, Key, Value, Timeout) ->
    {ok, exists} = exists(Url, Key, Timeout),
    MemberKey = io_lib:format("~s/~s", [Key, hash(Value)]),
    Result = etcd:delete(Url, MemberKey, Timeout),
    case Result of
        {ok, {delete, _, _, _}}  -> ok;
        {ok, {error, 100, _, _}} -> {error, not_in_set};
        _                        -> {error, Result}
    end.

%% @spec (Url, Key, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, boolean()} | {error, any()}
%% @end
-spec ismember(url(), key(), value(), pos_timeout()) -> {ok, boolean()} | {error, any()}.
ismember(Url, Key, Value, Timeout) ->
    {ok, exists} = exists(Url, Key, Timeout),
    MemberKey = io_lib:format("~s/~s", [Key, hash(Value)]),
    Result = etcd:get(Url, MemberKey, Timeout),
    case Result of
        {ok, {get, _, _, _, _}}               -> {ok, true};
        {ok, {error, 100, _, _}}              -> {ok, false};
        {ok, Response} when is_list(Response) -> {error, directory};
        _                                     -> {error, Result}
    end.

%% @spec (Url, Key, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, [binary()]} | {error, any()}
%% @end
-spec members(url(), key(), pos_timeout()) -> {ok, [binary()]} | {error, any()}.
members(Url, Key, Timeout) ->
    {ok, exists} = exists(Url, Key, Timeout),
    HeadKey = get_head_key(Key),
    Result = etcd:get(Url, Key, Timeout),
    case Result of
        {ok, Response} when is_list(Response) ->
            Response1 = lists:keydelete(list_to_binary(HeadKey), 2, Response),
            {ok, [ Value || {get, _, Value, _, _} <- Response1 ]};
        {ok, {get, _, _, _, _}} ->
            {error, not_a_directory};
        _ ->
            {error, Result}
    end.

%% @private
-spec bin_to_hex(binary()) -> string().
bin_to_hex(Binary) ->
    string:to_lower([ hd(integer_to_list(Nibble, 16)) || << Nibble:4 >> <= Binary ]).

%% @private
-spec hash(binary() | string()) -> string().
hash(Data) ->
    bin_to_hex(crypto:hash(md5, Data)).

%% @private
-spec get_head_key(key()) -> string().
get_head_key(Key) ->
    io_lib:format("~s/set-~s", [Key, hash(Key)]).

%% @private
-spec exists(url(), key(), pos_timeout()) -> {ok, exists | not_exists} | {error, any()}.
exists(Url, Key, Timeout) ->
    Result = etcd:get(Url, get_head_key(Key), Timeout),
    case Result of
        {ok, {get, _, _, _, _}}                 -> {ok, exists};
        {ok, {error, 100, _, _}}                -> {ok, not_exists};
        {ok, _Response} when is_list(_Response) -> {error, directory};
        _                                       -> {error, Result}
    end.


-module(etcd).

-export([start/0, stop/0]).
-export([set/4, set/5]).
-export([test_and_set/5, test_and_set/6]).
-export([get/3]).
-export([delete/3]).
-export([watch/3, watch/4]).

-include("include/etcd_types.hrl").

%% @doc Start application with all depencies
-spec start() -> ok | {error, term()}.
start() ->
    case application:ensure_all_started(etcd) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Stop application
-spec stop() -> ok | {error, term()}.
stop() ->
    application:stop(etcd).

%% @spec (Url, Key, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec set(url(), key(), value(), pos_timeout()) -> result().
set(Url, Key, Value, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = post_request(FullUrl, [{"value", Value}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Value, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Value = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec set(url(), key(), value(), pos_integer(), pos_timeout()) -> result().
set(Url, Key, Value, TTL, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = post_request(FullUrl, [{"value", Value}, {"ttl", TTL}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, PrevValue, Value, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   PrevValue = binary() | string()
%%   Value = binary() | string()
%%   Timeout = pos_integer() | 'infinity'
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec test_and_set(url(), key(), value(), value(), pos_timeout()) -> result().
test_and_set(Url, Key, PrevValue, Value, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = post_request(FullUrl, [{"value", Value}, {"prevValue", PrevValue}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, PrevValue, Value, TTL, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   PrevValue = binary() | string()
%%   Value = binary() | string()
%%   TTL = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec test_and_set(url(), key(), value(), value(), pos_integer(), pos_timeout()) -> result().
test_and_set(Url, Key, PrevValue, Value, TTL, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = post_request(FullUrl, [{"value", Value}, {"prevValue", PrevValue}, {"ttl", TTL}], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec get(url(), key(), pos_timeout()) -> result().
get(Url, Key, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = lhttpc:request(FullUrl, get, [], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec delete(url(), key(), pos_timeout()) -> result().
delete(Url, Key, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/keys" ++ convert_to_string(Key),
    Result = lhttpc:request(FullUrl, delete, [], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec watch(url(), key(), pos_timeout()) -> result().
watch(Url, Key, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/watch" ++ convert_to_string(Key),
    Result = lhttpc:request(FullUrl, get, [], Timeout),
    handle_request_result(Result).

%% @spec (Url, Key, Index, Timeout) -> Result
%%   Url = string()
%%   Key = binary() | string()
%%   Index = pos_integer()
%%   Timeout = pos_integer() | infinity
%%   Result = {ok, response() | [response()]} | {http_error, atom()}.
%% @end
-spec watch(url(), key(), pos_integer(), pos_timeout()) -> result().
watch(Url, Key, Index, Timeout) ->
    FullUrl = url_prefix(Url) ++ "/watch" ++ convert_to_string(Key),
    Result = post_request(FullUrl, [{"index", Index}], Timeout),
    handle_request_result(Result).

%% @private
convert_to_string(Value) when is_integer(Value) ->
    integer_to_list(Value);
convert_to_string(Value) when is_binary(Value) ->
    binary_to_list(Value);
convert_to_string(Value) when is_list(Value) ->
    Value.

%% @private
encode_params(Pairs) ->
    List = [ http_uri:encode(convert_to_string(Key)) ++ "=" ++ http_uri:encode(convert_to_string(Value)) || {Key, Value} <- Pairs ],
    binary:list_to_bin(string:join(List, "&")).

%% @private
url_prefix(Url) ->
    Url ++ "/v1".

%% @private
post_request(Url, Pairs, Timeout) ->
    Body = encode_params(Pairs),
    Headers = [{"Content-Type", "application/x-www-form-urlencoded"}],
    lhttpc:request(Url, post, Headers, Body, Timeout).

%% @private
parse_response(Decoded) when is_list(Decoded) ->
    [ parse_response_inner(Pairs) || {Pairs} <- Decoded ];
parse_response(Decoded) when is_tuple(Decoded) ->
    {Pairs} = Decoded,
    IsError = lists:keyfind(<<"errorCode">>, 1, Pairs),
    case IsError of
        {_, ErrorCode} ->
            parse_error_response(Pairs, #error{ errorCode = ErrorCode });
        false ->
            parse_response_inner(Pairs)
    end.

%% @private
parse_response_inner(Pairs) ->
    {_, Action} = lists:keyfind(<<"action">>, 1, Pairs),
    case Action of
        <<"SET">> ->
            parse_set_response(Pairs, #set{});
        <<"GET">> ->
            parse_get_response(Pairs, #get{});
        <<"DELETE">> ->
            parse_delete_response(Pairs, #delete{})
    end.

%% @private
parse_error_response([], Acc) ->
    Acc;
parse_error_response([Pair | Tail], Acc) ->
    {_, ErrorCode, Message, Cause} = Acc,
    case Pair of
        {<<"message">>, Message1} ->
            parse_error_response(Tail, {error, ErrorCode, Message1, Cause});
        {<<"cause">>, Cause1} ->
            parse_error_response(Tail, {error, ErrorCode, Message, Cause1});
        _ ->
            parse_error_response(Tail, Acc)
    end.

%% @private
parse_set_response([], Acc) ->
    Acc;
parse_set_response([Pair | Tail], Acc) ->
    {_, Key, Value, PrevValue, NewKey, Expiration, TTL, Index} = Acc,
    case Pair of
        {<<"key">>, Key1} ->
            parse_set_response(Tail, {set, Key1, Value, PrevValue, NewKey, Expiration, TTL, Index});
        {<<"value">>, Value1} ->
            parse_set_response(Tail, {set, Key, Value1, PrevValue, NewKey, Expiration, TTL, Index});
        {<<"prevValue">>, PrevValue1} ->
            parse_set_response(Tail, {set, Key, Value, PrevValue1, NewKey, Expiration, TTL, Index});
        {<<"newKey">>, NewKey1} ->
            parse_set_response(Tail, {set, Key, Value, PrevValue, NewKey1, Expiration, TTL, Index});
        {<<"expiration">>, Expiration1} ->
            parse_set_response(Tail, {set, Key, Value, PrevValue, NewKey, Expiration1, TTL, Index});
        {<<"ttl">>, TTL1} ->
            parse_set_response(Tail, {set, Key, Value, PrevValue, NewKey, Expiration, TTL1, Index});
        {<<"index">>, Index1} ->
            parse_set_response(Tail, {set, Key, Value, PrevValue, NewKey, Expiration, TTL, Index1});
        _ ->
            parse_set_response(Tail, Acc)
    end.

%% @private
parse_get_response([], Acc) ->
    Acc;
parse_get_response([Pair | Tail], Acc) ->
    {_, Key, Value, Dir, Index} = Acc,
    case Pair of
        {<<"key">>, Key1} ->
            parse_get_response(Tail, {get, Key1, Value, Dir, Index});
        {<<"value">>, Value1} ->
            parse_get_response(Tail, {get, Key, Value1, Dir, Index});
        {<<"dir">>, Dir1} ->
            parse_get_response(Tail, {get, Key, Value, Dir1, Index});
        {<<"index">>, Index1} ->
            parse_get_response(Tail, {get, Key, Value, Dir, Index1});
        _ ->
            parse_get_response(Tail, Acc)
    end.

%% @private
parse_delete_response([], Acc) ->
    Acc;
parse_delete_response([Pair | Tail], Acc) ->
    {_, Key, PrevValue, Index} = Acc,
    case Pair of
        {<<"key">>, Key1} ->
            parse_delete_response(Tail, {delete, Key1, PrevValue, Index});
        {<<"prevValue">>, PrevValue1} ->
            parse_delete_response(Tail, {delete, Key, PrevValue1, Index});
        {<<"index">>, Index1} ->
            parse_delete_response(Tail, {delete, Key, PrevValue, Index1});
        _ ->
            parse_delete_response(Tail, Acc)
    end.

%% @private
handle_request_result(Result) ->
    case Result of
        {ok, {{_StatusCode, _ReasonPhrase}, _Hdrs, ResponseBody}} ->
            Decoded = jiffy:decode(ResponseBody),
            {ok, parse_response(Decoded)};
        {error, Reason} ->
            {http_error, Reason}
    end.


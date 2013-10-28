-module(etcd_integration_tests).
-include_lib("eunit/include/eunit.hrl").
-define(URL, "http://localhost:4001").

set_test() ->
    etcd:start(),
    Result1 = etcd:set(?URL, "/file", "contents", infinity),
    ?assertMatch({ok, {set, <<"/file">>, <<"contents">>, undefined, true, undefined, undefined, _}}, Result1),
    Result2 = etcd:set(?URL, "/file", "new contents", infinity),
    ?assertMatch({ok, {set, <<"/file">>, <<"new contents">>, <<"contents">>, false, undefined, undefined, _}}, Result2),
    etcd:delete(?URL, "/file", infinity).

test_and_set_test() ->
    etcd:start(),
    etcd:set(?URL, "/file", "contents", infinity),
    Result1 = etcd:test_and_set(?URL, "/file", "contents", "new contents", infinity),
    ?assertMatch({ok, {set, <<"/file">>, <<"new contents">>, <<"contents">>, false, undefined, undefined, _}}, Result1),
    Result2 = etcd:test_and_set(?URL, "/file", "abracadabra", "very new contents", infinity),
    ?assertMatch({ok, {error, 101, _, _}}, Result2), % "The given PrevValue is not equal to the value of the key"
    etcd:delete(?URL, "/file", infinity).

delete_test() ->
    etcd:start(),
    etcd:set(?URL, "/file", "contents", infinity),
    Result1 = etcd:delete(?URL, "/file", infinity),
    ?assertMatch({ok, {delete, <<"/file">>, <<"contents">>, _}}, Result1),
    Result2 = etcd:get(?URL, "/file", infinity),
    ?assertMatch({ok, {error, 100, _, _}}, Result2). % "Key Not Found"

get_test() ->
    etcd:start(),
    etcd:set(?URL, "/file", "contents", infinity),
    Result1 = etcd:get(?URL, "/file", infinity),
    ?assertMatch({ok, {get, <<"/file">>, <<"contents">>, false, _}}, Result1),
    etcd:delete(?URL, "/file", infinity).

get_prefix_test() ->
    etcd:start(),
    etcd:set(?URL, "/dir/file1", "file1 contents", infinity),
    etcd:set(?URL, "/dir/file2", "file2 contents", infinity),
    Result1 = etcd:get(?URL, "/dir", infinity),
    ?assertMatch({ok, [{get, <<"/dir/file1">>, <<"file1 contents">>, false, _}, {get, <<"/dir/file2">>, <<"file2 contents">>, false, _}]}, Result1),
    etcd:delete(?URL, "/dir/file1", infinity),
    etcd:delete(?URL, "/dir/file2", infinity).

watch_test() ->
    etcd:start(),
    Self = self(),
    spawn(fun() ->
                  Result = etcd:watch(?URL, "/dir", infinity),
                  Self ! Result
          end),
    timer:sleep(5),
    etcd:set(?URL, "/dir/file", "contents", infinity),
    receive
        Result ->
            ?assertMatch({ok, {set, <<"/dir/file">>, <<"contents">>, undefined, true, undefined, undefined, _}}, Result)
    end,
    etcd:delete(?URL, "/dir/file", infinity).

watch_index_test() ->
    etcd:start(),
    Result1 = etcd:set(?URL, "/dir/file", "contents", infinity),
    ?assertMatch({ok, {set, <<"/dir/file">>, <<"contents">>, undefined, true, undefined, undefined, _}}, Result1),
    {_, {_, _, _, _, _, _, _, Index}} = Result1,
    Result2 = etcd:watch(?URL, "/dir", Index, infinity),
    ?assertEqual(Result1, Result2),
    etcd:delete(?URL, "/dir/file", infinity).

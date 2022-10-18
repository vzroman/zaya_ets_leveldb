-module(zaya_ets_leveldb_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([
    all/0,
    groups/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    low_level_api_tests/1,
    first_tests/1,
    last_tests/1,
    next_tests/1,
    prev_tests/1,
    find_tests/1,
    foldl_tests/1,
    foldr_tests/1
]).

-define(GET(Key, Value), proplists:get_value(Key, Value)).
-define(GET(Key, Value, Default), proplists:get_value(Key, Value, Default)).
-define(INRAM, element(2, Refs)).
-define(INDISK, element(3, Refs)).

all() -> [
    low_level_api_tests,
    first_tests,
    last_tests,
    next_tests,
    prev_tests,
    find_tests,
    foldl_tests,
    foldr_tests
].

groups() -> [].

init_per_suite(Config) ->
    zaya_ets_leveldb:create(#{dir => "./storage"}),
    Config.

end_per_suite(_Config) ->
    zaya_ets_leveldb:remove(#{leveldb => #{dir => "./storage"}}),
    ok.

init_per_group(_, Config) ->
    Config.

end_per_group(_, _Config) ->
    ok.

init_per_testcase(_, Config) ->
    Refs = zaya_ets_leveldb:open(#{dir => "./storage"}),
    [{refs, Refs} | Config].

end_per_testcase(_, Config) ->
    Refs = ?GET(refs, Config),
    zaya_ets_leveldb:close(Refs).

low_level_api_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records    
    ok = zaya_ets_leveldb:write(Refs, [{K, ok} || K <- lists:seq(1, Count)]),

    [{1, ok}] = zaya_ets_leveldb:read(Refs, [1]),
    [{20, ok}] = zaya_ets_leveldb:read(Refs, [20]),
    [{10020, ok}] = zaya_ets_leveldb:read(Refs, [10020]),
    [{20000, ok}] = zaya_ets_leveldb:read(Refs, [20000]),
    [{1, ok}, {20, ok}, {10020, ok}, {20000, ok}] = zaya_ets_leveldb:read(Refs, [1, 20, 10020, 20000]),

    [{1, ok}] = zaya_ets:read(?INRAM, [1]),
    [{20, ok}] = zaya_ets:read(?INRAM, [20]),
    [{10020, ok}] = zaya_ets:read(?INRAM, [10020]),
    [{20000, ok}] = zaya_ets:read(?INRAM, [20000]),
    [{1, ok}, {20, ok}, {10020, ok}, {20000, ok}] = zaya_ets:read(?INRAM, [1, 20, 10020, 20000]),

    [{1, ok}] = zaya_leveldb:read(?INDISK, [1]),
    [{20, ok}] = zaya_leveldb:read(?INDISK, [20]),
    [{10020, ok}] = zaya_leveldb:read(?INDISK, [10020]),
    [{20000, ok}] = zaya_leveldb:read(?INDISK, [20000]),
    [{1, ok}, {20, ok}, {10020, ok}, {20000, ok}] = zaya_leveldb:read(?INDISK, [1, 20, 10020, 20000]),

    ok = zaya_ets_leveldb:delete(Refs, [1]),
    ok = zaya_ets_leveldb:delete(Refs, [20]),
    ok = zaya_ets_leveldb:delete(Refs, [10020]),
    ok = zaya_ets_leveldb:delete(Refs, [20000]),
    ok = zaya_ets_leveldb:delete(Refs, [10, 30, 10030, 10090]),

    [] = zaya_ets_leveldb:read(Refs, []),
    [] = zaya_ets_leveldb:read(Refs, [1]),
    [] = zaya_ets_leveldb:read(Refs, [20]),
    [] = zaya_ets_leveldb:read(Refs, [10020]),
    [] = zaya_ets_leveldb:read(Refs, [20000]),
    [] = zaya_ets_leveldb:read(Refs, [1, 10, 20, 30, 10020, 10030, 10090, 20000]),

    [] = zaya_ets:read(?INRAM, []),
    [] = zaya_ets:read(?INRAM, [1]),
    [] = zaya_ets:read(?INRAM, [20]),
    [] = zaya_ets:read(?INRAM, [10020]),
    [] = zaya_ets:read(?INRAM, [20000]),
    [] = zaya_ets:read(?INRAM, [1, 10, 20, 30, 10020, 10030, 10090, 20000]),

    [] = zaya_leveldb:read(?INDISK, []),
    [] = zaya_leveldb:read(?INDISK, [1]),
    [] = zaya_leveldb:read(?INDISK, [20]),
    [] = zaya_leveldb:read(?INDISK, [10020]),
    [] = zaya_leveldb:read(?INDISK, [20000]),
    [] = zaya_leveldb:read(?INDISK, [1, 10, 20, 30, 10020, 10030, 10090, 20000]),

    [{2, ok}] = zaya_ets_leveldb:read(Refs, [2]),
    [{22, ok}] = zaya_ets_leveldb:read(Refs, [22]),
    [{10022, ok}] = zaya_ets_leveldb:read(Refs, [10022]),
    [{2, ok}, {22, ok}, {10022, ok}] = zaya_ets_leveldb:read(Refs, [2, 22, 10022]).

first_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records
    ok = zaya_ets_leveldb:write(Refs, [{K, ok} || K <- lists:seq(1, Count)]),

    {1, ok} = zaya_ets_leveldb:first(Refs),

    % Delete every second key in the storage
    ok = zaya_ets_leveldb:delete(Refs, [K || K <- lists:seq(1, Count, 2)]),

    {2, ok} = zaya_ets_leveldb:first(Refs),

    % Remove all records from the storage
    ok = zaya_ets_leveldb:delete(Refs, [K || K <- lists:seq(1, Count)]),

    undefined = (catch zaya_ets_leveldb:first(Refs)).

last_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records
    ok = zaya_ets_leveldb:write(Refs, [{K, ok} || K <- lists:seq(1, Count)]),

    {20000, ok} = zaya_ets_leveldb:last(Refs),

    % Delete every second key in the storage
    ok = zaya_ets_leveldb:delete(Refs, [K || K <- lists:seq(2, Count, 2)]),

    {19999, ok} = zaya_ets_leveldb:last(Refs),

    % Remove all records from the storage
    ok = zaya_ets_leveldb:delete(Refs, [K || K <- lists:seq(1, Count)]),

    undefined = (catch zaya_ets_leveldb:last(Refs)).

next_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records
    ok = zaya_ets_leveldb:write(Refs, [{K, ok} || K <- lists:seq(1, Count)]),

    {1, ok} = zaya_ets_leveldb:next(Refs, 0),
    {2, ok} = zaya_ets_leveldb:next(Refs, 1),
    {21, ok} = zaya_ets_leveldb:next(Refs, 20),
    {10001, ok} = zaya_ets_leveldb:next(Refs, 10000),
    {10021, ok} = zaya_ets_leveldb:next(Refs, 10020),
    undefined = (catch zaya_ets_leveldb:next(Refs, 20000)),

    % Delete every second key in the storage
    ok = zaya_ets_leveldb:delete(Refs, [K || K <- lists:seq(1, Count, 2)]),

    {2, ok} = zaya_ets_leveldb:next(Refs, 0),
    {2, ok} = zaya_ets_leveldb:next(Refs, 1),
    {22, ok} = zaya_ets_leveldb:next(Refs, 20),
    {10002, ok} = zaya_ets_leveldb:next(Refs, 10000),
    {10022, ok} = zaya_ets_leveldb:next(Refs, 10020),
    undefined = (catch zaya_ets_leveldb:next(Refs, 20000)).

prev_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records
    ok = zaya_ets_leveldb:write(Refs, [{K, ok} || K <- lists:seq(1, Count)]),

    undefined = (catch zaya_ets_leveldb:prev(Refs, 1)),
    {1, ok} = zaya_ets_leveldb:prev(Refs, 2),
    {20, ok} = zaya_ets_leveldb:prev(Refs, 21),
    {10000, ok} = zaya_ets_leveldb:prev(Refs, 10001),
    {10020, ok} = zaya_ets_leveldb:prev(Refs, 10021),
    {20000, ok} = zaya_ets_leveldb:prev(Refs, 20001),

    % Delete every second key in the storage
    ok = zaya_ets_leveldb:delete(Refs, [K || K <- lists:seq(1, Count, 2)]),

    undefined = (catch zaya_ets_leveldb:prev(Refs, 2)),
    {2, ok} = zaya_ets_leveldb:prev(Refs, 4),
    {20, ok} = zaya_ets_leveldb:prev(Refs, 22),
    {10000, ok} = zaya_ets_leveldb:prev(Refs, 10001),
    {10000, ok} = zaya_ets_leveldb:prev(Refs, 10002),
    {10020, ok} = zaya_ets_leveldb:prev(Refs, 10022),
    {20000, ok} = zaya_ets_leveldb:prev(Refs, 20001).

find_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records
    ok = zaya_ets_leveldb:write(Refs, [{K, {K rem 10, dog}} || K <- lists:seq(1, Count div 2)]),
    ok = zaya_ets_leveldb:write(Refs, [{K, {K rem 10, cat}} || K <- lists:seq((Count div 2) + 1, Count)]),
    
    %---Ful Query---
    MatchSpec = [
        {
            {'$1', {'$2', '$3'}},
            [
                {'andalso', {'>=', '$2', 0}, {'<', '$2', 5}, {'=:=', '$3', dog}}
            ],
            ['$_']
        }
    ],

    MatchSpec1 = [
        {
            {'$1', {'$2', '$3'}},
            [
                {'andalso', {'>=', '$2', 5}, {'=<', '$2', 9}, {'=:=', '$3', cat}}
            ],
            ['$_']
        }
    ],

    MatchSpec2 = [
        {
            {'$1', {'$2', '$3'}},
            [
                {'orelse', {'<', '$1', 2}, {'>', '$1', 10000}}
            ],
            ['$_']
        }
    ],

    Query = #{
        start => 9995,
        stop => 10010,
        ms => MatchSpec,
        limit => 10
    },

    Query1 = #{
        start => 9995,
        stop => 10010,
        ms => MatchSpec1,
        limit => 3
    },

    Query2 = #{
        stop => 10010,
        ms => MatchSpec2,
        limit => 2
    },

    [{10000, {0, dog}}] = zaya_ets_leveldb:find(Refs, Query),
    [{10005, {5, cat}}, {10006, {6, cat}}, {10007, {7, cat}}] = zaya_ets_leveldb:find(Refs, Query1),
    [{1, {1, dog}}, {10001, {1, cat}}] = zaya_ets_leveldb:find(Refs, Query2),

    %---ms_stop---
    MatchSpec3 = [
        {
            {'$1', {'$2', '$3'}},
            [
                {'=:=', '$2', 3}
            ],
            ['$_']
        }
    ],

    MS_stop = #{
        stop => 20,
        ms => MatchSpec3
    },

    MS_stop1 = #{
        start => 10001,
        stop => 10020,
        ms => MatchSpec3
    },

    [{3, {3, dog}}, {13, {3, dog}}] = zaya_ets_leveldb:find(Refs, MS_stop),
    [{10003, {3, cat}}, {10013, {3, cat}}] = zaya_ets_leveldb:find(Refs, MS_stop1),

    %---stop_limit---
    Stop_limit = #{
        stop => 20,
        limit => 3
    },

    Stop_limit1 = #{
        stop => 3,
        limit => 20
    },

    [{1, {1, dog}}, {2, {2, dog}}, {3, {3, dog}}] = zaya_ets_leveldb:find(Refs, Stop_limit),
    [{1, {1, dog}}, {2, {2, dog}}, {3, {3, dog}}] = zaya_ets_leveldb:find(Refs, Stop_limit1),

    %---stop---
    Stop_only = #{
        start => 10500,
        stop => 10502
    },

    [{10500, {0, cat}}, {10501, {1, cat}}, {10502, {2, cat}}] = zaya_ets_leveldb:find(Refs, Stop_only),

    %---ms_limit---
    MS_limit = #{
        ms => MatchSpec3,
        limit => 3
    },

    MS_limit1 = #{
        start => 10001,
        ms => MatchSpec3,
        limit => 3
    },

    [{3, {3, dog}}, {13, {3, dog}}, {23, {3, dog}}] = zaya_ets_leveldb:find(Refs, MS_limit),
    [{10003, {3, cat}}, {10013, {3, cat}}, {10023, {3, cat}}] = zaya_ets_leveldb:find(Refs, MS_limit1),

    %---ms---
    MatchSpec4 = [
        {
            {'$1', {'$2', '$3'}},
            [
                {'orelse', 
                    {'andalso', {'<', '$1', 20}, {'=:=', '$2', 4}},
                    {'andalso', {'>', '$1', 10000}, {'<', '$1', 10020}, {'=:=', '$2', 2}}
                }
            ],
            ['$_']
        }
    ],

    MS_only = #{
        ms => MatchSpec4
    },

    MS_only1 = #{
        start => 10001,
        ms => MatchSpec4
    },

    [{4, {4, dog}}, {14, {4, dog}}, {10002, {2, cat}}, {10012, {2, cat}}] = zaya_ets_leveldb:find(Refs, MS_only),
    [{10002, {2, cat}}, {10012, {2, cat}}] = zaya_ets_leveldb:find(Refs, MS_only1),

    %---Start_only---
    Start_only = #{
        start => 19997
    },

    [{19997, {7, cat}}, {19998, {8, cat}}, {19999, {9, cat}}, {20000, {0, cat}}] = zaya_ets_leveldb:find(Refs, Start_only),

    %---empty table---
    ok = zaya_ets_leveldb:delete(Refs, [K || K <- lists:seq(1, Count)]),

    %only works for #{ms=>MS, limit=>Limit} Query
    undefined = (catch zaya_ets_leveldb:find(Refs, MS_limit)),

    %---tab2list case---
    ok = zaya_ets_leveldb:write(Refs, [{K, dog} || K <- lists:seq(1, 3)]),

    [{1, dog}, {2, dog}, {3, dog}] = zaya_ets_leveldb:find(Refs, #{}).

foldl_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records
    ok = zaya_ets_leveldb:write(Refs, [{K, K rem 100} || K <- lists:seq(1, Count)]),

    MatchSpec = [
        {
            {'$1', '$2'},
            [
                {'=:=', {'rem', '$2', 2}, 0}
            ],
            ['$_']
        }
    ],

    MS_stop = #{
        ms => MatchSpec,
        stop => 10000
    },

    MS_only = #{
        ms => MatchSpec
    },

    Empty_query = #{},

    MaxFun =
        fun
            ({Key, Value}, B) when (Key + Value) > B -> Key + Value;
            (_, B) -> B
        end,

    {FirstKey, FirstValue} = zaya_ets_leveldb:first(Refs),
    Start = FirstKey + FirstValue,

    10096 = zaya_ets_leveldb:foldl(Refs, MS_stop, MaxFun, Start),
    20096 = zaya_ets_leveldb:foldl(Refs, MS_only, MaxFun, Start),
    20098 = zaya_ets_leveldb:foldl(Refs, Empty_query, MaxFun, Start).

foldr_tests(Config) ->
    Refs = ?GET(refs, Config),
    Count = 20000,

    % fill the storage with records
    ok = zaya_ets_leveldb:write(Refs, [{K, K rem 100} || K <- lists:seq(1, Count)]),

    MatchSpec = [
        {
            {'$1', '$2'},
            [
                {'=:=', {'rem', '$2', 2}, 0}
            ],
            ['$_']
        }
    ],

    MS_stop = #{
        ms => MatchSpec,
        stop => 10000
    },

    MS_only = #{
        ms => MatchSpec
    },

    Empty_query = #{},

    MinFun =
        fun
            ({Key, Value}, B) when (Key + Value) < B -> Key + Value;
            (_, B) -> B
        end,

    {FirstKey, FirstValue} = zaya_ets_leveldb:last(Refs),
    Start = FirstKey + FirstValue,

    10000 = zaya_ets_leveldb:foldr(Refs, MS_stop, MinFun, Start),
    4 = zaya_ets_leveldb:foldr(Refs, MS_only, MinFun, Start),
    2 = zaya_ets_leveldb:foldr(Refs, Empty_query, MinFun, Start).
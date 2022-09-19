
-module(zaya_ets_leveldb).

-ifndef(TEST).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-else.

-define(LOGERROR(Text),ct:pal("error: "++Text)).
-define(LOGERROR(Text,Params),ct:pal("error: "++Text,Params)).
-define(LOGWARNING(Text),ct:pal("warning: "++Text)).
-define(LOGWARNING(Text,Params),ct:pal("warning: "++Text,Params)).
-define(LOGINFO(Text),ct:pal("info: "++Text)).
-define(LOGINFO(Text,Params),ct:pal("info: "++Text,Params)).
-define(LOGDEBUG(Text),ct:pal("debug: "++Text)).
-define(LOGDEBUG(Text,Params),ct:pal("debug: "++Text,Params)).

-endif.

%%=================================================================
%%	SERVICE API
%%=================================================================
-export([
  create/1,
  open/1,
  close/1,
  remove/1
]).

%%=================================================================
%%	LOW_LEVEL API
%%=================================================================
-export([
  read/2,
  write/2,
  delete/2
]).

%%=================================================================
%%	ITERATOR API
%%=================================================================
-export([
  first/1,
  last/1,
  next/2,
  prev/2
]).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
-export([
  find/2,
  foldl/4,
  foldr/4
]).

%%=================================================================
%%	INFO API
%%=================================================================
-export([
  get_size/1
]).

%%=================================================================
%%	COPY API
%%=================================================================
-export([

]).

-record(ref,{ets,leveldb}).

%%=================================================================
%%	SERVICE
%%=================================================================
create( #{leveldb := Params} )->
  zaya_leveldb:create( Params ).

open( #{leveldb := LeveldbParams, ets := EtsParams} )->
  LeveldbRef = zaya_leveldb:open( LeveldbParams ),
  EtsRef = zaya_ets:open( EtsParams ),

  zaya_leveldb:foldl(LeveldbRef,#{},fun(Rec,Acc)->
    zaya_ets:write( EtsRef, [Rec] ),
    Acc
  end,[]),

  #ref{ ets = EtsRef, leveldb = LeveldbRef }.

close( #ref{ets = EtsRef, leveldb = LeveldbRef} )->
  zaya_ets:close( EtsRef ),
  zaya_leveldb:close( LeveldbRef ).

remove( #{leveldb = Params} )->
  zaya_leveldb:remove( Params ).

%%=================================================================
%%	LOW_LEVEL
%%=================================================================
read(#ref{ets = EtsRef}, Keys)->
  zaya_ets:read( EtsRef, Keys ).

write(#ref{ets = EtsRef, leveldb = LeveldbRef}, KVs)->
  zaya_leveldb:write( LeveldbRef, KVs ),
  zaya_ets:write( EtsRef, KVs ).

delete(#ref{ets = EtsRef, leveldb = LeveldbRef}, Keys)->
  zaya_leveldb:delete( LeveldbRef, Keys ),
  zaya_ets:delete( EtsRef, Keys ).

%%=================================================================
%%	ITERATOR
%%=================================================================
first( #ref{ets = EtsRef} )->
  zaya_ets:first( EtsRef ).

last( #ref{ets = EtsRef} )->
  zaya_ets:last( EtsRef ).

next( #ref{ets = EtsRef}, Key )->
  zaya_ets:next( EtsRef, Key ).

prev( #ref{ets = EtsRef}, Key )->
  zaya_ets:prev( EtsRef, Key ).

%%=================================================================
%%	HIGH-LEVEL API
%%=================================================================
%----------------------FIND------------------------------------------
find(#ref{ets = EtsRef}, Query)->
  zaya_ets:find( EtsRef, Query ).

%----------------------FOLD LEFT------------------------------------------
foldl( #ref{ets = EtsRef}, Query, Fun, InAcc )->
  zaya_ets:foldl( EtsRef, Query, Fun, InAcc ).

%----------------------FOLD RIGHT------------------------------------------
foldr( #ref{ets = EtsRef}, Query, Fun, InAcc )->
  zaya_ets:foldr( EtsRef, Query, Fun, InAcc ).

%%=================================================================
%%	INFO
%%=================================================================
get_size( #ref{ets = EtsRef})->
  zaya_ets:get_size( EtsRef ).

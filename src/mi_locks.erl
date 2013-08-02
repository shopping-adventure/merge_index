%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @odc A functional locking structure, used to make segment and
%%      buffer locking more convient.

-module(mi_locks).
-include("merge_index.hrl").
-author("Rusty Klophaus <rusty@basho.com>").
-export([
         new/0,
         claim/2,
         claim_many/2,
         release/2,
         when_free/3
        ]).

-record(lock, {
    key,
    count,
    funs=[]
}).

new() -> dict:new().

claim_many(Keys, Locks) ->
    lists:foldl(fun claim/2, Locks, Keys).

claim(Key, Locks) ->
    case dict:find(Key,Locks) of
        {ok,#lock{count=Count}=Lock}  ->
            dict:store(Key,Lock#lock{count=Count + 1},Locks);
        error ->
            dict:store(Key,#lock{key=Key,count=1,funs=[]},Locks)
    end.

release(Key, Locks) ->
    case dict:find(Key,Locks) of
        {ok,#lock{count=1,funs=Funs}} ->
            [X() || X <- Funs],
            dict:erase(Key,Locks);
        {ok,#lock{count=Count}=Lock} ->
            dict:store(Key,Lock#lock{count=Count - 1},Locks);
        error ->
            throw({lock_does_not_exist, Key})
    end.

%% Run the provided function when the key is free. If the key is
%% currently free, then this is run immeditaely.
when_free(Key, Fun, Locks) ->
    case dict:find(Key,Locks) of
        error ->
            Fun(),
            Locks;
        {ok,#lock{count=0,funs=Funs}} ->
            [X() || X <- [Fun|Funs]],
            dict:erase(Key,Locks);
        {ok,#lock{funs=Funs}=Lock} ->
            dict:store(Key,Lock#lock{funs=[Fun|Funs]},Locks)
    end.

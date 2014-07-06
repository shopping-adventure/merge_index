%% -------------------------------------------------------------------
%%
%% mi_scheduler: makes segment compaction single threaded.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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

-module(mi_scheduler).

%% API
-export([
    start_link/0,
    start/0,
    schedule_compaction/1,
    ms_before_replace/2,
    new_timering/1,
    replace_oldest/1
]).
%% Private export
-export([worker_loop/1]).

-include("merge_index.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { queue,
                 ready,
                 worker }).

%% ====================================================================
%% API
%% ====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

schedule_compaction(Pid) ->
    gen_server:call(?MODULE, {schedule_compaction, Pid}, infinity).


%% ====================================================================
%% gen_server callbacks
%% ====================================================================

init([]) ->
    %% Trap exits of the actual worker process
    process_flag(trap_exit, true),

    %% Use a dedicated worker sub-process to do the actual merging. The
    %% process may ignore messages for a long while during the compaction
    %% and we want to ensure that our message queue doesn't fill up with
    %% a bunch of dup requests for the same directory.
    Self = self(),
    WorkerPid = spawn_link(fun() -> worker_loop(worker_init_state(Self)) end),
    {ok, #state{ queue = queue:new(),
                 worker = WorkerPid,
                 ready = true}}.

handle_call({schedule_compaction, Pid}, _From, #state { ready = true, worker = WorkerPid } = State) ->
     WorkerPid ! {compaction, Pid},
     {reply, ok, State#state {ready = false}};
        
handle_call({schedule_compaction, Pid}, _From, #state { ready = false, queue = Q } = State) ->
    case queue:member(Pid, Q) of
        true ->
            {reply, already_queued, State};
        false ->
            NewState = State#state { queue = queue:in(Pid, Q) },
            {reply, ok, NewState}
    end;

handle_call(Event, _From, State) ->
    lager:error("unhandled_call ~p", [Event]),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    lager:error("unhandled_cast ~p", [Msg]),
    {noreply, State}.

handle_info({worker_ready, WorkerPid}, #state { queue = Q } = State) ->
    case queue:out(Q) of
        {empty, Q} ->
            {noreply, State#state{ ready = true }};
        {{value, Pid}, NewQ} ->
            WorkerPid ! {compaction, Pid},
            NewState = State#state { queue=NewQ , ready = false },
            {noreply, NewState}
    end;
handle_info({'EXIT', WorkerPid, Reason}, #state { worker = WorkerPid } = State) ->
    lager:error("Compaction worker ~p exited: ~p", [WorkerPid, Reason]),
    %% Start a new worker.
    Self = self(),
    NewWorkerPid = spawn_link(fun() -> worker_loop(worker_init_state(Self)) end),
    NewState = State#state { worker=NewWorkerPid , ready = true},
    {noreply, NewState};

handle_info(Info, State) ->
    lager:error("unhandled_info ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal worker
%% ====================================================================
-define(TIMERING_SIZE, 15).
-define(TIMERING_SPAN_INIT, 3).

-record(wstate, { parent,
                 timering,
                 timering_span,
                 test_start,
                 test_compactions}).

%% THROTTLING: Run M processes in N seconds waiting ms_before_replace(Ring,N) before doing something
%% and replace_oldest (impl is a set of size M of timestamps representing running processes.
%% Replace the oldest one if older than N seconds else wait (oldest_ts-Nsec) : {Ts_Set, Oldest_Idx}
new_timering(M)->{erlang:make_tuple(M,now()),1}.
replace_oldest({Set,Idx})->
   {erlang:setelement(Idx,Set,now()),case Idx+1 of X when X>erlang:size(Set) ->1; X->X end}.
ms_before_replace({Set,Idx},N)->
   max(0,timer:seconds(N) - trunc(timer:now_diff(now(),element(Idx,Set))/1000)).

worker_init_state(Parent)->
    #wstate{parent=Parent, timering=new_timering(?TIMERING_SIZE),
        timering_span=?TIMERING_SPAN_INIT,test_start=now(),test_compactions=[]}.

worker_loop(#wstate{timering_span=TimeRingSpan,test_start=TestStart,
                   test_compactions=TestCompactions}=State) when length(TestCompactions)==?TIMERING_SIZE ->
    {ok,WantedThroughput} = application:get_env(merge_index, compaction_throughput_mb_per_sec),
    TestBytes = lists:sum([OldBytes || {ok, _, OldBytes}<-TestCompactions]),
    TestSegments = lists:sum([OldSegments || {ok, OldSegments, _}<-TestCompactions]),
    TestElapsedSecs = timer:now_diff(os:timestamp(), TestStart) / 1000000,
    Throughput = TestBytes/TestElapsedSecs/(1024*1024),
    lager:info("Overall Compaction: ~p segments for ~p bytes in ~p seconds, ~.2f MB/sec",
               [TestSegments, TestBytes, TestElapsedSecs, Throughput]),
    AcceptableDiff = WantedThroughput*0.2,
    case abs(Throughput-WantedThroughput) of
        Diff when Diff < AcceptableDiff -> 
            worker_loop(State#wstate{timering_span=TimeRingSpan,test_start=now(),test_compactions=[]});
        _ -> %% We need to adjust timering span window in order to have the good throughput
            NewTimeRingSpan = trunc(TimeRingSpan * (Throughput/WantedThroughput)) + 1,
            lager:info("Adjust throttling to have ~p compaction every ~p seconds",[?TIMERING_SIZE,NewTimeRingSpan]),
            worker_loop(State#wstate{timering_span=NewTimeRingSpan,test_start=now(),test_compactions=[]})
    end;

worker_loop(#wstate{parent=Parent,timering=TimeRing,
                    timering_span=TimeRingSpan, test_compactions=TestCompactions}=State) ->
    Worker = self(),
    receive
        {compaction_res,Result}->
            ?MODULE:worker_loop(State#wstate{test_compactions=[Result|TestCompactions]});
        {compaction, Pid} ->
            spawn_link(fun()->
                Start = os:timestamp(),
                Result = merge_index:compact(Pid),
                case Result of
                    {ok, 0, 0}->ok;
                    {ok, OldSegments, OldBytes} ->
                        Worker ! {compaction_res,Result},
                        ElapsedSecs = timer:now_diff(os:timestamp(), Start) / 1000000,
                        lager:debug(
                          "Single Compaction ~p: ~p segments for ~p bytes in ~p seconds, ~.2f MB/sec",
                          [Pid, OldSegments, OldBytes, ElapsedSecs, OldBytes/ElapsedSecs/(1024*1024)]);
                    {Error, Reason} when Error == error; Error == 'EXIT' ->
                        lager:error("Failed to compact ~p: ~p", [Pid, Reason])
                end
            end),
            erlang:send_after(ms_before_replace(TimeRing,TimeRingSpan),Parent,{worker_ready,Worker}),
            ?MODULE:worker_loop(State#wstate{timering=replace_oldest(TimeRing)});
        _ ->
            %% ignore unknown messages
            ?MODULE:worker_loop(State)
    end.

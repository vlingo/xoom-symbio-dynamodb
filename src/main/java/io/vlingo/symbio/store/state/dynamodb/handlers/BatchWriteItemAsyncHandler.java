// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.handlers;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.state.StateStore;

import java.util.List;
import java.util.function.Function;

public class BatchWriteItemAsyncHandler<S,RS extends State<?>,C> implements AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult> {
    private final String id;
    private final S state;
    private final int stateVersion;
    private final StateStore.WriteResultInterest interest;
    private final Dispatchable<Entry<?>, RS> dispatchable;
    //private final StateStore.Dispatcher dispatcher;
    private final Object object;
    private final Function<Dispatchable<Entry<?>, RS>, Void> dispatchState;
    private final List<Source<C>> sources;

    public BatchWriteItemAsyncHandler(String id, S state, int stateVersion, final List<Source<C>> sources, StateStore.WriteResultInterest interest, final Object object, Dispatchable<Entry<?>, RS> dispatchable, Dispatcher<Dispatchable<Entry<?>, RS>> dispatcher, Function<Dispatchable<Entry<?>, RS>, Void> dispatchState) {
        this.id = id;
        this.state = state;
        this.stateVersion = stateVersion;
        this.sources = sources;
        this.interest = interest;
        this.object = object;
        this.dispatchable = dispatchable;
        //this.dispatcher = dispatcher;
        this.dispatchState = dispatchState;
    }

    @Override
    public void onError(Exception e) {
        interest.writeResultedIn(Failure.of(new StorageException(Result.NoTypeStore, e.getMessage(), e)), id, state, stateVersion, Source.none(), object);
    }

    @Override
    public void onSuccess(BatchWriteItemRequest request, BatchWriteItemResult batchWriteItemResult) {
        interest.writeResultedIn(Success.of(Result.Success), id, state, stateVersion, sources, object);
        dispatchState.apply(dispatchable);
        // TODO: Must know binary/text type to dispatch, but this is generic class
        //dispatcher.dispatch(state.id, state);
    }
}

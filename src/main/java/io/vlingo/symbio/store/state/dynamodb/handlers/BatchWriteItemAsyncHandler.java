// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.handlers;

import java.util.function.Function;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;

import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore;

public class BatchWriteItemAsyncHandler<S,RS extends State<?>> implements AsyncHandler<BatchWriteItemRequest, BatchWriteItemResult> {
    private final String id;
    private final S state;
    private final int stateVersion;
    private final StateStore.WriteResultInterest interest;
    private final StateStore.Dispatchable<RS> dispatchable;
    //private final StateStore.Dispatcher dispatcher;
    private final Object object;
    private final Function<StateStore.Dispatchable<RS>, Void> dispatchState;

    public BatchWriteItemAsyncHandler(String id, S state, int stateVersion, StateStore.WriteResultInterest interest, final Object object, StateStore.Dispatchable<RS> dispatchable, StateStore.Dispatcher dispatcher, Function<StateStore.Dispatchable<RS>, Void> dispatchState) {
        this.id = id;
        this.state = state;
        this.stateVersion = stateVersion;
        this.interest = interest;
        this.object = object;
        this.dispatchable = dispatchable;
        //this.dispatcher = dispatcher;
        this.dispatchState = dispatchState;
    }

    @Override
    public void onError(Exception e) {
        interest.writeResultedIn(Failure.of(new StorageException(Result.NoTypeStore, e.getMessage(), e)), id, state, stateVersion, object);
    }

    @Override
    public void onSuccess(BatchWriteItemRequest request, BatchWriteItemResult batchWriteItemResult) {
        interest.writeResultedIn(Success.of(Result.Success), id, state, stateVersion, object);
        dispatchState.apply(dispatchable);
        // TODO: Must know binary/text type to dispatch, but this is generic class
        //dispatcher.dispatch(state.id, state);
    }
}

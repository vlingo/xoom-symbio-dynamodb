// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.handlers;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import io.vlingo.symbio.store.state.StateStore;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DispatchAsyncHandler<T> implements AsyncHandler<ScanRequest, ScanResult> {
    private final Function<Map<String, AttributeValue>, StateStore.Dispatchable<T>> unmarshaller;
    private final Function<StateStore.Dispatchable<T>, Void> dispatchState;

    public DispatchAsyncHandler(Function<Map<String, AttributeValue>, StateStore.Dispatchable<T>> unmarshaller, Function<StateStore.Dispatchable<T>, Void> dispatchState) {
        this.unmarshaller = unmarshaller;
        this.dispatchState = dispatchState;
    }

    @Override
    public void onError(Exception e) {

    }

    @Override
    public void onSuccess(ScanRequest request, ScanResult scanResult) {
        List<Map<String, AttributeValue>> items = scanResult.getItems();
        for (Map<String, AttributeValue> item : items) {
            StateStore.Dispatchable<T> dispatchable = unmarshaller.apply(item);
            dispatchState.apply(dispatchable);
//            dispatcher.dispatch(dispatchable.id, dispatchableToState.apply(dispatchable));
        }
    }
}

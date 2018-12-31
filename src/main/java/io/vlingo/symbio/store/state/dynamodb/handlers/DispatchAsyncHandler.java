// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.handlers;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.StateStore;

public class DispatchAsyncHandler<RS extends State<?>> implements AsyncHandler<ScanRequest, ScanResult> {
    private final Function<Map<String, AttributeValue>, StateStore.Dispatchable<RS>> unmarshaller;
    private final Function<StateStore.Dispatchable<RS>, Void> dispatchState;

    public DispatchAsyncHandler(Function<Map<String, AttributeValue>, StateStore.Dispatchable<RS>> unmarshaller, Function<StateStore.Dispatchable<RS>, Void> dispatchState) {
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
            StateStore.Dispatchable<RS> dispatchable = unmarshaller.apply(item);
            dispatchState.apply(dispatchable);
//            dispatcher.dispatch(dispatchable.id, dispatchableToState.apply(dispatchable));
        }
    }
}

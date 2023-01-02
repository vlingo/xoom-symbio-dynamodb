// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.dynamodb.handlers;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class DispatchAsyncHandler<RS extends State<?>> implements AsyncHandler<ScanRequest, ScanResult> {
    private final Function<Map<String, AttributeValue>, Dispatchable<Entry<?>, RS>> unmarshaller;
    private final Function<Dispatchable<Entry<?>, RS>, Void> dispatchState;

    public DispatchAsyncHandler(Function<Map<String, AttributeValue>, Dispatchable<Entry<?>, RS>> unmarshaller, Function<Dispatchable<Entry<?>, RS>, Void> dispatchState) {
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
            Dispatchable<Entry<?>, RS> dispatchable = unmarshaller.apply(item);
            dispatchState.apply(dispatchable);
//            dispatcher.dispatch(dispatchable.id, dispatchableToState.apply(dispatchable));
        }
    }
}

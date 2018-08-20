// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.handlers;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.StateStore;

import java.util.Map;
import java.util.function.Function;

public class GetEntityAsyncHandler<T> implements AsyncHandler<GetItemRequest, GetItemResult> {
    private final String id;
    private final StateStore.ReadResultInterest<T> interest;
    private final Object object;
    private final State<T> nullState;
    private final Function<Map<String, AttributeValue>, State<T>> unmarshaller;

    public GetEntityAsyncHandler(String id, StateStore.ReadResultInterest<T> interest, final Object object, State<T> nullState, Function<Map<String, AttributeValue>, State<T>> unmarshaller) {
        this.id = id;
        this.interest = interest;
        this.object = object;
        this.nullState = nullState;
        this.unmarshaller = unmarshaller;
    }

    @Override
    public void onError(Exception e) {
        interest.readResultedIn(StateStore.Result.NoTypeStore, new IllegalStateException(e), id, nullState, null);
    }

    @Override
    public void onSuccess(GetItemRequest request, GetItemResult getItemResult) {
        Map<String, AttributeValue> item = getItemResult.getItem();
        if (item == null) {
            interest.readResultedIn(StateStore.Result.NotFound, id, nullState, null);
            return;
        }

        try {
            State<T> state = unmarshaller.apply(item);
            interest.readResultedIn(StateStore.Result.Success, id, state, object);
        } catch (Exception e) {
            interest.readResultedIn(StateStore.Result.Failure, e, id, nullState, object);
        }
    }
}

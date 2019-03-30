// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.handlers;

import java.util.Map;
import java.util.function.Function;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;

import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.StateStore;

public class GetEntityAsyncHandler<S, RS extends State<?>> implements AsyncHandler<GetItemRequest, GetItemResult> {
    private final String id;
    private final StateStore.ReadResultInterest interest;
    private final Object object;
    final StateAdapterProvider stateAdapterProvider;
    private final Function<Map<String, AttributeValue>, RS> unmarshaller;

    public GetEntityAsyncHandler(String id, StateStore.ReadResultInterest interest, final Object object, Function<Map<String, AttributeValue>, RS> unmarshaller, final StateAdapterProvider stateAdapterProvider) {
        this.id = id;
        this.interest = interest;
        this.object = object;
        this.unmarshaller = unmarshaller;
        this.stateAdapterProvider = stateAdapterProvider;
    }

    @Override
    public void onError(Exception e) {
        interest.readResultedIn(Failure.of(new StorageException(Result.NoTypeStore, e.getMessage(), e)), id, null, -1, null, object);
    }

    @Override
    public void onSuccess(GetItemRequest request, GetItemResult getItemResult) {
        Map<String, AttributeValue> item = getItemResult.getItem();
        if (item == null) {
            interest.readResultedIn(Failure.of(new StorageException(Result.NotFound, "Iteam not found for: " + id)), id, null, -1, null, object);
            return;
        }

        try {
            RS raw = unmarshaller.apply(item);
            S state = stateAdapterProvider.fromRaw(raw);
            interest.readResultedIn(Success.of(Result.Success), id, state, raw.dataVersion, raw.metadata, object);
        } catch (Exception e) {
            interest.readResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), id, null, -1, null, object);
        }
    }
}

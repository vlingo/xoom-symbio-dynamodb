// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.handlers;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.dispatch.ConfirmDispatchedResultInterest;

public class ConfirmDispatchableAsyncHandler implements AsyncHandler<DeleteItemRequest, DeleteItemResult> {
    private final String dispatchId;
    private final ConfirmDispatchedResultInterest interest;

    public ConfirmDispatchableAsyncHandler(String dispatchId, ConfirmDispatchedResultInterest interest) {
        this.dispatchId = dispatchId;
        this.interest = interest;
    }

    @Override
    public void onError(Exception e) {
        interest.confirmDispatchedResultedIn(Result.Failure, dispatchId);
    }

    @Override
    public void onSuccess(DeleteItemRequest request, DeleteItemResult deleteItemResult) {
        interest.confirmDispatchedResultedIn(Result.Success, dispatchId);
    }
}

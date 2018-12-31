// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.adapters;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.StateStore;

import java.util.Map;

public interface RecordAdapter<RS extends State<?>> {
    Map<String, AttributeValue> marshallState(RS state);

    Map<String, AttributeValue> marshallDispatchable(StateStore.Dispatchable<RS> dispatchable);

    Map<String, AttributeValue> marshallForQuery(String id);

    RS unmarshallState(Map<String, AttributeValue> record);

    StateStore.Dispatchable<RS> unmarshallDispatchable(Map<String, AttributeValue> item);
}

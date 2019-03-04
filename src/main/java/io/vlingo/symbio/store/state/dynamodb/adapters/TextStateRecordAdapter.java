// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb.adapters;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.state.StateStore;

public final class TextStateRecordAdapter implements RecordAdapter<TextState> {
    private static final String ID_FIELD = "Id";
    private static final String CREATED_AT_FIELD = "CreatedAt";
    private static final String STATE_FIELD = "State";
    private static final String DATA_FIELD = "Data";
    private static final String TYPE_FIELD = "Type";
    private static final String METADATA_FIELD = "Metadata";
    private static final String TYPE_VERSION_FIELD = "TypeVersion";
    private static final String DATA_VERSION_FIELD = "DataVersion";

    @Override
    public Map<String, AttributeValue> marshallState(TextState state) {
        String metadataAsJson = JsonSerialization.serialized(state.metadata);

        Map<String, AttributeValue> stateItem = new HashMap<>();
        stateItem.put(ID_FIELD, new AttributeValue().withS(state.id));
        stateItem.put(DATA_FIELD, new AttributeValue().withS(state.data));
        stateItem.put(TYPE_FIELD, new AttributeValue().withS(state.type));
        stateItem.put(METADATA_FIELD, new AttributeValue().withS(metadataAsJson));
        stateItem.put(TYPE_VERSION_FIELD, new AttributeValue().withN(String.valueOf(state.typeVersion)));
        stateItem.put(DATA_VERSION_FIELD, new AttributeValue().withN(String.valueOf(state.dataVersion)));

        return stateItem;
    }

    @Override
    public Map<String, AttributeValue> marshallDispatchable(StateStore.Dispatchable<TextState> dispatchable) {
        Map<String, AttributeValue> stateItem = new HashMap<>();
        stateItem.put(ID_FIELD, new AttributeValue().withS(dispatchable.id));
        stateItem.put(CREATED_AT_FIELD, new AttributeValue().withS(dispatchable.createdAt.toString()));
        stateItem.put(STATE_FIELD, new AttributeValue().withS(JsonSerialization.serialized(dispatchable.state)));

        return stateItem;
    }

    @Override
    public Map<String, AttributeValue> marshallForQuery(String id) {
        Map<String, AttributeValue> stateItem = new HashMap<>();
        stateItem.put(ID_FIELD, new AttributeValue().withS(id));

        return stateItem;
    }

    @Override
    public TextState unmarshallState(Map<String, AttributeValue> record) {
        try {
            return new State.TextState(
                    record.get(ID_FIELD).getS(),
                    Class.forName(record.get(TYPE_FIELD).getS()),
                    Integer.valueOf(record.get(TYPE_VERSION_FIELD).getN()),
                    record.get(DATA_FIELD).getS(),
                    Integer.valueOf(record.get(DATA_VERSION_FIELD).getN()),
                    JsonSerialization.deserialized(record.get(METADATA_FIELD).getS(), Metadata.class)
            );
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public StateStore.Dispatchable<TextState> unmarshallDispatchable(Map<String, AttributeValue> item) {
        String id = item.get(ID_FIELD).getS();
        LocalDateTime createdAt = LocalDateTime.parse(item.get(CREATED_AT_FIELD).getS());
        String json = item.get(STATE_FIELD).getS();

        return new StateStore.Dispatchable<>(id, createdAt, JsonSerialization.deserialized(json, State.TextState.class));
    }
}

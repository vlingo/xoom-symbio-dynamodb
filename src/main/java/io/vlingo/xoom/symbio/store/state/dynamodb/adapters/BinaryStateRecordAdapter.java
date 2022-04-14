// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.dynamodb.adapters;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import io.vlingo.xoom.common.serialization.JsonSerialization;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.State.BinaryState;
import io.vlingo.xoom.symbio.store.StoredTypes;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class BinaryStateRecordAdapter implements RecordAdapter<BinaryState> {
    private static final String ID_FIELD = "Id";
    private static final String CREATED_AT_FIELD = "CreatedAt";
    private static final String STATE_FIELD = "State";
    private static final String DATA_FIELD = "Data";
    private static final String TYPE_FIELD = "Type";
    private static final String METADATA_FIELD = "Metadata";
    private static final String TYPE_VERSION_FIELD = "TypeVersion";
    private static final String DATA_VERSION_FIELD = "DataVersion";

    @Override
    public Map<String, AttributeValue> marshallState(BinaryState state) {
        String metadataAsJson = JsonSerialization.serialized(state.metadata);

        Map<String, AttributeValue> stateItem = new HashMap<>();
        stateItem.put(ID_FIELD, new AttributeValue().withS(state.id));
        stateItem.put(DATA_FIELD, new AttributeValue().withB(ByteBuffer.wrap(state.data)));
        stateItem.put(TYPE_FIELD, new AttributeValue().withS(state.type));
        stateItem.put(METADATA_FIELD, new AttributeValue().withS(metadataAsJson));
        stateItem.put(TYPE_VERSION_FIELD, new AttributeValue().withN(String.valueOf(state.typeVersion)));
        stateItem.put(DATA_VERSION_FIELD, new AttributeValue().withN(String.valueOf(state.dataVersion)));

        return stateItem;
    }

    @Override
    public Map<String, AttributeValue> marshallDispatchable(Dispatchable<Entry<?>, BinaryState> dispatchable) {
        Map<String, AttributeValue> stateItem = new HashMap<>();
        stateItem.put(ID_FIELD, new AttributeValue().withS(dispatchable.id()));
        stateItem.put(CREATED_AT_FIELD, new AttributeValue().withS(dispatchable.createdOn().toString()));
        if (dispatchable.state().isPresent()) {
            stateItem.put(STATE_FIELD, new AttributeValue().withS(JsonSerialization.serialized(dispatchable.state().get())));
        }
        //TODO add entities
        return stateItem;
    }

    @Override
    public Map<String, AttributeValue> marshallForQuery(String id) {
        Map<String, AttributeValue> stateItem = new HashMap<>();
        stateItem.put(ID_FIELD, new AttributeValue().withS(id));

        return stateItem;
    }

    @Override
    public BinaryState unmarshallState(Map<String, AttributeValue> record) {
        try {
            return new State.BinaryState(
                    record.get(ID_FIELD).getS(),
                    StoredTypes.forName(record.get(TYPE_FIELD).getS()),
                    Integer.valueOf(record.get(TYPE_VERSION_FIELD).getN()),
                    record.get(DATA_FIELD).getB().array(),
                    Integer.valueOf(record.get(DATA_VERSION_FIELD).getN()),
                    JsonSerialization.deserialized(record.get(METADATA_FIELD).getS(), Metadata.class)
            );
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Dispatchable<Entry<?>, BinaryState> unmarshallDispatchable(Map<String, AttributeValue> item) {
        String id = item.get(ID_FIELD).getS();
        LocalDateTime createdAt = LocalDateTime.parse(item.get(CREATED_AT_FIELD).getS());
        String json = item.get(STATE_FIELD).getS();
        //TODO get entries
        return new Dispatchable<>(id, createdAt, JsonSerialization.deserialized(json, State.BinaryState.class), Collections.emptyList());
    }
}

// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.UUID;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

import io.vlingo.actors.Definition;
import io.vlingo.actors.Protocols;
import io.vlingo.actors.World;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.BinaryState;
import io.vlingo.symbio.store.state.BinaryStateStore;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.dynamodb.adapters.BinaryStateRecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public class DynamoDBBinaryStateActorTest extends DynamoDBStateActorTest<BinaryStateStore, BinaryState> {
    @Override
    protected Protocols stateStoreProtocols(World world, StateStore.Dispatcher dispatcher, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest) {
        return world.actorFor(
                Definition.has(DynamoDBBinaryStateActor.class, Definition.parameters(dispatcher, dynamodb, interest)),
                new Class[]{BinaryStateStore.class, StateStore.DispatcherControl.class}
        );
    }

    @Override
    protected void doWrite(BinaryStateStore actor, BinaryState state, StateStore.WriteResultInterest<BinaryState> interest) {
        actor.write(state, interest);
    }

    @Override
    protected void doRead(BinaryStateStore actor, String id, Class<?> type, StateStore.ReadResultInterest<BinaryState> interest) {
        actor.read(id, type, interest);
    }

    @Override
    protected BinaryState nullState() {
        return BinaryState.Null;
    }

    @Override
    protected BinaryState randomState() {
        return new State.BinaryState(
                UUID.randomUUID().toString(),
                Entity1.class,
                1,
                UUID.randomUUID().toString().getBytes(),
                1,
                new Metadata(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        );
    }

    @Override
    protected BinaryState newFor(BinaryState oldState) {
        return new State.BinaryState(
                oldState.id,
                oldState.typed(),
                oldState.typeVersion,
                oldState.data,
                oldState.dataVersion + 1,
                oldState.metadata
        );
    }

    @Override
    protected void verifyDispatched(StateStore.Dispatcher dispatcher, String id, StateStore.Dispatchable<BinaryState> dispatchable) {
        verify(dispatcher).dispatch(dispatchable.id, dispatchable.state.asBinaryState());
    }

    @Override
    protected void verifyDispatched(StateStore.Dispatcher dispatcher, String id, BinaryState state) {
        verify(dispatcher, timeout(DEFAULT_TIMEOUT)).dispatch(id, state.asBinaryState());
    }

    @Override
    protected RecordAdapter<BinaryState> recordAdapter() {
        return new BinaryStateRecordAdapter();
    }
}
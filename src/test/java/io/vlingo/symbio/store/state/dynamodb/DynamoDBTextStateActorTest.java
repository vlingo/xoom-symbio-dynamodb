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
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.TextStateStore;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.adapters.TextStateRecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public class DynamoDBTextStateActorTest extends DynamoDBStateActorTest<TextStateStore, TextState> {
    @Override
    protected Protocols stateStoreProtocols(World world, StateStore.Dispatcher dispatcher, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest) {
        return world.actorFor(
                Definition.has(DynamoDBTextStateActor.class, Definition.parameters(dispatcher, dynamodb, interest)),
                new Class[]{TextStateStore.class, StateStore.DispatcherControl.class}
        );
    }

    @Override
    protected void doWrite(TextStateStore actor, TextState state, StateStore.WriteResultInterest<TextState> interest) {
        actor.write(state, interest);
    }

    @Override
    protected void doRead(TextStateStore actor, String id, Class<?> type, StateStore.ReadResultInterest<TextState> interest) {
        actor.read(id, type, interest);
    }

    @Override
    protected TextState nullState() {
        return TextState.Null;
    }

    @Override
    protected TextState randomState() {
        return new State.TextState(
                UUID.randomUUID().toString(),
                Entity1.class,
                1,
                UUID.randomUUID().toString(),
                1,
                new Metadata(UUID.randomUUID().toString(), UUID.randomUUID().toString())
        );
    }

    @Override
    protected TextState newFor(TextState oldState) {
        return new State.TextState(
                oldState.id,
                oldState.typed(),
                oldState.typeVersion,
                oldState.data,
                oldState.dataVersion + 1,
                oldState.metadata
        );
    }

    @Override
    protected void verifyDispatched(StateStore.Dispatcher dispatcher, String id, StateStore.Dispatchable<TextState> dispatchable) {
        verify(dispatcher).dispatch(dispatchable.id, dispatchable.state.asTextState());
    }

    @Override
    protected void verifyDispatched(StateStore.Dispatcher dispatcher, String id, TextState state) {
        verify(dispatcher, timeout(DEFAULT_TIMEOUT)).dispatch(id, state.asTextState());
    }

    @Override
    protected RecordAdapter<TextState> recordAdapter() {
        return new TextStateRecordAdapter();
    }
}
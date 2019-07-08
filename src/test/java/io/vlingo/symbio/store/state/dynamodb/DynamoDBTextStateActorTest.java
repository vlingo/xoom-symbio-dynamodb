// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.adapters.TextStateRecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;
import org.junit.Before;

import java.time.LocalDateTime;
import java.util.Collections;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

public class DynamoDBTextStateActorTest extends DynamoDBStateActorTest<TextState> {

    @Override
    @Before
    public void setUp() {
      isBinaryTest = false;

      super.setUp();

      /*
       * NOTE: dispatcherControl is only created here and passed to stateStoreProtocol
       * so that this test case can directly interact with dispatcherControl. Normally
       * the DynamoDBStateActor provisions its own DispatcherControl (an instance of
       * DynamoDBDispatcherControlActor).
       */
      dispatcherControl = world.actorFor(
        DispatcherControl.class,
        Definition.has(
          DynamoDBDispatcherControlActor.class,
          Definition.parameters(dispatcher, dynamodb, new TextStateRecordAdapter(), 1000L, 1000L)));

      stateStore = stateStoreProtocol(world, dispatcher, dispatcherControl, dynamodb, createTableInterest);
    }

    @Override
    protected StateStore stateStoreProtocol(World world, Dispatcher<Dispatchable<Entry<?>, TextState>> dispatcher, DispatcherControl dispatcherControl, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest) {
      return world.actorFor(
        StateStore.class,
        Definition.has(DynamoDBStateActor.class, Definition.parameters(dispatcher, dispatcherControl, dynamodb, interest, new TextStateRecordAdapter()))
      );
    }

    @Override
    protected void verifyDispatched(Dispatcher<Dispatchable<Entry<?>, TextState>> dispatcher, String id, Dispatchable<Entry<?>,TextState> dispatchable) {
      verify(dispatcher).dispatch(dispatchable);
    }

    @Override
    protected void verifyDispatched(Dispatcher<Dispatchable<Entry<?>, TextState>> dispatcher, String id, TextState state) {
      verify(dispatcher, timeout(DEFAULT_TIMEOUT)).dispatch(new Dispatchable<>(id, LocalDateTime.now(), state, Collections.emptyList()));
    }

    @Override
    protected RecordAdapter<TextState> recordAdapter() {
        return new TextStateRecordAdapter();
    }
}
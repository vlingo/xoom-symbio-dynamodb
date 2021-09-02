// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.dynamodb;

import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Ignore;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;

import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State.TextState;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.dynamodb.DynamoDBDispatcherControlActor.DynamoDBDispatcherControlInstantiator;
import io.vlingo.xoom.symbio.store.state.dynamodb.DynamoDBStateActor.DynamoDBStateStoreInstantiator;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.TextStateRecordAdapter;
import io.vlingo.xoom.symbio.store.state.dynamodb.interests.CreateTableInterest;
import org.mockito.internal.verification.AtLeast;
import org.mockito.verification.VerificationMode;

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
          new DynamoDBDispatcherControlInstantiator<>(dispatcher, dynamodb, new TextStateRecordAdapter(), 1000L, 1000L)));

      stateStore = stateStoreProtocol(world, dispatcher, dispatcherControl, dynamodb, createTableInterest);
    }

    @Override
    protected StateStore stateStoreProtocol(World world, Dispatcher<Dispatchable<Entry<?>, TextState>> dispatcher, DispatcherControl dispatcherControl, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest) {
      return world.actorFor(
        StateStore.class,
        Definition.has(
                DynamoDBStateActor.class,
                new DynamoDBStateStoreInstantiator<>(Arrays.asList(dispatcher), dispatcherControl, dynamodb, interest, new TextStateRecordAdapter()))
      );
    }

    @Override
//    protected void verifyDispatched(List<Dispatcher<Dispatchable<Entry<?>, TextState>>> dispatchers, String id, Dispatchable<Entry<?>,TextState> dispatchable) {
//      verify(dispatchers).dispatch(dispatchable);
//    }
    protected void verifyDispatched(Dispatcher<Dispatchable<Entry<?>, TextState>> dispatcher, String id, Dispatchable<Entry<?>,TextState> dispatchable) {
      verify(dispatcher, new AtLeast(1)).dispatch(dispatchable);
    }

    @Override
//    protected void verifyDispatched(List<Dispatcher<Dispatchable<Entry<?>, TextState>>> dispatchers, String id, TextState state) {
//      verify(dispatchers, timeout(DEFAULT_TIMEOUT)).dispatch(new Dispatchable<>(id, LocalDateTime.now(), state, Collections.emptyList()));
//    }
    protected void verifyDispatched(Dispatcher<Dispatchable<Entry<?>, TextState>> dispatcher, String id, TextState state) {
      verify(dispatcher, timeout(DEFAULT_TIMEOUT).atLeast(1)).dispatch(new Dispatchable<>(id, LocalDateTime.now(), state, Collections.emptyList()));
    }

    @Override
    protected RecordAdapter<TextState> recordAdapter() {
        return new TextStateRecordAdapter();
    }
}
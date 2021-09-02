// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.state.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.common.Cancellable;
import io.vlingo.xoom.common.Scheduled;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.dispatch.ConfirmDispatchedResultInterest;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.xoom.symbio.store.state.dynamodb.handlers.ConfirmDispatchableAsyncHandler;
import io.vlingo.xoom.symbio.store.state.dynamodb.handlers.DispatchAsyncHandler;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
/**
 * DynamoDBDispatcherControlActor is responsible for ensuring that
 * dispatching of {@link Dispatchable dispatchables} occurs and
 * is confirmed.
 */
public class DynamoDBDispatcherControlActor<RS extends State<?>>  extends Actor
implements DispatcherControl,Scheduled<Object> {

  public final static long DEFAULT_REDISPATCH_DELAY = 2000L;

  private final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers;
  private final AmazonDynamoDBAsync dynamodb;
  private final RecordAdapter<RS> recordAdapter;
  private final long confirmationExpiration;
  private final Cancellable cancellable;

  @SuppressWarnings("unchecked")
  public DynamoDBDispatcherControlActor(
    final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers,
    final AmazonDynamoDBAsync dynamodb,
    final RecordAdapter<RS> recordAdapter,
    final long checkConfirmationExpirationInterval,
    final long confirmationExpiration)
  {
    super();
    this.dispatchers = dispatchers;
    this.dynamodb = dynamodb;
    this.recordAdapter = recordAdapter;
    this.confirmationExpiration = confirmationExpiration;
    this.cancellable = scheduler().schedule(
      selfAs(Scheduled.class),
      null,
      DEFAULT_REDISPATCH_DELAY,
      checkConfirmationExpirationInterval);
  }

  @Override
  public void intervalSignal(Scheduled<Object> scheduled, Object data) {
    dispatchUnconfirmed();
  }

  @Override
  public void confirmDispatched(String dispatchId, ConfirmDispatchedResultInterest interest) {
    dynamodb.deleteItemAsync(
      new DeleteItemRequest(
        DynamoDBStateActor.DISPATCHABLE_TABLE_NAME,
        recordAdapter.marshallForQuery(dispatchId)),
      new ConfirmDispatchableAsyncHandler(dispatchId, interest)
    );
  }

  @Override
  public void dispatchUnconfirmed() {
    dynamodb.scanAsync(
      new ScanRequest(DynamoDBStateActor.DISPATCHABLE_TABLE_NAME).withLimit(100),
            new DispatchAsyncHandler<>(recordAdapter::unmarshallDispatchable, this::doDispatch)
    );
  }

  private Void doDispatch(Dispatchable<Entry<?>, RS> dispatchable) {
    Duration duration = Duration.between(dispatchable.createdOn(), LocalDateTime.now());
    if (Math.abs(duration.toMillis()) > confirmationExpiration) {
      dispatchers.forEach(d -> d.dispatch(dispatchable));
    }
    return null;
  }

  @Override
  public void stop() {
    if (cancellable != null)
      cancellable.cancel();
    super.stop();
  }

  public static class DynamoDBDispatcherControlInstantiator<RS extends State<?>> implements ActorInstantiator<DynamoDBDispatcherControlActor<RS>> {
    private static final long serialVersionUID = 7698155745427728286L;

    private final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers;
    private final AmazonDynamoDBAsync dynamodb;
    private final RecordAdapter<RS> recordAdapter;
    private final long checkConfirmationExpirationInterval;
    private final long confirmationExpiration;

    public DynamoDBDispatcherControlInstantiator(
            final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers,
            final AmazonDynamoDBAsync dynamodb,
            final RecordAdapter<RS> recordAdapter,
            final long checkConfirmationExpirationInterval,
            final long confirmationExpiration) {
      this.dispatchers = dispatchers;
      this.dynamodb = dynamodb;
      this.recordAdapter = recordAdapter;
      this.checkConfirmationExpirationInterval = checkConfirmationExpirationInterval;
      this.confirmationExpiration = confirmationExpiration;
    }

    public DynamoDBDispatcherControlInstantiator(
            final Dispatcher<Dispatchable<Entry<?>, RS>> dispatcher,
            final AmazonDynamoDBAsync dynamodb,
            final RecordAdapter<RS> recordAdapter,
            final long checkConfirmationExpirationInterval,
            final long confirmationExpiration) {
      this(Collections.singletonList(dispatcher), dynamodb, recordAdapter, checkConfirmationExpirationInterval, confirmationExpiration);
    }

    @Override
    public DynamoDBDispatcherControlActor<RS> instantiate() {
      return new DynamoDBDispatcherControlActor<>(
              dispatchers,
              dynamodb,
              recordAdapter,
              checkConfirmationExpirationInterval,
              confirmationExpiration);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Class<DynamoDBDispatcherControlActor<RS>> type() {
      return (Class) DynamoDBDispatcherControlActor.class;
    }
  }
}

// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.state.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;

import java.time.Duration;
import java.time.LocalDateTime;

import io.vlingo.actors.Actor;
import io.vlingo.common.Cancellable;
import io.vlingo.common.Scheduled;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStore.ConfirmDispatchedResultInterest;
import io.vlingo.symbio.store.state.StateStore.Dispatchable;
import io.vlingo.symbio.store.state.StateStore.Dispatcher;
import io.vlingo.symbio.store.state.StateStore.DispatcherControl;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.handlers.ConfirmDispatchableAsyncHandler;
import io.vlingo.symbio.store.state.dynamodb.handlers.DispatchAsyncHandler;
/**
 * DynamoDBDispatcherControlActor is responsible for ensuring that
 * dispatching of {@link Dispatchable dispatchables} occurs and
 * is confirmed.
 */
public class DynamoDBDispatcherControlActor<RS extends State<?>>  extends Actor
implements DispatcherControl,Scheduled<Object> {
  
  public final static long DEFAULT_REDISPATCH_DELAY = 2000L;
  
  private final StateStore.Dispatcher dispatcher;
  private final AmazonDynamoDBAsync dynamodb;
  private final RecordAdapter<RS> recordAdapter;
  private final long confirmationExpiration;
  private final Cancellable cancellable;
  
  @SuppressWarnings("unchecked")
  public DynamoDBDispatcherControlActor(
    final Dispatcher dispatcher,
    final AmazonDynamoDBAsync dynamodb,
    final RecordAdapter<RS> recordAdapter,
    final long checkConfirmationExpirationInterval,
    final long confirmationExpiration)
  {
    super();
    this.dispatcher = dispatcher;
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
      new DispatchAsyncHandler<RS>(recordAdapter::unmarshallDispatchable, this::doDispatch)
    );
  }
  
  private Void doDispatch(Dispatchable<?> dispatchable) {
    Duration duration = Duration.between(dispatchable.createdAt, LocalDateTime.now());
    if (Math.abs(duration.toMillis()) > confirmationExpiration) {
      dispatcher.dispatch(dispatchable.id, dispatchable.state);
    }
    return null;
  }
  
  @Override
  public void stop() {
    if (cancellable != null)
      cancellable.cancel();
    super.stop();
  }
}

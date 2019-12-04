// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.actors.ActorInstantiator;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.store.state.StateStoreEntryReader;

public class DynamoDBStateStoreEntryReaderActor<T extends Entry<?>> extends Actor implements StateStoreEntryReader<T> {
  private final String name;

  public DynamoDBStateStoreEntryReaderActor(final String name) {
    this.name = name;
  }

  @Override
  public void close() {
    // TODO
  }

  @Override
  public Completes<String> name() {
    return completes().with(name);
  }

  @Override
  public Completes<T> readNext() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<T> readNext(final String fromId) {
    seekTo(fromId);
    return readNext();
  }

  @Override
  public Completes<List<T>> readNext(final int maximumEntries) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Completes<List<T>> readNext(final String fromId, final int maximumEntries) {
    seekTo(fromId);
    return readNext(maximumEntries);
  }

  @Override
  public void rewind() {

  }

  @Override
  public Completes<String> seekTo(final String id) {
    return null;
  }

  @Override
  public Completes<Long> size() {
    return completes().with(-1L);
  }

  public static class DynamoDBStateStoreEntryReaderInstantiator<T extends Entry<?>> implements ActorInstantiator<DynamoDBStateStoreEntryReaderActor<T>> {
    private final String name;

    DynamoDBStateStoreEntryReaderInstantiator(final String name) {
      this.name = name;
    }

    @Override
    public DynamoDBStateStoreEntryReaderActor<T> instantiate() {
      return new DynamoDBStateStoreEntryReaderActor<>(name);
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Class<DynamoDBStateStoreEntryReaderActor<T>> type() {
      return (Class) DynamoDBStateStoreEntryReaderActor.class;
    }
  }
}

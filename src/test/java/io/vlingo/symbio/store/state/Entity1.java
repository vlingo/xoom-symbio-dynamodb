// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State.BinaryState;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapter;

public class Entity1 {
  public static final Metadata StdMetadata = Metadata.with("", "");

  public final String id;
  public final int value;
  public final int stateVersion;

  public Entity1(final String id, final int value) {
    this.id = id;
    this.value = value;
    this.stateVersion = 1;
  }

  public Entity1(final String id, final int value, final int stateVersion) {
    this.id = id;
    this.value = value;
    this.stateVersion = stateVersion;
  }

  @Override
  public int hashCode() {
    return 31 * id.hashCode();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || other.getClass() != this.getClass()) {
      return false;
    }
    return this.id.equals(((Entity1) other).id);
  }

  @Override
  public String toString() {
    return "Entity1[id=" + id + " value=" + value + "]";
  }

  public static class Entity1BinaryStateAdapter implements StateAdapter<Entity1,BinaryState> {
    private static Charset CHARSET_VALUE = Charset.forName(StandardCharsets.UTF_8.name());

    @Override
    public int typeVersion() {
      return 1;
    }

    @Override
    public Entity1 fromRawState(final BinaryState raw) {
      final String serialization = new String(raw.data, 0, raw.data.length, CHARSET_VALUE);
      return JsonSerialization.deserialized(serialization, raw.typed());
    }

    @Override
    public <ST> ST fromRawState(final BinaryState raw, final Class<ST> stateType) {
      throw new UnsupportedOperationException("Binray conversion not supported.");
    }

    @Override
    public BinaryState toRawState(final Entity1 state, final int stateVersion) {
      return toRawState(state.id, state, stateVersion, StdMetadata);
    }

    @Override
    public BinaryState toRawState(final String id, final Entity1 state, final int stateVersion, final Metadata metadata) {
      final String serialization = JsonSerialization.serialized(state);
      final byte[] bytes = serialization.getBytes(CHARSET_VALUE);
      return new BinaryState(state.id, Entity1.class, typeVersion(), bytes, stateVersion, metadata);
    }
  }

  public static class Entity1TextStateAdapter implements StateAdapter<Entity1,TextState> {

    @Override
    public int typeVersion() {
      return 1;
    }

    @Override
    public Entity1 fromRawState(final TextState raw) {
      return JsonSerialization.deserialized(raw.data, raw.typed());
    }

    @Override
    public <ST> ST fromRawState(final TextState raw, final Class<ST> stateType) {
      return JsonSerialization.deserialized(raw.data, stateType);
    }

    @Override
    public TextState toRawState(final Entity1 state, final int stateVersion) {
      return toRawState(state, stateVersion, StdMetadata);
    }

    @Override
    public TextState toRawState(final Entity1 state, final int stateVersion, final Metadata metadata) {
      return toRawState(state.id, state, stateVersion, metadata);
    }

    @Override
    public TextState toRawState(final String id, final Entity1 state, final int stateVersion, final Metadata metadata) {
      final String serialization = JsonSerialization.serialized(state);
      return new TextState(id, Entity1.class, typeVersion(), serialization, stateVersion, metadata);
    }
  }
}

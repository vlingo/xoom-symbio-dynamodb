package io.vlingo.xoom.symbio.implnative;

import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.dynamodb.DynamoDBDispatcherControlActor;
import io.vlingo.xoom.symbio.store.state.dynamodb.DynamoDBStateActor;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.TextStateRecordAdapter;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.nativeimage.c.type.CTypeConversion;

import java.util.Arrays;

public final class NativeBuildEntryPoint {
  @CEntryPoint(name = "Java_io_vlingo_xoom_symbio_dynamodbnative_Native_start")
  public static int start(@CEntryPoint.IsolateThreadContext long isolateId, CCharPointer name) {
    final String nameString = CTypeConversion.toJavaString(name);
    World world = World.startWithDefaults(nameString);

    DispatcherControl dispatcherControl = world.actorFor(
        DispatcherControl.class,
        Definition.has(
            DynamoDBDispatcherControlActor.class,
            new DynamoDBDispatcherControlActor.DynamoDBDispatcherControlInstantiator<>(new Dispatcher() {
              @Override
              public void controlWith(DispatcherControl control) {

              }

              @Override
              public void dispatch(Dispatchable dispatchable) {

              }
            }, null, new TextStateRecordAdapter(), 1000L, 1000L)));

    StateStore stateStore = world.actorFor(
        StateStore.class,
        Definition.has(
            DynamoDBStateActor.class,
            new DynamoDBStateActor.DynamoDBStateStoreInstantiator<>(Arrays.asList(null), dispatcherControl, null, null, new TextStateRecordAdapter()))
    );
    return 0;
  }
}

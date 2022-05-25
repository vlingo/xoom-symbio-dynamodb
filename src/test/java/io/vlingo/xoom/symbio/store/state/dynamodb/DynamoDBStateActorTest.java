// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.dynamodb;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.*;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.common.Success;
import io.vlingo.xoom.symbio.*;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.dispatch.ConfirmDispatchedResultInterest;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.Entity1;
import io.vlingo.xoom.symbio.store.state.Entity1.Entity1BinaryStateAdapter;
import io.vlingo.xoom.symbio.store.state.Entity1.Entity1TextStateAdapter;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.xoom.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.xoom.symbio.store.state.dynamodb.interests.CreateTableInterest;
import org.junit.*;

import java.util.*;

import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public abstract class DynamoDBStateActorTest<RS extends State<?>> {
    protected static final int DEFAULT_TIMEOUT = 6000;
    private static final String DYNAMODB_HOST = "http://localhost:8000";
    private static final String DYNAMODB_REGION = "eu-west-1";
    private static final AWSStaticCredentialsProvider DYNAMODB_CREDENTIALS = new AWSStaticCredentialsProvider(new BasicAWSCredentials("1", "2"));
    private static final AwsClientBuilder.EndpointConfiguration DYNAMODB_ENDPOINT_CONFIGURATION = new AwsClientBuilder.EndpointConfiguration(DYNAMODB_HOST, DYNAMODB_REGION);
    private static final String TABLE_NAME = "xoom_io_vlingo_xoom_symbio_store_state_Entity1";
    private static final String DISPATCHABLE_TABLE_NAME = "xoom_dispatchables";
    private static DynamoDBProxyServer dynamodbServer;

    private StateStore.WriteResultInterest writeResultInterest;
    private StateStore.ReadResultInterest readResultInterest;
    private ConfirmDispatchedResultInterest confirmDispatchedResultInterest;
    private Random random = new Random();

    protected boolean isBinaryTest; // must be set by subclass before invoking super.setUp();
    protected EntryAdapterProvider entryAdapterProvider;
    protected StateAdapterProvider stateAdapterProvider;

    protected World world;
    protected List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers;
    protected AmazonDynamoDBAsync dynamodb;
    protected CreateTableInterest createTableInterest;
    protected DispatcherControl dispatcherControl;
    protected StateStore stateStore;

    @BeforeClass
    public static void setUpDynamoDB() throws Exception {
        System.setProperty("sqlite4java.library.path", "native-libs");
        final String[] localArgs = {"-inMemory"};

        dynamodbServer = ServerRunner.createServerFromCommandLineArgs(localArgs);
        dynamodbServer.start();
    }

    @AfterClass
    public static void tearDownDynamoDb() throws Exception {
        dynamodbServer.stop();
        StateTypeStateStoreMap.reset();
    }

    protected <S> void doWrite(StateStore store, String id, S state, int stateVersion, StateStore.WriteResultInterest interest) {
      store.write(id, state, stateVersion, interest);
    }

    protected void doRead(StateStore store, String id, Class<?> type, StateStore.ReadResultInterest interest) {
      store.read(id, type, interest);
    }

    protected Entity1 randomState() {
      return new Entity1(UUID.randomUUID().toString(), random.nextInt(5_000_000));
    }

    protected Entity1 newFor(Entity1 oldState) {
      return new Entity1(oldState.id, oldState.value, oldState.stateVersion + 1);
    }

    protected abstract StateStore stateStoreProtocol(final World world, final List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers, final DispatcherControl dispatcherControl, final AmazonDynamoDBAsync dynamodb, final CreateTableInterest interest);

    protected abstract void verifyDispatched(List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers, String id, Dispatchable<Entry<?>, RS> dispatchable);

    protected abstract void verifyDispatched(List<Dispatcher<Dispatchable<Entry<?>, RS>>> dispatchers, String id, RS state);

    protected abstract RecordAdapter<RS> recordAdapter();

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        createTable(TABLE_NAME);
        createTable(DISPATCHABLE_TABLE_NAME);

        world = World.startWithDefaults(UUID.randomUUID().toString());

        stateAdapterProvider = new StateAdapterProvider(world);
        entryAdapterProvider = new EntryAdapterProvider(world);

        if (isBinaryTest) {
          stateAdapterProvider.registerAdapter(Entity1.class, new Entity1BinaryStateAdapter());
        } else {
          stateAdapterProvider.registerAdapter(Entity1.class, new Entity1TextStateAdapter());
        }
        dynamodb = AmazonDynamoDBAsyncClient.asyncBuilder()
                .withCredentials(DYNAMODB_CREDENTIALS)
                .withEndpointConfiguration(DYNAMODB_ENDPOINT_CONFIGURATION)
                .build();

        createTableInterest = mock(CreateTableInterest.class);
        writeResultInterest = mock(StateStore.WriteResultInterest.class);
        readResultInterest = mock(StateStore.ReadResultInterest.class);
        confirmDispatchedResultInterest = mock(ConfirmDispatchedResultInterest.class);

        dispatchers = Collections.singletonList(mock(Dispatcher.class));

        //store is created by setup() in subclasses
    }

    @After
    public void tearDown() {
        dropTable(TABLE_NAME);
        dropTable(DISPATCHABLE_TABLE_NAME);
    }

    @Test
    public void testThatCreatingATextStateActorCreatesTheDispatchableTable() {
        verify(createTableInterest).createDispatchableTable(dynamodb, DISPATCHABLE_TABLE_NAME);
    }

    @Test
    public void testThatWritingAndReadingTransactionReturnsCurrentState() {
        Entity1 currentState = randomState();
        doWrite(stateStore, currentState.id, currentState, 1, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), currentState.id, currentState, currentState.stateVersion, Source.none(), null);

        doRead(stateStore, currentState.id, Entity1.class, readResultInterest);
        verify(readResultInterest, timeout(DEFAULT_TIMEOUT)).readResultedIn(eq(Success.of(Result.Success)), eq(currentState.id), eq(currentState), eq(currentState.stateVersion), any(Metadata.class), eq(null));
    }

    @Test
    public void testThatWritingToATableCallsCreateTableInterest() {
        dropTable(TABLE_NAME);

        Entity1 currentState = randomState();
        doWrite(stateStore, currentState.id, currentState, 1, writeResultInterest);
        verify(createTableInterest, timeout(DEFAULT_TIMEOUT)).createEntityTable(dynamodb, TABLE_NAME);
    }

    @Test
    public void testThatWritingToATableThatDoesntExistFails() {
        dropTable(TABLE_NAME);
        Entity1 currentState = randomState();

        doWrite(stateStore, currentState.id, currentState, 1, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(
                eq(Failure.of(new StorageException(Result.NoTypeStore, ""))),
                eq(currentState.id),
                eq(currentState),
                eq(1),
                eq(Source.none()),
                eq(null)
        );
    }

    @Test
    public void testThatReadingAnUnknownStateFailsWithNotFound() {
        Entity1 currentState = randomState();

        doRead(stateStore, currentState.id, Entity1.class, readResultInterest);
        verify(readResultInterest, timeout(DEFAULT_TIMEOUT)).readResultedIn(
                Failure.of(new StorageException(Result.NotFound, "")),
                currentState.id,
                null,
                -1,
                null,
                null
        );
    }

    @Test
    public void testThatReadingOnAnUnknownTableFails() {
        dropTable(TABLE_NAME);
        Entity1 currentState = randomState();

        doRead(stateStore, currentState.id, Entity1.class, readResultInterest);
        verify(readResultInterest, timeout(DEFAULT_TIMEOUT)).readResultedIn(
                eq(Failure.of(new StorageException(Result.NoTypeStore, ""))),
                eq(currentState.id),
                eq(null),
                eq(-1),
                eq(null),
                eq(null)
        );
    }

    @Test
    public void testThatShouldNotAcceptWritingAnOldDataVersion() {
        Entity1 currentState = randomState();
        Entity1 newState = newFor(currentState);

        doWrite(stateStore, newState.id, newState, newState.stateVersion, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), newState.id, newState, newState.stateVersion, Source.none(), null);

        doWrite(stateStore, currentState.id, currentState, currentState.stateVersion, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Failure.of(new StorageException(Result.ConcurrencyViolation, "")), currentState.id, currentState, currentState.stateVersion, Source.none(), null);
    }

    @Test
    public void testThatDispatchesOnWrite() {
        Entity1 currentState = randomState();

        doWrite(stateStore, currentState.id, currentState, 1, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), currentState.id, currentState, currentState.stateVersion, Source.none(), null);

        RS raw = stateAdapterProvider.asRaw(currentState.id, currentState, currentState.stateVersion);
        verifyDispatched(dispatchers, currentState.getClass().getName() + ":" + currentState.id, raw);
    }

    @Test
    public void testThatWritingStoresTheDispatchableOnDynamoDB() {
        Entity1 currentState = randomState();

        doWrite(stateStore, currentState.id, currentState, currentState.stateVersion, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), currentState.id, currentState, currentState.stateVersion, Source.none(), null);

        RS raw = stateAdapterProvider.asRaw(currentState.id, currentState, currentState.stateVersion);
        Dispatchable<Entry<?>, RS> dispatchable = dispatchableByState(raw);
        Assert.assertTrue(dispatchable.state().isPresent());
        Assert.assertEquals(raw, dispatchable.state().get());
    }

    @Test
    public void testThatDispatchUnconfirmedShouldDispatchAllOnDynamoDB() {
        Entity1 currentState = randomState();

        doWrite(stateStore, currentState.id, currentState, currentState.stateVersion, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), currentState.id, currentState, currentState.stateVersion, Source.none(), null);

        RS raw = stateAdapterProvider.asRaw(currentState.id, currentState, currentState.stateVersion);
        Dispatchable<Entry<?>, RS> dispatchable = dispatchableByState(raw);

        dispatcherControl.dispatchUnconfirmed();
        verifyDispatched(dispatchers, dispatchable.id(), dispatchable);
    }

    @Test
    public void testThatConfirmDispatchRemovesRecordFromDynamoDB() {
        Entity1 currentState = randomState();

        doWrite(stateStore, currentState.id, currentState, currentState.stateVersion, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), currentState.id, currentState, currentState.stateVersion, Source.none(), null);

        RS raw = stateAdapterProvider.asRaw(currentState.id, currentState, currentState.stateVersion);
        Dispatchable<Entry<?>, RS> dispatchable = dispatchableByState(raw);
        dispatcherControl.confirmDispatched(dispatchable.id(), confirmDispatchedResultInterest);

        verify(confirmDispatchedResultInterest, timeout(DEFAULT_TIMEOUT))
                .confirmDispatchedResultedIn(Result.Success, dispatchable.id());

        assertNull(dispatchableByState(raw));
    }

    @Test
    public void testThatConfirmDispatchFailsWithFailureIfTableDoesNotExist() {
        dropTable(DISPATCHABLE_TABLE_NAME);

        String dispatchableId = UUID.randomUUID().toString();
        dispatcherControl.confirmDispatched(dispatchableId, confirmDispatchedResultInterest);

        verify(confirmDispatchedResultInterest, timeout(DEFAULT_TIMEOUT))
                .confirmDispatchedResultedIn(Result.Failure, dispatchableId);
    }

    private void createTable(String tableName) {
        AmazonDynamoDB syncDynamoDb = dynamoDBSyncClient();

        List<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("Id").withAttributeType("S"));

        List<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName("Id").withKeyType(KeyType.HASH));

        CreateTableRequest request = new CreateTableRequest()
                .withStreamSpecification(
                        new StreamSpecification()
                          .withStreamEnabled(true)
                          .withStreamViewType(StreamViewType.NEW_IMAGE))
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));

        syncDynamoDb.createTable(request);
    }

    private void dropTable(String tableName) {
        AmazonDynamoDB syncDynamoDb = dynamoDBSyncClient();

        try {
            syncDynamoDb.deleteTable(tableName);
        } catch (Exception ex) {

        }
    }

    private Dispatchable<Entry<?>, RS> dispatchableByState(RS state) {
        String dispatchableId = state.type + ":" + state.id;

        GetItemResult item = dynamoDBSyncClient().getItem(DISPATCHABLE_TABLE_NAME, recordAdapter().marshallForQuery(dispatchableId));

        Map<String, AttributeValue> dispatchableSerializedItem = item.getItem();
        if (dispatchableSerializedItem == null) {
            return null;
        }

        return recordAdapter().unmarshallDispatchable(dispatchableSerializedItem);
    }

    private AmazonDynamoDB dynamoDBSyncClient() {
        return AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(DYNAMODB_ENDPOINT_CONFIGURATION)
                .withCredentials(DYNAMODB_CREDENTIALS)
                .build();
    }
}

// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.dynamodb;

import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

import io.vlingo.actors.Protocols;
import io.vlingo.actors.World;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.dynamodb.adapters.RecordAdapter;
import io.vlingo.symbio.store.state.dynamodb.interests.CreateTableInterest;

public abstract class DynamoDBStateActorTest<T extends StateStore, RS extends State<?>> {
    protected static final int DEFAULT_TIMEOUT = 6000;
    private static final String DYNAMODB_HOST = "http://localhost:8000";
    private static final String DYNAMODB_REGION = "eu-west-1";
    private static final AWSStaticCredentialsProvider DYNAMODB_CREDENTIALS = new AWSStaticCredentialsProvider(new BasicAWSCredentials("1", "2"));
    private static final AwsClientBuilder.EndpointConfiguration DYNAMODB_ENDPOINT_CONFIGURATION = new AwsClientBuilder.EndpointConfiguration(DYNAMODB_HOST, DYNAMODB_REGION);
    private static final String TABLE_NAME = "vlingo_io_vlingo_symbio_store_state_Entity1";
    private static final String DISPATCHABLE_TABLE_NAME = "vlingo_dispatchables";
    private static DynamoDBProxyServer dynamodbServer;

    private World world;
    private AmazonDynamoDBAsync dynamodb;
    private CreateTableInterest createTableInterest;
    private T stateStore;
    private StateStore.DispatcherControl dispatcherControl;
    private StateStore.WriteResultInterest<RS> writeResultInterest;
    private StateStore.ReadResultInterest<RS> readResultInterest;
    private StateStore.Dispatcher dispatcher;
    private StateStore.ConfirmDispatchedResultInterest confirmDispatchedResultInterest;

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

    protected abstract Protocols stateStoreProtocols(World world, StateStore.Dispatcher dispatcher, AmazonDynamoDBAsync dynamodb, CreateTableInterest interest);

    protected abstract void doWrite(T actor, RS state, StateStore.WriteResultInterest<RS> interest);

    protected abstract void doRead(T actor, String id, Class<?> type, StateStore.ReadResultInterest<RS> interest);

    protected abstract RS nullState();

    protected abstract RS randomState();

    protected abstract RS newFor(RS oldState);

    protected abstract void verifyDispatched(StateStore.Dispatcher dispatcher, String id, StateStore.Dispatchable<RS> dispatchable);

    protected abstract void verifyDispatched(StateStore.Dispatcher dispatcher, String id, RS state);

    protected abstract RecordAdapter<RS> recordAdapter();

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        createTable(TABLE_NAME);
        createTable(DISPATCHABLE_TABLE_NAME);

        world = World.startWithDefaults(UUID.randomUUID().toString());

        dynamodb = AmazonDynamoDBAsyncClient.asyncBuilder()
                .withCredentials(DYNAMODB_CREDENTIALS)
                .withEndpointConfiguration(DYNAMODB_ENDPOINT_CONFIGURATION)
                .build();

        createTableInterest = mock(CreateTableInterest.class);
        writeResultInterest = mock(StateStore.WriteResultInterest.class);
        readResultInterest = mock(StateStore.ReadResultInterest.class);
        confirmDispatchedResultInterest = mock(StateStore.ConfirmDispatchedResultInterest.class);

        dispatcher = mock(StateStore.Dispatcher.class);

        Protocols protocols = stateStoreProtocols(world, dispatcher, dynamodb, createTableInterest);

        stateStore = protocols.get(0);
        dispatcherControl = protocols.get(1);
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
        RS currentState = randomState();
        doWrite(stateStore, currentState, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), currentState.id, currentState, null);

        doRead(stateStore, currentState.id, currentState.typed(), readResultInterest);
        verify(readResultInterest, timeout(DEFAULT_TIMEOUT)).readResultedIn(Success.of(Result.Success), currentState.id, currentState, null);
    }

    @Test
    public void testThatWritingToATableCallsCreateTableInterest() {
        dropTable(TABLE_NAME);

        doWrite(stateStore, randomState(), writeResultInterest);
        verify(createTableInterest, timeout(DEFAULT_TIMEOUT)).createEntityTable(dynamodb, TABLE_NAME);
    }

    @Test
    public void testThatWritingToATableThatDoesntExistFails() {
        dropTable(TABLE_NAME);
        RS state = randomState();

        doWrite(stateStore, state, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(
                eq(Failure.of(new StorageException(Result.NoTypeStore, ""))),
                eq(state.id),
                eq(nullState()),
                eq(null)
        );
    }

    @Test
    public void testThatReadingAnUnknownStateFailsWithNotFound() {
        RS state = randomState();

        doRead(stateStore, state.id, Entity1.class, readResultInterest);
        verify(readResultInterest, timeout(DEFAULT_TIMEOUT)).readResultedIn(
                Failure.of(new StorageException(Result.NotFound, "")),
                state.id,
                nullState(),
                null
        );
    }

    @Test
    public void testThatReadingOnAnUnknownTableFails() {
        dropTable(TABLE_NAME);
        RS state = randomState();

        doRead(stateStore, state.id, Entity1.class, readResultInterest);
        verify(readResultInterest, timeout(DEFAULT_TIMEOUT)).readResultedIn(
                eq(Failure.of(new StorageException(Result.NoTypeStore, ""))),
                eq(state.id),
                eq(nullState()),
                eq(null)
        );
    }

    @Test
    public void testThatShouldNotAcceptWritingAnOldDataVersion() {
        RS oldState = randomState();
        RS newState = newFor(oldState);

        doWrite(stateStore, newState, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), newState.id, newState, null);

        doWrite(stateStore, oldState, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Failure.of(new StorageException(Result.ConcurrentyViolation, "")), newState.id, newState, null);
    }

    @Test
    public void testThatDispatchesOnWrite() {
        RS state = randomState();

        doWrite(stateStore, state, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), state.id, state, null);

        verifyDispatched(dispatcher, state.type + ":" + state.id, state);
//        verify(dispatcher, timeout(DEFAULT_TIMEOUT)).dispatch(state.type + ":" + state.id, state.asTextState());
    }

    @Test
    public void testThatWritingStoresTheDispatchableOnDynamoDB() {
        RS state = randomState();

        doWrite(stateStore, state, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), state.id, state, null);

        StateStore.Dispatchable<RS> dispatchable = dispatchableByState(state);
        Assert.assertEquals(state, dispatchable.state);
    }

    @Test
    public void testThatDispatchUnconfirmedShouldDispatchAllOnDynamoDB() {
        RS state = randomState();

        doWrite(stateStore, state, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), state.id, state, null);

        StateStore.Dispatchable<RS> dispatchable = dispatchableByState(state);

        dispatcherControl.dispatchUnconfirmed();
        verifyDispatched(dispatcher, dispatchable.id, dispatchable);
//        verify(dispatcher).dispatch(dispatchable.id, stateFromDispatchable(dispatchable));
    }

    @Test
    public void testThatConfirmDispatchRemovesRecordFromDynamoDB() {
        RS state = randomState();

        doWrite(stateStore, state, writeResultInterest);
        verify(writeResultInterest, timeout(DEFAULT_TIMEOUT)).writeResultedIn(Success.of(Result.Success), state.id, state, null);

        StateStore.Dispatchable<RS> dispatchable = dispatchableByState(state);
        dispatcherControl.confirmDispatched(dispatchable.id, confirmDispatchedResultInterest);

        verify(confirmDispatchedResultInterest, timeout(DEFAULT_TIMEOUT))
                .confirmDispatchedResultedIn(Result.Success, dispatchable.id);

        assertNull(dispatchableByState(state));
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

    private StateStore.Dispatchable<RS> dispatchableByState(RS state) {
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

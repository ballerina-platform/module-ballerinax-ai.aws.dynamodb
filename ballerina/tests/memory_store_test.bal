// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Unit tests for `ShortTermMemoryStore`.
//
// The tests run entirely against an in-memory `FakeDynamoDbClient`
// (`tests/fake_dynamodb_client.bal`) that is installed via
// `test:mock(dynamodb:Client, fake)`, so no AWS credentials and no running
// DynamoDB instance are required.
//
// Mocking is the only option here, not merely the convenient one: the
// `ballerinax/aws.dynamodb` client derives its endpoint solely from the AWS
// region (`https://dynamodb.<region>.amazonaws.com`) and its `ConnectionConfig`
// exposes no endpoint override, so the tests cannot be pointed at DynamoDB
// Local or any other local emulator.

import ballerina/ai;
import ballerina/test;
import ballerinax/aws.dynamodb;

// -----------------------------------------------------------------------------
// Common fixtures
// -----------------------------------------------------------------------------

const string K1 = "key1";
const string K2 = "key2";
const string K3 = "key3";

const string TABLE_NAME = "chat_memory";

final readonly & ai:ChatSystemMessage SYSTEM_WEATHER = {
    role: ai:SYSTEM,
    content: "You are a helpful assistant that is aware of the weather."
};
final readonly & ai:ChatSystemMessage SYSTEM_SPORTS = {
    role: ai:SYSTEM,
    content: "You are a helpful assistant that is aware of sports."
};

final readonly & ai:ChatUserMessage USER_INTRO = {role: ai:USER, content: "Hello, my name is Alice. I'm from Seattle."};
final readonly & ai:ChatAssistantMessage ASSISTANT_GREETING = {
    role: ai:ASSISTANT,
    content: "Hello Alice, what can I do for you?"
};
final readonly & ai:ChatUserMessage USER_WEATHER_Q = {role: ai:USER, content: "I would like to know the weather today."};
final readonly & ai:ChatAssistantMessage ASSISTANT_WEATHER_A = {
    role: ai:ASSISTANT,
    content: "The weather in Seattle today is mostly cloudy with occasional showers and a high around 58°F."
};
final readonly & ai:ChatUserMessage USER_K2 = {role: ai:USER, content: "Hello, my name is Bob."};

// Builds a fresh storage and the corresponding mocked `dynamodb:Client` used as
// the dependency injected into a store under test. Each test calls this to
// rotate the (module-level) active storage so state never leaks between tests.
function newFakePair() returns [FakeStorage, dynamodb:Client] {
    FakeStorage fake = newFakeStorage();
    dynamodb:Client mocked = test:mock(dynamodb:Client, new FakeDynamoDbClient());
    return [fake, mocked];
}

// -----------------------------------------------------------------------------
// Table lifecycle / initialization
// -----------------------------------------------------------------------------

@test:Config {}
function testInitCreatesTableWhenAbsent() returns error? {
    var [fake, mocked] = newFakePair();
    test:assertFalse(fake.hasTable(TABLE_NAME), "Table should not exist before store init");

    _ = check new ShortTermMemoryStore(mocked);

    test:assertTrue(fake.hasTable(TABLE_NAME),
        "Store init must create the backing DynamoDB table when it does not exist");
}

@test:Config {}
function testInitReusesExistingTable() returns error? {
    var [_, mocked] = newFakePair();
    // First init creates the table; the second init must succeed without errors,
    // because the connector handles the `ResourceInUseException` returned by the
    // fake the same way the real DynamoDB does.
    _ = check new ShortTermMemoryStore(mocked);
    _ = check new ShortTermMemoryStore(mocked);
}

@test:Config {}
function testInitRejectsInvalidTableName() returns error? {
    var [_, mocked] = newFakePair();
    string[] invalidNames = ["ab", "has space", "exclaim!", "two/parts"];
    foreach string name in invalidNames {
        Error|ShortTermMemoryStore result = new ShortTermMemoryStore(mocked, tableConfig = {tableName: name});
        test:assertTrue(result is Error,
            string `Expected init to fail for invalid table name '${name}'`);
    }
}

@test:Config {}
function testInitAcceptsValidTableName() returns error? {
    var [fake, mocked] = newFakePair();
    string customTable = "custom.memory-table_1";
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {tableName: customTable});
    test:assertTrue(fake.hasTable(customTable),
        string `Store must create the requested custom table '${customTable}'`);
}

@test:Config {}
function testInitRejectsNonPositiveMaxMessagesPerKey() returns error? {
    var [_, mocked] = newFakePair();
    foreach int invalid in [0, -1, -100] {
        Error|ShortTermMemoryStore result = new (mocked, invalid);
        test:assertTrue(result is Error,
            string `Expected init to fail for maxMessagesPerKey '${invalid}'`);
        if result is Error {
            test:assertTrue(result.message().includes("maxMessagesPerKey"),
                "The error must name the offending parameter");
        }
    }
}

@test:Config {}
function testInitFromConnectionConfigBuildsItsOwnClient() returns error? {
    // The `dynamodb:ConnectionConfig` overload constructs the client internally.
    // Auto-create is switched off so init makes no control-plane call and the test
    // stays entirely offline — the client is built, never used.
    ShortTermMemoryStore store = check new ({
        awsCredentials: {accessKeyId: "test-access-key", secretAccessKey: "test-secret-key"},
        region: "us-east-1"
    }, 7, {createTableIfNotExists: false});

    test:assertEquals(store.getCapacity(), 7,
        "A store built from a connection config must be fully initialized");
}

@test:Config {}
function testInitFailsWhenConnectionConfigIsUnusable() returns error? {
    // A region that cannot form a valid endpoint host makes the underlying
    // `dynamodb:Client` constructor fail; the store must wrap that rather than
    // panic or return a half-built store.
    Error|ShortTermMemoryStore result = new ({
        awsCredentials: {accessKeyId: "test-access-key", secretAccessKey: "test-secret-key"},
        region: "not a region"
    }, tableConfig = {createTableIfNotExists: false});

    test:assertTrue(result is Error, "An unusable connection config must fail init");
    if result is Error {
        test:assertTrue(result.message().includes("Failed to create DynamoDB client"),
            string `Unexpected error message: ${result.message()}`);
    }
}

@test:Config {}
function testInitPassesTagsAndEncryptionToCreateTable() returns error? {
    var [fake, mocked] = newFakePair();
    dynamodb:Tag[] tags = [{Key: "env", Value: "test"}, {Key: "owner", Value: "memory-store"}];

    _ = check new ShortTermMemoryStore(mocked, tableConfig = {
        tags,
        sseSpecification: {Enabled: true}
    });

    dynamodb:TableCreateInput? createInput = fake.peekCreateTableInput();
    if createInput is () {
        test:assertFail("Expected the store to issue a CreateTable call");
    }
    test:assertEquals(createInput.Tags, tags, "Configured tags must reach CreateTable");
    test:assertEquals(createInput?.SSESpecification?.Enabled, true,
        "Configured server-side encryption must reach CreateTable");
}

@test:Config {}
function testInitOmitsTagsAndEncryptionWhenNotConfigured() returns error? {
    var [fake, mocked] = newFakePair();
    // An empty tag array is treated the same as none: DynamoDB rejects an empty
    // `Tags` list, so the store must leave the field off entirely.
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {tags: []});

    dynamodb:TableCreateInput? createInput = fake.peekCreateTableInput();
    if createInput is () {
        test:assertFail("Expected the store to issue a CreateTable call");
    }
    test:assertEquals(createInput.Tags, (), "An empty tag array must not be sent as Tags");
    test:assertEquals(createInput.SSESpecification, (),
        "SSESpecification must be omitted when not configured");
    test:assertEquals(createInput.BillingMode, dynamodb:PAY_PER_REQUEST,
        "The default billing mode must be on-demand");
    test:assertEquals(createInput.ProvisionedThroughput, (),
        "Throughput must be omitted under PAY_PER_REQUEST billing");
}

@test:Config {}
function testInitPassesProvisionedThroughputToCreateTable() returns error? {
    var [fake, mocked] = newFakePair();
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: 3, writeCapacityUnits: 2
    });

    dynamodb:TableCreateInput? createInput = fake.peekCreateTableInput();
    if createInput is () {
        test:assertFail("Expected the store to issue a CreateTable call");
    }
    test:assertEquals(createInput.BillingMode, dynamodb:PROVISIONED);
    test:assertEquals(createInput?.ProvisionedThroughput?.ReadCapacityUnits, 3);
    test:assertEquals(createInput?.ProvisionedThroughput?.WriteCapacityUnits, 2);
}

@test:Config {}
function testGetCapacityDefault() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    test:assertEquals(store.getCapacity(), 20);
}

@test:Config {}
function testGetCapacityCustom() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 7);
    test:assertEquals(store.getCapacity(), 7);
}

// -----------------------------------------------------------------------------
// Happy paths: put + get for system, interactive, and combined messages.
// -----------------------------------------------------------------------------

@test:Config {}
function testPutAndGetSystemMessage() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);

    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(K1);
    test:assertTrue(actual is ai:ChatSystemMessage, "Expected a system message to be returned");
    assertChatMessageEquals(<ai:ChatMessage>actual, SYSTEM_WEATHER);
}

@test:Config {}
function testGetSystemMessageReturnsNilWhenAbsent() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(K1);
    test:assertTrue(actual is (), "Expected nil when no system message is set");
}

@test:Config {}
function testPutAndGetInteractiveMessagesPreservesOrder() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);
    check store.put(K1, ASSISTANT_WEATHER_A);

    check assertInteractiveMessages(store, K1,
        [USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q, ASSISTANT_WEATHER_A]);
}

@test:Config {}
function testGetAllCombinesSystemAndInteractive() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testGetAllReturnsOnlyInteractiveWhenNoSystem() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    ai:ChatMessage[] all = check store.getAll(K1);
    test:assertEquals(all.length(), 2);
    assertChatMessageEquals(all[0], USER_INTRO);
    assertChatMessageEquals(all[1], ASSISTANT_GREETING);
}

@test:Config {}
function testGetAllOnEmptyKey() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check assertAllMessages(store, K3, []);
}

// -----------------------------------------------------------------------------
// put() variants: arrays, mixed message kinds, prompt content, and name fields.
// -----------------------------------------------------------------------------

@test:Config {}
function testPutAllInsertsMixedBatch() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testPutAllWithMultipleSystemMessagesKeepsLast() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    // The store contract says: when an array contains multiple ChatSystemMessage
    // values, only the LAST one is persisted; earlier ones are discarded.
    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING, SYSTEM_SPORTS]);

    check assertSystemMessage(store, K1, SYSTEM_SPORTS);
    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testPutAllWithEmptyArrayIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, []);

    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testPutAllWithOnlySystemMessages() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, [SYSTEM_WEATHER, SYSTEM_SPORTS]);

    check assertSystemMessage(store, K1, SYSTEM_SPORTS);
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testPutSystemMessageOverwrites() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, SYSTEM_SPORTS);

    check assertSystemMessage(store, K1, SYSTEM_SPORTS);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testPutFunctionAndAssistantMessages() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    final readonly & ai:ChatFunctionMessage funcMessage = {
        role: "function",
        name: "getWeather",
        id: "func1"
    };

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, funcMessage);

    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING, funcMessage]);
}

@test:Config {}
function testPutUserMessageWithNameField() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    final readonly & ai:ChatUserMessage namedUser = {role: ai:USER, content: "Hi", name: "alice"};
    check store.put(K1, namedUser);

    check assertInteractiveMessages(store, K1, [namedUser]);
}

@test:Config {}
function testPutSystemMessageWithNameField() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    final readonly & ai:ChatSystemMessage namedSystem =
        {role: ai:SYSTEM, content: "You are helpful.", name: "system_v2"};
    check store.put(K1, namedSystem);

    check assertSystemMessage(store, K1, namedSystem);
}

isolated function createTestPrompt(string[] & readonly strings, anydata[] & readonly insertions)
        returns readonly & ai:Prompt => isolated object ai:Prompt {
    public final string[] & readonly strings = strings;
    public final anydata[] & readonly insertions = insertions;
};

@test:Config {}
function testPutUserMessageWithPromptContent() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    string[] & readonly parts = ["Hello, my name is ", "."];
    anydata[] & readonly insertions = ["Alice"];
    final readonly & ai:Prompt prompt = createTestPrompt(parts, insertions);
    final readonly & ai:ChatUserMessage userWithPrompt = {role: ai:USER, content: prompt};

    check store.put(K1, userWithPrompt);

    check assertInteractiveMessages(store, K1, [userWithPrompt]);
}

@test:Config {}
function testPutSystemMessageWithPromptContent() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    string[] & readonly parts = ["You are a ", " assistant."];
    anydata[] & readonly insertions = ["helpful"];
    final readonly & ai:Prompt prompt = createTestPrompt(parts, insertions);
    final readonly & ai:ChatSystemMessage sysWithPrompt = {role: ai:SYSTEM, content: prompt};

    check store.put(K1, sysWithPrompt);

    check assertSystemMessage(store, K1, sysWithPrompt);
}

// -----------------------------------------------------------------------------
// Removal: system message, interactive messages, all.
// -----------------------------------------------------------------------------

@test:Config {}
function testRemoveSystemMessageLeavesInteractiveIntact() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatSystemMessage(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testRemoveAllInteractiveLeavesSystemIntact() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatInteractiveMessages(K1);

    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testRemoveInteractivePartial() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);
    check store.put(K1, ASSISTANT_WEATHER_A);

    // Remove the first two interactive messages.
    check store.removeChatInteractiveMessages(K1, 2);

    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
    check assertInteractiveMessages(store, K1, [USER_WEATHER_Q, ASSISTANT_WEATHER_A]);
}

@test:Config {}
function testRemoveInteractiveCountZeroIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatInteractiveMessages(K1, 0);

    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testRemoveInteractiveCountExceedsLength() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeChatInteractiveMessages(K1, 10);

    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testRemoveInteractiveNegativeCountErrors() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);

    Error? result = store.removeChatInteractiveMessages(K1, -1);
    test:assertTrue(result is Error, "Negative counts must yield an error");
}

@test:Config {}
function testRemoveAllClearsEverythingForKey() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    check store.removeAll(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);
    check assertAllMessages(store, K1, []);
}

@test:Config {}
function testRemoveSystemOnNonExistentKeyIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    Error? result = store.removeChatSystemMessage(K3);
    test:assertTrue(result is (), "Removing a non-existent system message must succeed");
}

@test:Config {}
function testRemoveInteractiveOnNonExistentKeyIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    Error? result = store.removeChatInteractiveMessages(K3);
    test:assertTrue(result is (), "Removing interactive messages on an unseen key must succeed");
}

@test:Config {}
function testRemoveAllOnNonExistentKeyIsNoOp() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    Error? result = store.removeAll(K3);
    test:assertTrue(result is (), "Removing on an unseen key must succeed");
}

@test:Config {}
function testAddingMessagesAfterRemoveAllStartsClean() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.removeAll(K1);

    check store.put(K1, USER_WEATHER_Q);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, [USER_WEATHER_Q]);
}

// -----------------------------------------------------------------------------
// Multi-key isolation.
// -----------------------------------------------------------------------------

@test:Config {}
function testKeysAreIsolated() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);
    check store.put(K2, USER_K2);

    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);

    check assertSystemMessage(store, K2, ());
    check assertInteractiveMessages(store, K2, [USER_K2]);

    check store.removeAll(K1);

    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);

    check assertInteractiveMessages(store, K2, [USER_K2]);
}

// -----------------------------------------------------------------------------
// Custom table name behaviour.
// -----------------------------------------------------------------------------

@test:Config {}
function testCustomTableNameWritesToThatTable() returns error? {
    var [fake, mocked] = newFakePair();
    string custom = "custom_memory_table";
    ShortTermMemoryStore store = check new (mocked, tableConfig = {tableName: custom});

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);

    test:assertTrue(fake.hasTable(custom),
        string `Expected custom table '${custom}' to be created`);
    test:assertEquals(fake.hasTable(TABLE_NAME), false,
        "Default table name should NOT be created when a custom one is supplied");
    test:assertTrue(fake.peekBody(custom, K1, SYSTEM_MESSAGE_ID) is string,
        "System message must be persisted in the custom table");
}

@test:Config {}
function testTwoStoresOnDifferentTablesAreIsolated() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore storeA = check new (mocked, tableConfig = {tableName: "memory_a"});
    ShortTermMemoryStore storeB = check new (mocked, tableConfig = {tableName: "memory_b"});

    check storeA.put(K1, USER_INTRO);
    check storeB.put(K1, USER_K2);

    check assertInteractiveMessages(storeA, K1, [USER_INTRO]);
    check assertInteractiveMessages(storeB, K1, [USER_K2]);
}

// -----------------------------------------------------------------------------
// isFull() and capacity boundaries.
// -----------------------------------------------------------------------------

@test:Config {}
function testIsFullFalseWhenEmpty() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 3);

    test:assertFalse(check store.isFull(K1));
}

@test:Config {}
function testIsFullFalseWhenBelowCapacity() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 3);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    test:assertFalse(check store.isFull(K1));
}

@test:Config {}
function testIsFullTrueAtCapacity() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 2);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);

    test:assertTrue(check store.isFull(K1));
}

@test:Config {}
function testIsFullTrueAboveCapacity() returns error? {
    var [_, mocked] = newFakePair();
    // The store does not enforce the cap on put — `isFull` is purely advisory.
    ShortTermMemoryStore store = check new (mocked, 2);

    check store.put(K1, USER_INTRO);
    check store.put(K1, ASSISTANT_GREETING);
    check store.put(K1, USER_WEATHER_Q);

    test:assertTrue(check store.isFull(K1));
}

@test:Config {}
function testIsFullIgnoresSystemMessage() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 2);

    check store.put(K1, SYSTEM_WEATHER);
    check store.put(K1, USER_INTRO);

    test:assertFalse(check store.isFull(K1),
        "isFull must not count the system message towards capacity");
}

// -----------------------------------------------------------------------------
// Insertion-order sanity (many interactive messages).
// -----------------------------------------------------------------------------

@test:Config {}
function testManyInteractiveMessagesPreserveOrder() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 100);

    ai:ChatUserMessage[] inserted = [];
    foreach int i in 0 ..< 30 {
        ai:ChatUserMessage msg = {role: ai:USER, content: string `message-${i}`};
        check store.put(K1, msg);
        inserted.push(msg);
    }

    ai:ChatInteractiveMessage[] readBack = check store.getChatInteractiveMessages(K1);
    test:assertEquals(readBack.length(), inserted.length());
    foreach int i in 0 ..< inserted.length() {
        assertChatMessageEquals(readBack[i], inserted[i]);
    }
}

@test:Config {}
function testPutAllAppendBatchPreservesOrder() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 100);

    ai:ChatMessage[] firstBatch = [];
    foreach int i in 0 ..< 5 {
        firstBatch.push({role: ai:USER, content: string `first-${i}`});
    }
    ai:ChatMessage[] secondBatch = [];
    foreach int i in 0 ..< 5 {
        secondBatch.push({role: ai:USER, content: string `second-${i}`});
    }

    check store.put(K1, firstBatch);
    check store.put(K1, secondBatch);

    ai:ChatMessage[] combined = [...firstBatch, ...secondBatch];
    ai:ChatInteractiveMessage[] readBack = check store.getChatInteractiveMessages(K1);
    test:assertEquals(readBack.length(), combined.length());
    foreach int i in 0 ..< combined.length() {
        assertChatMessageEquals(readBack[i], combined[i]);
    }
}

// -----------------------------------------------------------------------------
// Chunking at the BatchWriteItem limit.
//
// DynamoDB caps BatchWriteItem at 25 requests, so both the append and the delete
// paths split larger workloads into chunks. These tests cross that boundary
// several times over to prove nothing is dropped or reordered at the seams.
// -----------------------------------------------------------------------------

@test:Config {}
function testPutAllChunksBatchesOverTheWriteLimit() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 100);

    // 60 messages span three chunks (25 + 25 + 10).
    ai:ChatMessage[] batch = [];
    foreach int i in 0 ..< 60 {
        batch.push({role: ai:USER, content: string `bulk-${i}`});
    }

    check store.put(K1, batch);

    ai:ChatInteractiveMessage[] readBack = check store.getChatInteractiveMessages(K1);
    test:assertEquals(readBack.length(), 60, "Every message in a multi-chunk batch must be stored");
    foreach int i in 0 ..< 60 {
        assertChatMessageEquals(readBack[i], batch[i]);
    }
    test:assertEquals(fake.peekCounter(TABLE_NAME, K1), 60,
        "The sequence counter must be reserved once for the whole batch");
}

@test:Config {}
function testRemoveAllChunksDeletesOverTheWriteLimit() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 100);

    ai:ChatMessage[] batch = [SYSTEM_WEATHER];
    foreach int i in 0 ..< 60 {
        batch.push({role: ai:USER, content: string `bulk-${i}`});
    }
    check store.put(K1, batch);

    check store.removeAll(K1);

    test:assertEquals(fake.peekSortIds(TABLE_NAME, K1), [],
        "A multi-chunk delete must clear every item for the key, counter included");
    check assertSystemMessage(store, K1, ());
    check assertInteractiveMessages(store, K1, []);
}

@test:Config {}
function testRemoveInteractivePartialAcrossChunkBoundary() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 100);

    ai:ChatMessage[] batch = [];
    foreach int i in 0 ..< 60 {
        batch.push({role: ai:USER, content: string `bulk-${i}`});
    }
    check store.put(K1, batch);

    // Removing 30 oldest messages spans two delete chunks (25 + 5).
    check store.removeChatInteractiveMessages(K1, 30);

    ai:ChatInteractiveMessage[] readBack = check store.getChatInteractiveMessages(K1);
    test:assertEquals(readBack.length(), 30);
    foreach int i in 0 ..< 30 {
        assertChatMessageEquals(readBack[i], batch[i + 30]);
    }
}

// -----------------------------------------------------------------------------
// Control-plane / retry branches.
//
// These paths depend on timing and throttling that a real table cannot be made
// to reproduce on demand, so they are exercised here by arming the
// fault-injection knobs on `FakeDynamoDbClient`.
// -----------------------------------------------------------------------------

@test:Config {}
function testInitWaitsForTableToBecomeActive() returns error? {
    var [fake, mocked] = newFakePair();
    // Force the first DescribeTable *after* creation to report CREATING; the
    // store must poll again (sleeping between polls) until it sees ACTIVE before
    // init returns. One armed poll is enough to drive the loop's wait branch.
    fake.setActivationPolls(1);

    ShortTermMemoryStore store = check new ShortTermMemoryStore(mocked);

    test:assertTrue(fake.hasTable(TABLE_NAME), "Table must be created during init");
    // The store is fully usable the instant init returns, which proves init did
    // not return until the table reported ACTIVE.
    check store.put(K1, USER_INTRO);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testBatchWriteRetriesUnprocessedItems() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    // The first BatchWriteItem reports every item as unprocessed; the store must
    // retry the chunk and ultimately persist all three messages in order.
    fake.setUnprocessedRounds(1);

    check store.put(K1, [USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q]);

    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING, USER_WEATHER_Q]);
}

@test:Config {}
function testBatchWriteFailsAfterMaxRetries() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    // Every attempt keeps reporting unprocessed items, so the chunk never drains.
    // The store must give up after MAX_BATCH_WRITE_RETRIES and surface an error
    // rather than silently dropping the writes. (100 > the 1 + MAX retries the
    // store performs, so the fault outlives every attempt.)
    fake.setUnprocessedRounds(100);

    Error? result = store.put(K1, [USER_INTRO, ASSISTANT_GREETING]);
    test:assertTrue(result is Error, "put must fail when batch writes never drain");
}

@test:Config {}
function testInitRejectsProvisionedWithNonPositiveCapacity() returns error? {
    var [_, mocked] = newFakePair();
    // Under PROVISIONED billing the store validates that both capacities are
    // positive integers, returning an error before any AWS call is made.
    Error|ShortTermMemoryStore zeroRead = new (mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: 0, writeCapacityUnits: 5
    });
    test:assertTrue(zeroRead is Error, "Zero readCapacityUnits under PROVISIONED must be rejected");

    Error|ShortTermMemoryStore zeroWrite = new (mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: 5, writeCapacityUnits: 0
    });
    test:assertTrue(zeroWrite is Error, "Zero writeCapacityUnits under PROVISIONED must be rejected");

    Error|ShortTermMemoryStore negative = new (mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: -1, writeCapacityUnits: -1
    });
    test:assertTrue(negative is Error, "Negative capacities under PROVISIONED must be rejected");
}

@test:Config {}
function testInitProvisionedWithValidCapacityCreatesTable() returns error? {
    var [fake, mocked] = newFakePair();
    // The positive-capacity PROVISIONED path must pass validation and create the
    // table (the throughput is carried on the CreateTable input).
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {
        billingMode: dynamodb:PROVISIONED, readCapacityUnits: 3, writeCapacityUnits: 2
    });
    test:assertTrue(fake.hasTable(TABLE_NAME), "PROVISIONED table with valid capacities must be created");
}

@test:Config {}
function testInitWithCreateTableIfNotExistsFalseSkipsCreate() returns error? {
    var [fake, mocked] = newFakePair();
    test:assertFalse(fake.hasTable(TABLE_NAME), "Precondition: table must be absent");

    // With auto-create disabled the store performs no control-plane calls during
    // init: it neither describes nor creates the table.
    _ = check new ShortTermMemoryStore(mocked, tableConfig = {createTableIfNotExists: false});

    test:assertFalse(fake.hasTable(TABLE_NAME),
        "init with createTableIfNotExists=false must not create the table");
}

@test:Config {}
function testInitWithCreateTableIfNotExistsFalseUsesExistingTable() returns error? {
    var [_, mocked] = newFakePair();
    // Provision the table out of band first (as IaC would).
    _ = check new ShortTermMemoryStore(mocked);

    // A second store with auto-create disabled assumes the table already exists
    // and operates against it without any control-plane call.
    ShortTermMemoryStore store = check new (mocked, tableConfig = {createTableIfNotExists: false});
    check store.put(K1, USER_INTRO);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

// -----------------------------------------------------------------------------
// Control-plane failure propagation.
//
// `FakeStorage.setOpFailure` fails a chosen AWS call so the tests can pin down
// which failures init must abort on and which it must absorb.
// -----------------------------------------------------------------------------

@test:Config {}
function testInitFailsWhenDescribeTableErrors() returns error? {
    var [fake, mocked] = newFakePair();
    // Anything other than ResourceNotFoundException (here: AccessDenied) means we
    // cannot tell whether the table exists, so init must abort instead of
    // attempting a CreateTable that would also fail.
    fake.setOpFailure(OP_DESCRIBE_TABLE);

    Error|ShortTermMemoryStore result = new (mocked);

    test:assertTrue(result is Error, "A non-ResourceNotFound DescribeTable failure must fail init");
    if result is Error {
        test:assertTrue(result.message().includes("Failed to check existence"),
            string `Unexpected error message: ${result.message()}`);
    }
    test:assertFalse(fake.hasTable(TABLE_NAME),
        "init must not attempt to create the table when DescribeTable is inconclusive");
}

@test:Config {}
function testInitToleratesTableCreatedConcurrently() returns error? {
    var [fake, mocked] = newFakePair();
    _ = check new ShortTermMemoryStore(mocked);

    // Model the init race: our DescribeTable reports the table absent, but by the
    // time we call CreateTable another initializer has already made it, so AWS
    // answers ResourceInUseException. The store must treat that as success and
    // simply wait for the table to go ACTIVE.
    fake.setPhantomAbsentDescribes(1);

    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testInitFailsWhenCreateTableErrors() returns error? {
    var [fake, mocked] = newFakePair();
    // A CreateTable failure that is *not* ResourceInUseException (here:
    // LimitExceeded) is fatal — there is no table to fall back on.
    fake.setOpFailure(OP_CREATE_TABLE);

    Error|ShortTermMemoryStore result = new (mocked);

    test:assertTrue(result is Error, "A fatal CreateTable failure must fail init");
    if result is Error {
        test:assertTrue(result.message().includes("Failed to create"),
            string `Unexpected error message: ${result.message()}`);
    }
}

// Disabled by design: the store polls `MAX_TABLE_ACTIVATION_RETRIES` (300) times
// at `TABLE_ACTIVATION_RETRY_INTERVAL` (2s), so driving it to the give-up branch
// takes ~10 minutes of wall clock — too slow for the default suite. Enable it
// when touching the activation-wait loop or either constant.
@test:Config {enable: false}
function testInitFailsWhenTableNeverBecomesActive() returns error? {
    var [fake, mocked] = newFakePair();
    _ = check new ShortTermMemoryStore(mocked);

    // Report CREATING for more polls than the store is willing to make.
    fake.setActivationPolls(MAX_TABLE_ACTIVATION_RETRIES + 1);

    Error|ShortTermMemoryStore result = new (mocked);

    test:assertTrue(result is Error, "init must give up once the activation budget is exhausted");
    if result is Error {
        test:assertTrue(result.message().includes("did not become active"),
            string `Unexpected error message: ${result.message()}`);
    }
}

@test:Config {}
function testInitFailsWhenActivationPollErrors() returns error? {
    var [fake, mocked] = newFakePair();
    _ = check new ShortTermMemoryStore(mocked);

    // Let the existence check through, then fail the DescribeTable that
    // `waitForTableActive` issues. The status is unknown, so init must abort.
    fake.setOpFailure(OP_DESCRIBE_TABLE, count = 1, skip = 1);

    Error|ShortTermMemoryStore result = new (mocked);

    test:assertTrue(result is Error, "A DescribeTable failure while polling must fail init");
    if result is Error {
        test:assertTrue(result.message().includes("Failed to check the status"),
            string `Unexpected error message: ${result.message()}`);
    }
}

@test:Config {}
function testInitToleratesTransientActivationPollErrors() returns error? {
    var [fake, mocked] = newFakePair();
    _ = check new ShortTermMemoryStore(mocked);

    // The DynamoDB control plane is eventually consistent and throttles aggressively, so a
    // DescribeTable issued while polling can fail transiently even though the table is fine.
    // Let the existence check through, then fail the first poll with a retryable error: the
    // store must keep polling within its retry budget instead of failing init.
    fake.setDescribeFailuresTransient(true);
    fake.setOpFailure(OP_DESCRIBE_TABLE, count = 1, skip = 1);

    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, USER_INTRO);
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

// -----------------------------------------------------------------------------
// Data-plane failure propagation.
//
// Every store operation wraps its DynamoDB failures in an `Error`. These tests
// fail one AWS call at a time and assert the operation surfaces it instead of
// returning a partial or empty result.
// -----------------------------------------------------------------------------

@test:Config {}
function testGetSystemMessageFailsWhenGetItemErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, SYSTEM_WEATHER);

    fake.setOpFailure(OP_GET_ITEM);
    ai:ChatSystemMessage|Error? result = store.getChatSystemMessage(K1);

    test:assertTrue(result is Error,
        "A GetItem failure must surface as an error, not as an absent system message");
}

@test:Config {}
function testGetInteractiveMessagesFailsWhenQueryErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, USER_INTRO);

    fake.setOpFailure(OP_QUERY);
    ai:ChatInteractiveMessage[]|Error result = store.getChatInteractiveMessages(K1);

    test:assertTrue(result is Error,
        "A Query failure must surface as an error, not as an empty message list");
}

@test:Config {}
function testGetAllFailsWhenQueryErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO]);

    fake.setOpFailure(OP_QUERY);
    var result = store.getAll(K1);

    test:assertTrue(result is Error,
        "A Query failure must surface as an error, not as an empty message list");
}

@test:Config {}
function testIsFullFailsWhenQueryErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked, 2);
    check store.put(K1, USER_INTRO);

    fake.setOpFailure(OP_QUERY);
    boolean|Error result = store.isFull(K1);

    test:assertTrue(result is Error,
        "isFull must not report `false` when the count query failed");
}

@test:Config {}
function testPutSystemMessageFailsWhenPutItemErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    fake.setOpFailure(OP_CREATE_ITEM);
    Error? result = store.put(K1, SYSTEM_WEATHER);

    test:assertTrue(result is Error, "A failed system-message PutItem must be reported");
    test:assertEquals(fake.peekBody(TABLE_NAME, K1, SYSTEM_MESSAGE_ID), (),
        "Nothing must be persisted when the write failed");
}

@test:Config {}
function testPutInteractiveMessageFailsWhenPutItemErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    fake.setOpFailure(OP_CREATE_ITEM);
    Error? result = store.put(K1, USER_INTRO);

    test:assertTrue(result is Error, "A failed interactive-message PutItem must be reported");
}

@test:Config {}
function testPutFailsWhenSequenceCounterUpdateErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    // Without a sequence number there is no sort key to write under, so the
    // append must abort before touching any item.
    fake.setOpFailure(OP_UPDATE_ITEM);
    Error? result = store.put(K1, USER_INTRO);

    test:assertTrue(result is Error, "A failed counter UpdateItem must be reported");
    test:assertEquals(fake.peekSortIds(TABLE_NAME, K1), [],
        "No message item must be written when the sequence could not be reserved");
}

@test:Config {}
function testPutFailsWhenSequenceCounterResponseHasNoAttributes() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    // A counter update that comes back without `Attributes` must not be read as
    // sequence 0 — that would silently overwrite the first stored message.
    fake.setCounterResponseMode(COUNTER_RESPONSE_NO_ATTRIBUTES);
    Error? result = store.put(K1, USER_INTRO);

    test:assertTrue(result is Error, "A counter response without Attributes must be rejected");
}

@test:Config {}
function testPutFailsWhenSequenceCounterResponseIsNonNumeric() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    fake.setCounterResponseMode(COUNTER_RESPONSE_NON_NUMERIC);
    Error? result = store.put(K1, USER_INTRO);

    test:assertTrue(result is Error, "An unparseable counter value must be rejected");
}

@test:Config {}
function testPutAllFailsWhenBatchWriteErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    // An outright BatchWriteItem error (as opposed to unprocessed items, which
    // are retried) is not retryable and must be surfaced immediately.
    fake.setOpFailure(OP_WRITE_BATCH_ITEMS);
    Error? result = store.put(K1, [USER_INTRO, ASSISTANT_GREETING]);

    test:assertTrue(result is Error, "A failed BatchWriteItem must be reported");
}

@test:Config {}
function testRemoveSystemMessageFailsWhenDeleteItemErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, SYSTEM_WEATHER);

    fake.setOpFailure(OP_DELETE_ITEM);
    Error? result = store.removeChatSystemMessage(K1);

    test:assertTrue(result is Error, "A failed DeleteItem must be reported");
    // The message is still there, so a caller that retries can still delete it.
    check assertSystemMessage(store, K1, SYSTEM_WEATHER);
}

@test:Config {}
function testRemoveInteractiveMessagesFailsWhenQueryErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, USER_INTRO);

    // The sort keys to delete come from a Query; if that fails there is nothing
    // to delete and the caller must hear about it.
    fake.setOpFailure(OP_QUERY);
    Error? result = store.removeChatInteractiveMessages(K1);

    test:assertTrue(result is Error, "A failed sort-key Query must be reported");
    check assertInteractiveMessages(store, K1, [USER_INTRO]);
}

@test:Config {}
function testRemoveInteractiveMessagesFailsWhenBatchWriteErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, [USER_INTRO, ASSISTANT_GREETING]);

    // Let the sort-key Query through and fail the batch delete instead.
    fake.setOpFailure(OP_WRITE_BATCH_ITEMS);
    Error? result = store.removeChatInteractiveMessages(K1, 1);

    test:assertTrue(result is Error, "A failed batch delete must be reported");
    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

@test:Config {}
function testRemoveAllFailsWhenBatchWriteErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO]);

    fake.setOpFailure(OP_WRITE_BATCH_ITEMS);
    Error? result = store.removeAll(K1);

    test:assertTrue(result is Error, "A failed batch delete must be reported");
    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO]);
}

@test:Config {}
function testRemoveAllFailsWhenQueryErrors() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, USER_INTRO);

    fake.setOpFailure(OP_QUERY);
    Error? result = store.removeAll(K1);

    test:assertTrue(result is Error, "A failed sort-key Query must be reported");
}

// -----------------------------------------------------------------------------
// Malformed and unexpected stored data.
//
// The store owns the encoding of every item body, but a body can still be
// corrupted out of band (a manual console edit, a partial migration, another
// writer). Reads must fail loudly rather than hand back a half-decoded message.
// -----------------------------------------------------------------------------

@test:Config {}
function testGetSystemMessageFailsOnMalformedStoredBody() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, SYSTEM_WEATHER);

    fake.putItem(TABLE_NAME, K1, SYSTEM_MESSAGE_ID, "{\"role\":");
    ai:ChatSystemMessage|Error? result = store.getChatSystemMessage(K1);

    test:assertTrue(result is Error, "A corrupt system-message body must fail the read");
}

@test:Config {}
function testGetAllFailsOnMalformedSystemBody() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO]);

    // Valid JSON, but not a system message: the role is missing.
    fake.putItem(TABLE_NAME, K1, SYSTEM_MESSAGE_ID, "{\"content\":\"orphaned\"}");
    var result = store.getAll(K1);

    test:assertTrue(result is Error, "A corrupt system-message body must fail getAll");
}

@test:Config {}
function testGetAllFailsOnMalformedInteractiveBody() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, USER_INTRO);

    string interactiveSortId = check firstInteractiveSortId(fake, TABLE_NAME, K1);
    fake.putItem(TABLE_NAME, K1, interactiveSortId, "not json at all");
    var result = store.getAll(K1);

    test:assertTrue(result is Error, "A corrupt interactive-message body must fail getAll");
}

@test:Config {}
function testGetInteractiveMessagesFailsOnMalformedInteractiveBody() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, USER_INTRO);

    string interactiveSortId = check firstInteractiveSortId(fake, TABLE_NAME, K1);
    // Valid JSON of the wrong shape — an unknown role the store cannot map.
    fake.putItem(TABLE_NAME, K1, interactiveSortId, "{\"role\":\"alien\",\"content\":\"hi\"}");
    ai:ChatInteractiveMessage[]|Error result = store.getChatInteractiveMessages(K1);

    test:assertTrue(result is Error, "A corrupt interactive-message body must fail the read");
}

@test:Config {}
function testGetAllSkipsQueryRowsWithoutAnItem() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    check store.put(K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);

    // A Query page that carries no `Item` (which the connector's stream can
    // yield) must be skipped rather than counted as a message.
    fake.setEmitItemlessQueryRow(true);

    check assertAllMessages(store, K1, [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING]);
    check assertInteractiveMessages(store, K1, [USER_INTRO, ASSISTANT_GREETING]);
}

// -----------------------------------------------------------------------------
// Shared assertions.
// -----------------------------------------------------------------------------

// Returns the sort key of the first interactive item stored under `key`, so a
// test can corrupt a message body without hard-coding the padded sequence.
function firstInteractiveSortId(FakeStorage fake, string tableName, string key) returns string|error {
    foreach string sortId in fake.peekSortIds(tableName, key) {
        if sortId.startsWith(INTERACTIVE_ID_PREFIX) {
            return sortId;
        }
    }
    return error(string `No interactive item found under '${key}'`);
}

function assertAllMessages(ShortTermMemoryStore store, string key, ai:ChatMessage[] expected) returns error? {
    ai:ChatMessage[] actual = check store.getAll(key);
    test:assertEquals(actual.length(), expected.length(),
        string `getAll(${key}) length mismatch`);
    foreach int i in 0 ..< actual.length() {
        assertChatMessageEquals(actual[i], expected[i]);
    }
}

function assertSystemMessage(ShortTermMemoryStore store, string key, ai:ChatSystemMessage? expected) returns error? {
    ai:ChatSystemMessage? actual = check store.getChatSystemMessage(key);
    if expected is () && actual is () {
        return;
    }
    if expected is () || actual is () {
        test:assertFail(string `getChatSystemMessage(${key}) presence mismatch`);
    }
    assertChatMessageEquals(actual, expected);
}

function assertInteractiveMessages(ShortTermMemoryStore store, string key,
        ai:ChatInteractiveMessage[] expected) returns error? {
    ai:ChatInteractiveMessage[] actual = check store.getChatInteractiveMessages(key);
    test:assertEquals(actual.length(), expected.length(),
        string `getChatInteractiveMessages(${key}) length mismatch`);
    foreach int i in 0 ..< actual.length() {
        assertChatMessageEquals(actual[i], expected[i]);
    }
}

isolated function assertChatMessageEquals(ai:ChatMessage actual, ai:ChatMessage expected) {
    if (actual is ai:ChatUserMessage && expected is ai:ChatUserMessage) ||
            (actual is ai:ChatSystemMessage && expected is ai:ChatSystemMessage) {
        test:assertEquals(actual.role, expected.role);
        assertContentEquals(actual.content, expected.content);
        test:assertEquals(actual.name, expected.name);
        return;
    }
    if actual is ai:ChatFunctionMessage && expected is ai:ChatFunctionMessage {
        test:assertEquals(actual.role, expected.role);
        test:assertEquals(actual.name, expected.name);
        test:assertEquals(actual.id, expected.id);
        return;
    }
    if actual is ai:ChatAssistantMessage && expected is ai:ChatAssistantMessage {
        test:assertEquals(actual.role, expected.role);
        test:assertEquals(actual.content, expected.content);
        test:assertEquals(actual.name, expected.name);
        test:assertEquals(actual.toolCalls, expected.toolCalls);
        return;
    }
    test:assertFail("ChatMessage type mismatch");
}

isolated function assertContentEquals(ai:Prompt|string actual, ai:Prompt|string expected) {
    if actual is string && expected is string {
        test:assertEquals(actual, expected);
        return;
    }
    if actual is ai:Prompt && expected is ai:Prompt {
        test:assertEquals(actual.strings, expected.strings);
        test:assertEquals(actual.insertions, expected.insertions);
        return;
    }
    test:assertFail("Message content type mismatch");
}

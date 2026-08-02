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

// Unit tests for the human-in-the-loop checkpoint operations on
// `ShortTermMemoryStore`, run against the same in-memory `FakeDynamoDbClient`
// used by `memory_store_test.bal`.

import ballerina/ai;
import ballerina/test;
import ballerinax/aws.dynamodb;

final readonly & ai:ChatFunctionMessage LOOKUP_ORDER_RESULT = {
    role: "function",
    name: "lookupOrder",
    content: "{\"id\":\"ORD-1\"}"
};

function buildPendingApproval(string sessionId) returns ai:PendingApproval {
    ai:FunctionCall toolCall = {name: "issueRefund", arguments: {orderId: "ORD-1", amount: 20}, id: "call-1"};
    ai:ApprovalRequest request = {
        id: "req-1",
        sessionId,
        toolName: "issueRefund",
        toolDescription: "Issues a refund for an order",
        arguments: {orderId: "ORD-1", amount: 20},
        toolCallId: "call-1",
        batchIndex: 0
    };
    ai:Iteration iteration = {
        history: [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING],
        output: [ASSISTANT_GREETING, LOOKUP_ORDER_RESULT],
        startTime: [1700000000, 0.5d],
        endTime: [1700000001, 0.25d]
    };
    return {
        sessionId,
        executionId: "exec-1",
        iterationsUsed: 1,
        history: [SYSTEM_WEATHER, USER_INTRO, ASSISTANT_GREETING, LOOKUP_ORDER_RESULT],
        historyPrefixLength: 2,
        iterations: [iteration],
        toolCalls: [toolCall],
        startTime: [1700000000, 0d],
        originalBatch: [toolCall],
        pendingRequests: [request],
        decisions: [()]
    };
}

// `PendingApproval` is not statically `anydata` (its `history`/`iterations` admit `Prompt` and
// `Error`), so compare via the database-storable form, which captures every persisted field.
function assertCheckpointEquals(ai:PendingApproval? actual, ai:PendingApproval expected) {
    if actual !is ai:PendingApproval {
        test:assertFail("expected a persisted checkpoint but found none");
    }
    test:assertEquals(toApprovalDatabaseMessage(actual), toApprovalDatabaseMessage(expected));
}

function assertNoCheckpoint(ai:PendingApproval? actual) {
    test:assertEquals(actual, ());
}

@test:Config {}
function testCheckpointOperationsDoNotTouchTableLifecycle() returns error? {
    var [fake, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    // Arm every subsequent describeTable/createTable call to fail. Checkpoint operations must
    // still succeed - proof that they never touch table lifecycle (no second table is ever
    // created or checked for) and only ever issue GetItem/PutItem/DeleteItem against the table
    // the store already has, under a reserved partition key.
    fake.setOpFailure(OP_DESCRIBE_TABLE, 1000);
    fake.setOpFailure(OP_CREATE_TABLE, 1000);

    ai:PendingApproval approval = buildPendingApproval(K1);
    check store.putCheckpoint(approval);
    assertCheckpointEquals(check store.getCheckpoint(K1), approval);
    check store.removeCheckpoint(K1);
    assertNoCheckpoint(check store.getCheckpoint(K1));
}

@test:Config {}
function testReservedSessionKeyRejectedOnMessageMethods() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    string reserved = "checkpoint#alice";

    // A message session key equal to the reserved prefix + another session's name would collide
    // with that other session's checkpoint partition, so every entry point that turns a raw key
    // into a partition key must reject it - not just the write path, since a read or delete keyed
    // by the reserved value would just as readily touch the other session's checkpoint data.
    Error? putResult = store.put(reserved, USER_INTRO);
    test:assertTrue(putResult is Error);
    if putResult is Error {
        test:assertTrue(putResult.message().includes("reserved for checkpoint storage"));
    }

    ai:ChatSystemMessage|Error? getResult = store.getChatSystemMessage(reserved);
    test:assertTrue(getResult is Error);

    Error? removeResult = store.removeAll(reserved);
    test:assertTrue(removeResult is Error);

    boolean|Error fullResult = store.isFull(reserved);
    test:assertTrue(fullResult is Error);
}

@test:Config {}
function testReservedSessionKeyRejectedOnCheckpointMethods() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);
    string reserved = "checkpoint#alice";

    Error? putResult = store.putCheckpoint(buildPendingApproval(reserved));
    test:assertTrue(putResult is Error);
    if putResult is Error {
        test:assertTrue(putResult.message().includes("reserved for checkpoint storage"));
    }

    ai:PendingApproval?|Error getResult = store.getCheckpoint(reserved);
    test:assertTrue(getResult is Error);

    Error? removeResult = store.removeCheckpoint(reserved);
    test:assertTrue(removeResult is Error);

    ai:PendingApproval?|Error takeResult = store.takeCheckpoint(reserved);
    test:assertTrue(takeResult is Error);
}

@test:Config {}
function testNonReservedSessionKeyNotAffected() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    // A session key that merely contains the reserved prefix, but doesn't start with it, is fine.
    string notReserved = "alice-checkpoint#session";
    check store.put(notReserved, USER_INTRO);
    check store.putCheckpoint(buildPendingApproval(notReserved));
    assertCheckpointEquals(check store.getCheckpoint(notReserved), buildPendingApproval(notReserved));
}

@test:Config {}
function testCheckpointPersistAndRetrieve() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    assertNoCheckpoint(check store.getCheckpoint(K1));

    ai:PendingApproval approval = buildPendingApproval(K1);
    check store.putCheckpoint(approval);

    // getCheckpoint returns an equal value and leaves it in place.
    assertCheckpointEquals(check store.getCheckpoint(K1), approval);
    assertCheckpointEquals(check store.getCheckpoint(K1), approval);

    // A checkpoint is scoped to its session.
    assertNoCheckpoint(check store.getCheckpoint(K2));
}

@test:Config {}
function testCheckpointReplace() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.putCheckpoint(buildPendingApproval(K1));

    ai:PendingApproval updated = buildPendingApproval(K1);
    updated.executionId = "exec-2";
    check store.putCheckpoint(updated);

    assertCheckpointEquals(check store.getCheckpoint(K1), updated);
}

@test:Config {}
function testTakeCheckpointClaimsAtomically() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    assertNoCheckpoint(check store.takeCheckpoint(K1));

    ai:PendingApproval approval = buildPendingApproval(K1);
    check store.putCheckpoint(approval);

    assertCheckpointEquals(check store.takeCheckpoint(K1), approval);

    // The checkpoint should no longer be present after being taken.
    assertNoCheckpoint(check store.takeCheckpoint(K1));
    assertNoCheckpoint(check store.getCheckpoint(K1));
}

@test:Config {}
function testRemoveCheckpoint() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.putCheckpoint(buildPendingApproval(K1));
    check store.removeCheckpoint(K1);
    assertNoCheckpoint(check store.getCheckpoint(K1));

    // Removing a checkpoint that doesn't exist should be a no-op, not an error.
    check store.removeCheckpoint(K2);
}

@test:Config {}
function testCheckpointClearedOnRemoveAll() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.put(K1, SYSTEM_WEATHER);
    check store.putCheckpoint(buildPendingApproval(K1));

    check store.removeAll(K1);

    test:assertEquals(check store.getChatSystemMessage(K1), ());
    assertNoCheckpoint(check store.getCheckpoint(K1));
}

@test:Config {}
function testRemoveAllOfOneSessionDoesNotTouchAnotherSessionsCheckpoint() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    check store.putCheckpoint(buildPendingApproval(K1));
    check store.putCheckpoint(buildPendingApproval(K2));

    check store.removeAll(K1);

    assertNoCheckpoint(check store.getCheckpoint(K1));
    assertCheckpointEquals(check store.getCheckpoint(K2), buildPendingApproval(K2));
}

@test:Config {}
function testCheckpointErrorOutputStringified() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    ai:PendingApproval approval = buildPendingApproval(K1);
    approval.iterations[0].output = [
        ASSISTANT_GREETING,
        error ai:Error("tool execution failed", cause = error("timeout"))
    ];
    check store.putCheckpoint(approval);

    ai:PendingApproval? retrieved = check store.getCheckpoint(K1);
    if retrieved !is ai:PendingApproval {
        test:assertFail("expected a persisted checkpoint");
    }
    var restoredError = retrieved.iterations[0].output[1];
    if restoredError !is error {
        test:assertFail("expected the second output entry to be an error");
    }
    test:assertTrue(restoredError.message().includes("tool execution failed"));
}

@test:Config {}
function testCheckpointWithPromptContent() returns error? {
    var [_, mocked] = newFakePair();
    ShortTermMemoryStore store = check new (mocked);

    string city = "Seattle";
    ai:Prompt prompt = `What is the weather in ${city}?`;
    ai:ChatUserMessage userMessage = {role: ai:USER, content: prompt};

    ai:PendingApproval approval = buildPendingApproval(K1);
    approval.history = [userMessage];
    check store.putCheckpoint(approval);

    ai:PendingApproval? retrieved = check store.getCheckpoint(K1);
    if retrieved !is ai:PendingApproval {
        test:assertFail("expected a persisted checkpoint");
    }
    var restored = retrieved.history[0];
    if restored !is ai:ChatUserMessage {
        test:assertFail("expected a user message");
    }
    ai:Prompt|string content = restored.content;
    if content !is ai:Prompt {
        test:assertFail("expected the prompt content to round-trip as a Prompt");
    }
    test:assertEquals(content.strings, prompt.strings);
    test:assertEquals(content.insertions, prompt.insertions);
}

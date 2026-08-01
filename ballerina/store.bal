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

import ballerina/ai;
import ballerina/lang.regexp;
import ballerina/lang.runtime;
import ballerina/random;
import ballerinax/aws.dynamodb;

# Represents a distinct error type for memory store errors.
public type Error distinct ai:MemoryError;

# Configuration for the DynamoDB table that backs the short-term memory store.
#
# + tableName - The name of the DynamoDB table used to store chat messages.
# Must be 3-255 characters long and contain only letters, digits, underscores, dots, and hyphens
# + createTableIfNotExists - Whether the store should create the backing table when it does not
# already exist. Defaults to `true`. When `true`, initialization calls `DescribeTable` (and
# `CreateTable` if the table is absent), which requires the `dynamodb:DescribeTable` and
# `dynamodb:CreateTable` IAM permissions. Set to `false` when the table is provisioned out of band
# (e.g. via IaC) and the runtime role is restricted to data-plane permissions; in that case the
# store performs no control-plane calls during initialization and assumes the table already exists
# + billingMode - The billing mode to request when the connector creates the table. Defaults to
# `dynamodb:PAY_PER_REQUEST` (on-demand). Note that this differs from the AWS `CreateTable` API
# default of `PROVISIONED`; set this explicitly to `dynamodb:PROVISIONED` (and provide
# `readCapacityUnits`/`writeCapacityUnits`) for provisioned-capacity tables. Ignored when
# `createTableIfNotExists` is `false` or the table already exists
# + readCapacityUnits - The read capacity units to provision when `billingMode` is `dynamodb:PROVISIONED`
# + writeCapacityUnits - The write capacity units to provision when `billingMode` is `dynamodb:PROVISIONED`
# + consistentReads - Whether reads against DynamoDB use strongly consistent reads. Defaults to `false`
# (eventually consistent), matching the DynamoDB default. Strongly consistent reads cost twice the read
# capacity units of eventually consistent reads; set to `true` only when strong consistency is required
# + tags - Optional tags to apply to the DynamoDB table when the connector creates it. Ignored if the
# table already exists
# + sseSpecification - Optional server-side encryption settings to apply when the connector creates
# the table. If omitted, the table uses the default AWS-owned encryption key. Ignored if the table
# already exists
@display {label: "Table Configuration"}
public type TableConfig record {|
    @display {label: "Table Name"}
    string tableName = "chat_memory";
    @display {label: "Create Table If Not Exists"}
    boolean createTableIfNotExists = true;
    @display {label: "Billing Mode"}
    dynamodb:BillingMode billingMode = dynamodb:PAY_PER_REQUEST;
    @display {label: "Read Capacity Units"}
    int readCapacityUnits = 5;
    @display {label: "Write Capacity Units"}
    int writeCapacityUnits = 5;
    @display {label: "Consistent Reads"}
    boolean consistentReads = false;
    @display {label: "Tags"}
    dynamodb:Tag[]? tags = ();
    @display {label: "Server-Side Encryption"}
    dynamodb:SSESpecification? sseSpecification = ();
|};

// The name of the partition (HASH) key attribute. Holds the memory/session key.
const string PARTITION_KEY_ATTRIBUTE = "MemoryKey";
// The name of the sort (RANGE) key attribute. Holds the per-item message identifier.
const string SORT_KEY_ATTRIBUTE = "MessageId";
// The name of the attribute that stores the JSON-encoded message body.
const string BODY_ATTRIBUTE = "Body";
// The name of the numeric attribute that holds the per-key interactive message sequence.
const string SEQUENCE_ATTRIBUTE = "Seq";

// The fixed sort key of the (singleton) system message item for a key.
const string SYSTEM_MESSAGE_ID = "system";
// The fixed sort key of the per-key interactive message sequence counter item.
const string COUNTER_MESSAGE_ID = "counter";
// The sort key prefix for interactive message items. Interactive items sort after
// `counter` and before `system`, so `begins_with` cleanly isolates them.
const string INTERACTIVE_ID_PREFIX = "msg#";

// Zero-pad interactive sequence numbers to this width so that the lexicographic
// order of the sort key matches the numeric insertion order.
const int SEQUENCE_PAD_WIDTH = 19;
// The maximum number of write requests DynamoDB accepts in a single BatchWriteItem call.
const int MAX_BATCH_WRITE_SIZE = 25;
// The maximum number of retries for unprocessed items returned by BatchWriteItem.
const int MAX_BATCH_WRITE_RETRIES = 5;
// Full-jitter exponential backoff for `UnprocessedItems` retries, as recommended by the AWS
// DynamoDB Developer Guide. The actual sleep is `random(0, min(MAX, BASE * 2^attempt))` seconds.
const decimal BATCH_WRITE_BASE_DELAY = 0.1;
const decimal BATCH_WRITE_MAX_DELAY = 20.0;
// The maximum number of polls while waiting for a newly created table to become active.
// 300 × 2s ≈ 10 minutes, matching the AWS Java SDK v2 `tableExists` waiter default — long
// enough to absorb slow first-creates in some regions, while the short interval keeps
// detection latency low when activation is fast.
const int MAX_TABLE_ACTIVATION_RETRIES = 300;
const decimal TABLE_ACTIVATION_RETRY_INTERVAL = 2;
// AWS error keywords that make a `DescribeTable` failure worth re-polling while waiting for a
// table to become active: the control plane is eventually consistent right after `CreateTable`
// (so the table can briefly be reported absent), control-plane calls are throttled
// aggressively, and server-side faults are by definition retryable. Anything else — a
// permission or validation failure, say — leaves the status unknown for good and aborts.
final readonly & string[] RETRYABLE_DESCRIBE_TABLE_ERROR_KEYWORDS = [
    "ResourceNotFound",
    "Throttling",
    "ProvisionedThroughputExceeded",
    "RequestLimitExceeded",
    "InternalServerError",
    "InternalFailure",
    "ServiceUnavailable"
];

# Represents a DynamoDB-backed short-term memory store for messages.
@display {label: "Amazon DynamoDB Short Term Memory Store"}
public isolated class ShortTermMemoryStore {
    *ai:ShortTermMemoryStore;

    private final dynamodb:Client dynamodbClient;
    private final int maxMessagesPerKey;
    private final string tableName;
    private final boolean consistentReads;

    # Initializes the DynamoDB-backed short-term memory store.
    #
    # + dbConnection - The DynamoDB client or connection configuration to connect to DynamoDB
    # + maxMessagesPerKey - The maximum number of interactive messages to store per key
    # + tableConfig - Configuration for the DynamoDB table that backs the store, including the table
    # name, whether to auto-create the table, billing mode, provisioned throughput, read consistency,
    # tags, and server-side encryption
    # + returns - An error if the initialization fails
    public isolated function init(@display {label: "Database Connection"} dynamodb:ConnectionConfig|dynamodb:Client dbConnection,
            @display {label: "Max Messages Per Key"} int maxMessagesPerKey = 20,
            @display {label: "Table Configuration"} TableConfig tableConfig = {}) returns Error? {
        if !isValidTableName(tableConfig.tableName) {
            return error(string `Invalid table name: '${tableConfig.tableName}'.`
                + " Table name must be 3-255 characters long and can only contain "
                + "letters, digits, underscores, dots, and hyphens.");
        }
        if maxMessagesPerKey < 1 {
            return error(string `Invalid maxMessagesPerKey: '${maxMessagesPerKey}'.`
                + " It must be a positive integer.");
        }
        if tableConfig.billingMode == dynamodb:PROVISIONED
                && (tableConfig.readCapacityUnits < 1 || tableConfig.writeCapacityUnits < 1) {
            return error(string `Invalid provisioned throughput: readCapacityUnits `
                + string `'${tableConfig.readCapacityUnits}' and writeCapacityUnits `
                + string `'${tableConfig.writeCapacityUnits}' must both be positive integers `
                + "when billingMode is dynamodb:PROVISIONED.");
        }
        self.tableName = tableConfig.tableName;
        if dbConnection is dynamodb:Client {
            self.dynamodbClient = dbConnection;
        } else {
            dynamodb:Client|error initializedClient = new (dbConnection);
            if initializedClient is error {
                return error("Failed to create DynamoDB client: " + initializedClient.message(), initializedClient);
            }
            self.dynamodbClient = initializedClient;
        }
        self.maxMessagesPerKey = maxMessagesPerKey;
        self.consistentReads = tableConfig.consistentReads;
        if tableConfig.createTableIfNotExists {
            check self.initializeTable(tableConfig);
        }
    }

    # Retrieves the system message, if it was provided, for a given key.
    #
    # + key - The key associated with the memory
    # + return - A copy of the message if it was specified, nil if it was not, or an
    # `Error` error if the operation fails
    public isolated function getChatSystemMessage(string key) returns ai:ChatSystemMessage|Error? {
        string|Error? systemMessageJson = self.getMessageBody(key, SYSTEM_MESSAGE_ID);

        if systemMessageJson is () {
            return ();
        }

        if systemMessageJson is Error {
            return error("Failed to retrieve system message: " + systemMessageJson.message(), systemMessageJson);
        }

        ChatSystemMessageDatabaseMessage|error dbMessage = systemMessageJson.fromJsonStringWithType();
        if dbMessage is error {
            return error("Failed to parse chat message from DynamoDB: " + dbMessage.message(), dbMessage);
        }

        return transformFromSystemMessageDatabaseMessage(dbMessage);
    }

    # Retrieves all stored interactive chat messages (i.e., all chat messages except the system
    # message) for a given key.
    #
    # + key - The key associated with the memory
    # + return - A copy of the messages, or an `Error` error if the operation fails
    public isolated function getChatInteractiveMessages(string key) returns ai:ChatInteractiveMessage[]|Error {
        // `getAllFromDynamoDb` already wraps its failures, so they are surfaced as-is here.
        final var allMessages = check self.getAllFromDynamoDb(key);
        if allMessages is readonly & ai:ChatInteractiveMessage[] {
            return allMessages;
        }
        var [_, ...interactiveMessages] = allMessages;
        return interactiveMessages;
    }

    # Retrieves all stored chat messages for a given key.
    #
    # + key - The key associated with the memory
    # + return - A copy of the messages, or an `Error` error if the operation fails
    public isolated function getAll(string key)
            returns [ai:ChatSystemMessage, ai:ChatInteractiveMessage...]|ai:ChatInteractiveMessage[]|Error {
        // `getAllFromDynamoDb` already wraps its failures, so they are surfaced as-is here.
        return self.getAllFromDynamoDb(key);
    }

    # Adds one or more chat messages to the memory store for a given key.
    #
    # + key - The key associated with the memory
    # + message - The `ChatMessage` message or messages to store. If multiple
    #             `ChatSystemMessage` values are provided in an array, only the last one is
    #             persisted; earlier system messages in the array are discarded.
    # + return - nil on success, or an `Error` if the operation fails
    public isolated function put(string key, ai:ChatMessage|ai:ChatMessage[] message) returns Error? {
        if message is ai:ChatMessage[] {
            return self.putAll(key, message);
        }
        ChatMessageDatabaseMessage dbMessage = transformToDatabaseMessage(message);
        if dbMessage is ChatSystemMessageDatabaseMessage {
            return self.putSystemItem(key, dbMessage.toJsonString());
        }
        return self.appendInteractiveItems(key, [dbMessage.toJsonString()]);
    }

    private isolated function putAll(string key, ai:ChatMessage[] messages) returns Error? {
        if messages.length() == 0 {
            return;
        }

        final var [newSystemMessages, newInteractiveMessages] = partitionMessagesByType(messages);
        final ai:ChatSystemMessage? finalChatSystemMessage = getLatestSystemMessage(newSystemMessages);

        // The system PutItem and the interactive BatchWriteItem are separate calls and not
        // atomic. DynamoDB does not support a multi-item transaction through the operations
        // exposed by the connector. Cap enforcement is the wrapper's responsibility
        // (`ai:ShortTermMemory`), so the store does not pre-validate against `maxMessagesPerKey`.
        if finalChatSystemMessage is ai:ChatSystemMessage {
            ChatMessageDatabaseMessage dbMessage = transformToDatabaseMessage(finalChatSystemMessage);
            check self.putSystemItem(key, dbMessage.toJsonString());
        }

        if newInteractiveMessages.length() > 0 {
            string[] jsonValues = from ai:ChatInteractiveMessage msg in newInteractiveMessages
                let ChatMessageDatabaseMessage dbMsg = transformToDatabaseMessage(msg)
                select dbMsg.toJsonString();
            check self.appendInteractiveItems(key, jsonValues);
        }
    }

    # Removes the system chat message, if specified, for a given key.
    #
    # + key - The key associated with the memory
    # + return - nil on success or if there is no system chat message against the key,
    # or an `Error` error if the operation fails
    public isolated function removeChatSystemMessage(string key) returns Error? {
        dynamodb:ItemDeleteInput deleteInput = {
            TableName: self.tableName,
            Key: itemKey(key, SYSTEM_MESSAGE_ID)
        };
        dynamodb:ItemDescription|error deleteResult = self.dynamodbClient->deleteItem(deleteInput);
        if deleteResult is error {
            return error("Failed to delete existing system message: " + deleteResult.message(), deleteResult);
        }
    }

    # Removes all stored interactive chat messages (i.e., all chat messages except the system
    # message) for a given key.
    #
    # + key - The key associated with the memory
    # + count - Optional number of messages to remove, starting from the first interactive message in;
    # if not provided, removes all messages
    # + return - nil on success, or an `Error` error if the operation fails
    public isolated function removeChatInteractiveMessages(string key, int? count = ()) returns Error? {
        if count is int && count < 0 {
            return error("Invalid count: must be >= 0");
        }

        do {
            string[] sortIds = check self.querySortIds(key, true);
            if count is () {
                // Removing every interactive message also drops the sequence counter,
                // leaving a clean slate for the key.
                check self.deleteItems(key, [...sortIds, COUNTER_MESSAGE_ID]);
            } else {
                int removeCount = count < sortIds.length() ? count : sortIds.length();
                if removeCount > 0 {
                    check self.deleteItems(key, sortIds.slice(0, removeCount));
                }
            }
        } on fail Error err {
            return error("Failed to delete chat messages: " + err.message(), err);
        }
    }

    # Removes all stored chat messages for a given key.
    #
    # + key - The key associated with the memory
    # + return - nil on success, or an `Error` error if the operation fails
    public isolated function removeAll(string key) returns Error? {
        do {
            string[] sortIds = check self.querySortIds(key, false);
            check self.deleteItems(key, sortIds);
        } on fail Error err {
            return error("Failed to delete chat messages: " + err.message(), err);
        }
    }

    # Checks if the memory store is full for a given key.
    #
    # + key - The key associated with the memory
    # + return - true if the memory store is full, false otherwise, or an `Error` error if the operation fails
    public isolated function isFull(string key) returns boolean|Error {
        int count = check self.countInteractiveMessages(key);
        return count >= self.maxMessagesPerKey;
    }

    # Retrieves the maximum number of interactive messages that can be stored for each key.
    #
    # + return - The configured capacity of the message store per key
    public isolated function getCapacity() returns int {
        return self.maxMessagesPerKey;
    }

    // Ensures the backing table exists and is active, creating it if necessary.
    private isolated function initializeTable(TableConfig tableConfig) returns Error? {
        dynamodb:TableDescription|error existing = self.dynamodbClient->describeTable(self.tableName);
        if existing is dynamodb:TableDescription {
            return self.waitForTableActive();
        }
        if !errorMentions(existing, "ResourceNotFound") {
            return error(string `Failed to check existence of the '${self.tableName}' table: ${existing.message()}`,
                existing);
        }

        dynamodb:TableCreateInput createInput = {
            TableName: self.tableName,
            AttributeDefinitions: [
                {AttributeName: PARTITION_KEY_ATTRIBUTE, AttributeType: dynamodb:S},
                {AttributeName: SORT_KEY_ATTRIBUTE, AttributeType: dynamodb:S}
            ],
            KeySchema: [
                {AttributeName: PARTITION_KEY_ATTRIBUTE, KeyType: dynamodb:HASH},
                {AttributeName: SORT_KEY_ATTRIBUTE, KeyType: dynamodb:RANGE}
            ],
            BillingMode: tableConfig.billingMode
        };
        if tableConfig.billingMode == dynamodb:PROVISIONED {
            createInput.ProvisionedThroughput = {
                ReadCapacityUnits: tableConfig.readCapacityUnits,
                WriteCapacityUnits: tableConfig.writeCapacityUnits
            };
        }
        dynamodb:Tag[]? tags = tableConfig.tags;
        if tags is dynamodb:Tag[] && tags.length() > 0 {
            createInput.Tags = tags;
        }
        dynamodb:SSESpecification? sseSpecification = tableConfig.sseSpecification;
        if sseSpecification is dynamodb:SSESpecification {
            createInput.SSESpecification = sseSpecification;
        }

        dynamodb:TableDescription|error created = self.dynamodbClient->createTable(createInput);
        if created is error {
            // A concurrent initializer may have created the table first.
            if errorMentions(created, "ResourceInUse") {
                return self.waitForTableActive();
            }
            return error(string `Failed to create the '${self.tableName}' table: ${created.message()}`, created);
        }
        return self.waitForTableActive();
    }

    // Polls the table until its status is `ACTIVE`. The DynamoDB control plane is eventually
    // consistent, so right after `CreateTable` a `DescribeTable` can transiently report the
    // table as absent or throttle the caller even though the table is on its way to `ACTIVE`.
    // Such failures are re-polled within the same retry budget and only surface once the
    // budget is exhausted; any other failure (e.g. `AccessDenied`) aborts immediately.
    private isolated function waitForTableActive() returns Error? {
        error? lastTransientError = ();
        foreach int _ in 0 ..< MAX_TABLE_ACTIVATION_RETRIES {
            dynamodb:TableDescription|error description = self.dynamodbClient->describeTable(self.tableName);
            if description is error {
                if !isRetryableDescribeTableError(description) {
                    return error(string `Failed to check the status of the '${self.tableName}' table: `
                        + description.message(), description);
                }
                lastTransientError = description;
            } else if description?.TableStatus == dynamodb:ACTIVE {
                return;
            }
            runtime:sleep(TABLE_ACTIVATION_RETRY_INTERVAL);
        }
        if lastTransientError is error {
            return error(string `The '${self.tableName}' table did not become active within the expected time: `
                + lastTransientError.message(), lastTransientError);
        }
        return error(string `The '${self.tableName}' table did not become active within the expected time.`);
    }

    // Retrieves the JSON body string of a single item, or nil if the item does not exist.
    private isolated function getMessageBody(string key, string sortId) returns string|Error? {
        dynamodb:ItemGetInput getInput = {
            TableName: self.tableName,
            Key: itemKey(key, sortId),
            ConsistentRead: self.consistentReads,
            ProjectionExpression: "#body",
            ExpressionAttributeNames: {"#body": BODY_ATTRIBUTE}
        };
        dynamodb:ItemGetOutput|error result = self.dynamodbClient->getItem(getInput);
        if result is error {
            return error("Failed to retrieve message from DynamoDB: " + result.message(), result);
        }
        map<dynamodb:AttributeValue>? item = result?.Item;
        if item is () {
            return ();
        }
        return extractBody(item);
    }

    // Stores (overwriting any existing value) the singleton system message item.
    private isolated function putSystemItem(string key, string body) returns Error? {
        dynamodb:ItemCreateInput createInput = {
            TableName: self.tableName,
            Item: {
                [PARTITION_KEY_ATTRIBUTE]: {S: key},
                [SORT_KEY_ATTRIBUTE]: {S: SYSTEM_MESSAGE_ID},
                [BODY_ATTRIBUTE]: {S: body}
            }
        };
        dynamodb:ItemDescription|error result = self.dynamodbClient->createItem(createInput);
        if result is error {
            return error("Failed to set system message: " + result.message(), result);
        }
    }

    // Appends interactive message items in insertion order using a monotonic per-key counter.
    private isolated function appendInteractiveItems(string key, string[] bodies) returns Error? {
        if bodies.length() == 0 {
            return;
        }
        int endSequence = check self.incrementCounter(key, bodies.length());
        int startSequence = endSequence - bodies.length() + 1;

        // The hot path is `put(key, oneMessage)`. A 1-item BatchWriteItem carries the
        // `RequestItems` map wrapper and the `UnprocessedItems` retry plumbing for no
        // benefit, so fall through to a direct PutItem here.
        if bodies.length() == 1 {
            string sortId = INTERACTIVE_ID_PREFIX + paddedSequence(startSequence);
            dynamodb:ItemCreateInput createInput = {
                TableName: self.tableName,
                Item: {
                    [PARTITION_KEY_ATTRIBUTE]: {S: key},
                    [SORT_KEY_ATTRIBUTE]: {S: sortId},
                    [BODY_ATTRIBUTE]: {S: bodies[0]}
                }
            };
            dynamodb:ItemDescription|error result = self.dynamodbClient->createItem(createInput);
            if result is error {
                return error("Failed to append interactive message: " + result.message(), result);
            }
            return;
        }

        dynamodb:WriteRequest[] writeRequests = [];
        foreach int i in 0 ..< bodies.length() {
            string sortId = INTERACTIVE_ID_PREFIX + paddedSequence(startSequence + i);
            writeRequests.push({
                PutRequest: {
                    Item: {
                        [PARTITION_KEY_ATTRIBUTE]: {S: key},
                        [SORT_KEY_ATTRIBUTE]: {S: sortId},
                        [BODY_ATTRIBUTE]: {S: bodies[i]}
                    }
                }
            });
        }
        return self.executeBatchWrite(writeRequests);
    }

    // Atomically increments the per-key sequence counter by `delta`, returning the new value.
    private isolated function incrementCounter(string key, int delta) returns int|Error {
        dynamodb:ItemUpdateInput updateInput = {
            TableName: self.tableName,
            Key: itemKey(key, COUNTER_MESSAGE_ID),
            UpdateExpression: "ADD #seq :delta",
            ExpressionAttributeNames: {"#seq": SEQUENCE_ATTRIBUTE},
            ExpressionAttributeValues: {":delta": {N: delta.toString()}},
            ReturnValues: dynamodb:UPDATED_NEW
        };
        dynamodb:ItemDescription|error result = self.dynamodbClient->updateItem(updateInput);
        if result is error {
            return error("Failed to update the message sequence counter: " + result.message(), result);
        }
        map<dynamodb:AttributeValue>? attributes = result?.Attributes;
        dynamodb:AttributeValue? sequenceAttr = attributes is () ? () : attributes[SEQUENCE_ATTRIBUTE];
        string? sequenceValue = sequenceAttr is () ? () : sequenceAttr?.N;
        if sequenceValue is () {
            return error("The message sequence counter update did not return a numeric value.");
        }
        int|error sequence = int:fromString(sequenceValue);
        if sequence is error {
            return error("Failed to parse the message sequence counter value: " + sequence.message(), sequence);
        }
        return sequence;
    }

    // Counts the interactive message items currently stored for a key. Used only by `isFull`.
    // DynamoDB's `Select=COUNT` would be a closer fit (server returns only the aggregate count),
    // but the upstream `ballerinax/aws.dynamodb` connector strips the `Count` field from its
    // `QueryOutput`, so the items have to be iterated. The projection is kept to a single
    // small attribute (the sort key) to minimize transfer.
    private isolated function countInteractiveMessages(string key) returns int|Error {
        do {
            dynamodb:QueryInput queryInput = {
                TableName: self.tableName,
                ConsistentRead: self.consistentReads,
                ProjectionExpression: "#sk",
                KeyConditionExpression: "#pk = :pk and begins_with(#sk, :prefix)",
                ExpressionAttributeNames: {"#pk": PARTITION_KEY_ATTRIBUTE, "#sk": SORT_KEY_ATTRIBUTE},
                ExpressionAttributeValues: {":pk": {S: key}, ":prefix": {S: INTERACTIVE_ID_PREFIX}}
            };
            stream<dynamodb:QueryOutput, error?> resultStream = check self.dynamodbClient->query(queryInput);
            int count = 0;
            while true {
                record {|dynamodb:QueryOutput value;|}? next = check resultStream.next();
                if next is () {
                    break;
                }
                count += 1;
            }
            return count;
        } on fail error err {
            return error("Failed to count interactive messages in DynamoDB: " + err.message(), err);
        }
    }

    // Retrieves the sort keys for a key, either of the interactive items only or of every item.
    private isolated function querySortIds(string key, boolean interactiveOnly) returns string[]|Error {
        do {
            dynamodb:QueryInput queryInput = interactiveOnly ? {
                    TableName: self.tableName,
                    ConsistentRead: self.consistentReads,
                    ScanIndexForward: true,
                    ProjectionExpression: "#sk",
                    KeyConditionExpression: "#pk = :pk and begins_with(#sk, :prefix)",
                    ExpressionAttributeNames: {"#pk": PARTITION_KEY_ATTRIBUTE, "#sk": SORT_KEY_ATTRIBUTE},
                    ExpressionAttributeValues: {":pk": {S: key}, ":prefix": {S: INTERACTIVE_ID_PREFIX}}
                } : {
                    TableName: self.tableName,
                    ConsistentRead: self.consistentReads,
                    ScanIndexForward: true,
                    ProjectionExpression: "#sk",
                    KeyConditionExpression: "#pk = :pk",
                    ExpressionAttributeNames: {"#pk": PARTITION_KEY_ATTRIBUTE, "#sk": SORT_KEY_ATTRIBUTE},
                    ExpressionAttributeValues: {":pk": {S: key}}
                };
            stream<dynamodb:QueryOutput, error?> resultStream = check self.dynamodbClient->query(queryInput);
            string[] sortIds = [];
            while true {
                record {|dynamodb:QueryOutput value;|}? next = check resultStream.next();
                if next is () {
                    break;
                }
                map<dynamodb:AttributeValue>? item = next.value?.Item;
                if item is map<dynamodb:AttributeValue> {
                    dynamodb:AttributeValue? sortKeyAttr = item[SORT_KEY_ATTRIBUTE];
                    string? sortId = sortKeyAttr is () ? () : sortKeyAttr?.S;
                    if sortId is string {
                        sortIds.push(sortId);
                    }
                }
            }
            return sortIds;
        } on fail error err {
            return error("Failed to retrieve message identifiers from DynamoDB: " + err.message(), err);
        }
    }

    // Loads all messages for a key from DynamoDB.
    //
    // A single `Query` on the partition key returns the system item, the counter item,
    // and every interactive item in one round trip. With `ScanIndexForward: true` the
    // stored sort keys ("counter" < "msg#…" < "system" lexicographically) come back in
    // an order that lets us classify each row by its sort-key prefix and skip the
    // counter row, while preserving chronological order of the interactive items.
    private isolated function getAllFromDynamoDb(string key)
            returns readonly & ([ai:ChatSystemMessage, ai:ChatInteractiveMessage...]|ai:ChatInteractiveMessage[])|Error {
        do {
            dynamodb:QueryInput queryInput = {
                TableName: self.tableName,
                ConsistentRead: self.consistentReads,
                ScanIndexForward: true,
                KeyConditionExpression: "#pk = :pk",
                ExpressionAttributeNames: {"#pk": PARTITION_KEY_ATTRIBUTE},
                ExpressionAttributeValues: {":pk": {S: key}}
            };
            stream<dynamodb:QueryOutput, error?> resultStream = check self.dynamodbClient->query(queryInput);

            (ai:ChatSystemMessage & readonly)? systemMessage = ();
            (ai:ChatInteractiveMessage & readonly)[] interactiveMessages = [];

            while true {
                record {|dynamodb:QueryOutput value;|}? next = check resultStream.next();
                if next is () {
                    break;
                }
                map<dynamodb:AttributeValue>? item = next.value?.Item;
                if item is () {
                    continue;
                }
                dynamodb:AttributeValue? sortKeyAttr = item[SORT_KEY_ATTRIBUTE];
                string? sortId = sortKeyAttr is () ? () : sortKeyAttr?.S;
                if sortId is () || sortId == COUNTER_MESSAGE_ID {
                    continue;
                }

                if sortId == SYSTEM_MESSAGE_ID {
                    string body = check extractBody(item);
                    ChatSystemMessageDatabaseMessage|error dbMessage = body.fromJsonStringWithType();
                    if dbMessage is error {
                        return error("Failed to parse system message from DynamoDB: " + dbMessage.message(),
                            dbMessage);
                    }
                    systemMessage = transformFromSystemMessageDatabaseMessage(dbMessage);
                } else if sortId.startsWith(INTERACTIVE_ID_PREFIX) {
                    string body = check extractBody(item);
                    ChatInteractiveMessageDatabaseMessage|error dbMessage = body.fromJsonStringWithType();
                    if dbMessage is error {
                        return error("Failed to parse chat message from DynamoDB: " + dbMessage.message(),
                            dbMessage);
                    }
                    interactiveMessages.push(transformFromInteractiveMessageDatabaseMessage(dbMessage));
                }
            }

            if systemMessage is () {
                return interactiveMessages.cloneReadOnly();
            }
            return [systemMessage, ...interactiveMessages];
        } on fail error err {
            return error("Failed to retrieve chat messages: " + err.message(), err);
        }
    }

    // Deletes the items identified by the given sort keys for a key, via chunked BatchWriteItem.
    private isolated function deleteItems(string key, string[] sortIds) returns Error? {
        dynamodb:WriteRequest[] writeRequests = from string sortId in sortIds
            select {
                DeleteRequest: {
                    Key: itemKey(key, sortId)
                }
            };
        return self.executeBatchWrite(writeRequests);
    }

    // Executes a batch of write requests, chunked to the BatchWriteItem limit, with retries
    // for any unprocessed items.
    private isolated function executeBatchWrite(dynamodb:WriteRequest[] requests) returns Error? {
        int index = 0;
        while index < requests.length() {
            int end = int:min(index + MAX_BATCH_WRITE_SIZE, requests.length());
            check self.writeChunk(requests.slice(index, end));
            index = end;
        }
    }

    private isolated function writeChunk(dynamodb:WriteRequest[] chunk) returns Error? {
        dynamodb:WriteRequest[] pending = chunk;
        int attempts = 0;
        while pending.length() > 0 {
            dynamodb:BatchItemInsertInput batchInput = {
                RequestItems: {[self.tableName]: pending}
            };
            dynamodb:BatchItemInsertOutput|error result = self.dynamodbClient->writeBatchItems(batchInput);
            if result is error {
                return error("Failed to apply batch write to DynamoDB: " + result.message(), result);
            }
            map<dynamodb:WriteRequest[]>? unprocessed = result?.UnprocessedItems;
            dynamodb:WriteRequest[]? remaining = unprocessed is () ? () : unprocessed[self.tableName];
            if remaining is () || remaining.length() == 0 {
                return;
            }
            if attempts >= MAX_BATCH_WRITE_RETRIES {
                return error("Failed to apply all batch writes to DynamoDB after retries.");
            }
            pending = remaining;
            runtime:sleep(fullJitterBackoff(attempts));
            attempts += 1;
        }
    }
}

isolated function partitionMessagesByType(ai:ChatMessage[] messages)
    returns [ai:ChatSystemMessage[], ai:ChatInteractiveMessage[]] {
    ai:ChatSystemMessage[] systemMsgs = [];
    ai:ChatInteractiveMessage[] interactiveMsgs = [];
    foreach ai:ChatMessage msg in messages {
        if msg is ai:ChatSystemMessage {
            systemMsgs.push(msg);
        } else if msg is ai:ChatInteractiveMessage {
            interactiveMsgs.push(msg);
        }
    }
    return [systemMsgs, interactiveMsgs];
}

isolated function getLatestSystemMessage(ai:ChatSystemMessage[] systemMessages) returns ai:ChatSystemMessage? {
    if systemMessages.length() == 0 {
        return;
    }
    return systemMessages[systemMessages.length() - 1];
}

// Builds the composite primary key (partition key + sort key) of an item.
isolated function itemKey(string key, string sortId) returns map<dynamodb:AttributeValue> => {
    [PARTITION_KEY_ATTRIBUTE]: {S: key},
    [SORT_KEY_ATTRIBUTE]: {S: sortId}
};

// Extracts the JSON body string from a stored item.
isolated function extractBody(map<dynamodb:AttributeValue> item) returns string|Error {
    dynamodb:AttributeValue? bodyAttr = item[BODY_ATTRIBUTE];
    string? body = bodyAttr is () ? () : bodyAttr?.S;
    if body is () {
        return error("Stored DynamoDB item is missing a valid message body attribute.");
    }
    return body;
}

// Zero-pads a sequence number so that the lexicographic order of sort keys matches
// the numeric insertion order.
isolated function paddedSequence(int sequence) returns string {
    string value = sequence.toString();
    int width = value.length();
    if width >= SEQUENCE_PAD_WIDTH {
        return value;
    }
    string padding = "";
    foreach int _ in 0 ..< SEQUENCE_PAD_WIDTH - width {
        padding += "0";
    }
    return padding + value;
}

// Validates a DynamoDB table name against the AWS naming rules.
isolated function isValidTableName(string tableName) returns boolean =>
    regexp:isFullMatch(re `^[A-Za-z0-9_.\-]{3,255}$`, tableName);

// Returns whether an error (message or detail) mentions the given AWS error keyword.
isolated function errorMentions(error err, string keyword) returns boolean =>
    err.toString().toLowerAscii().includes(keyword.toLowerAscii());

// Returns whether a `DescribeTable` failure is transient, i.e. worth polling again rather than
// failing initialization outright. See `RETRYABLE_DESCRIBE_TABLE_ERROR_KEYWORDS`.
isolated function isRetryableDescribeTableError(error err) returns boolean {
    foreach string keyword in RETRYABLE_DESCRIBE_TABLE_ERROR_KEYWORDS {
        if errorMentions(err, keyword) {
            return true;
        }
    }
    return false;
}

// Computes a full-jitter exponential backoff delay in seconds for the given retry attempt.
// `sleep = random(0, min(MAX_DELAY, BASE_DELAY * 2^attempt))`. See the AWS Architecture Blog
// post "Exponential Backoff And Jitter" for the rationale behind the full-jitter variant.
isolated function fullJitterBackoff(int attempt) returns decimal {
    decimal cap = BATCH_WRITE_BASE_DELAY;
    foreach int _ in 0 ..< attempt {
        cap = cap * 2d;
        if cap >= BATCH_WRITE_MAX_DELAY {
            cap = BATCH_WRITE_MAX_DELAY;
            break;
        }
    }
    return <decimal>random:createDecimal() * cap;
}

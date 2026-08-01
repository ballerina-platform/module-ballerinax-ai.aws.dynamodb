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

type Prompt record {|
    string[] strings;
    anydata[] insertions;
|};

type ChatUserMessageDatabaseMessage record {|
    ai:USER role;
    string|Prompt content;
    string name?;
|};

type ChatSystemMessageDatabaseMessage record {|
    ai:SYSTEM role;
    string|Prompt content;
    string name?;
|};

type ChatMessageDatabaseMessage
    ChatUserMessageDatabaseMessage|ChatSystemMessageDatabaseMessage|ai:ChatAssistantMessage|ai:ChatFunctionMessage;

type ChatInteractiveMessageDatabaseMessage
    ChatUserMessageDatabaseMessage|ai:ChatAssistantMessage|ai:ChatFunctionMessage;

isolated function transformToDatabaseMessage(ai:ChatMessage message) returns ChatMessageDatabaseMessage {
    if message is ai:ChatAssistantMessage|ai:ChatFunctionMessage {
        return message;
    }

    string|ai:Prompt content = message.content;
    string|Prompt transformedContent = content is string ? content : {
        strings: content.strings,
        insertions: content.insertions
    };

    // `name` is an optional field on both message types, so it is read with the optional field
    // access operator and only set on the mapped record when present. Assigning it
    // unconditionally would panic when absent and would persist `"name": null`.
    string? name = message?.name;

    if message is ai:ChatUserMessage {
        if name is string {
            return {role: ai:USER, content: transformedContent, name};
        }
        return {role: ai:USER, content: transformedContent};
    }

    if name is string {
        return {role: ai:SYSTEM, content: transformedContent, name};
    }
    return {role: ai:SYSTEM, content: transformedContent};
}

isolated function transformFromSystemMessageDatabaseMessage(ChatSystemMessageDatabaseMessage dbMessage)
        returns ai:ChatSystemMessage & readonly {
    string|Prompt content = dbMessage.content;
    string|(ai:Prompt & readonly) transformedContent = content is string ?
            content : createAIPrompt(content.strings.cloneReadOnly(), content.insertions.cloneReadOnly());

    string? name = dbMessage?.name;
    if name is string {
        return {role: ai:SYSTEM, content: transformedContent, name};
    }
    return {role: ai:SYSTEM, content: transformedContent};
}

isolated function transformFromInteractiveMessageDatabaseMessage(ChatInteractiveMessageDatabaseMessage dbMessage)
        returns ai:ChatInteractiveMessage & readonly {
    if dbMessage is ai:ChatAssistantMessage|ai:ChatFunctionMessage {
        return dbMessage.cloneReadOnly();
    }

    string|Prompt content = dbMessage.content;
    string|(ai:Prompt & readonly) transformedContent = content is string ?
            content : createAIPrompt(content.strings.cloneReadOnly(), content.insertions.cloneReadOnly());

    string? name = dbMessage?.name;
    if name is string {
        return {role: ai:USER, content: transformedContent, name};
    }
    return {role: ai:USER, content: transformedContent};
}

isolated function createAIPrompt(string[] & readonly strings, anydata[] & readonly insertions)
        returns readonly & ai:Prompt => isolated object ai:Prompt {
    public final string[] & readonly strings = strings;
    public final anydata[] & readonly insertions = insertions;
};

# Upstash SQS

[![Tests](https://github.com/upstash/sqs/actions/workflows/tests.yaml/badge.svg)](https://github.com/upstash/sqs/actions/workflows/tests.yaml)

> [!NOTE]  
> **This project is in the Experimental Stage.**
> 
> We declare this project experimental to set clear expectations for your usage. There could be known or unknown bugs, the API could evolve, or the project could be discontinued if it does not find community adoption. While we cannot provide professional support for experimental projects, we’d be happy to hear from you if you see value in this project!

A simple, fast, robust stream based message queue for Node.js, backed by Upstash Redis.

- Simple: ~350 LOC, and single dependency.
- Lightweight: Under ~5kb zipped
- Fast: maximizes throughput by minimizing Redis and network overhead. Benchmarks well.
- Robust: designed with concurrency, atomicity, and failure in mind; full code coverage.

```ts
import { Redis } from "@upstash/redis";

const queue = new Queue({ redis: new Redis() });

await queue.sendMessage({ hello: "world1" });

const message1 = await queue.receiveMessage<{ hello: "world1" }>();
expect(message1?.body).toEqual({ hello: "world1" });
```

## Introduction

`@upstash/sqs` is a Node.js library that provides a simple and efficient way to implement a message queue system using Redis streams. It offers features such as message sending, receiving, automatic message verification, and concurrency control. This library is particularly useful for building distributed systems and background job processing.

## Why Upstash SQS?

Upstash SQS brings the simplicity and performance of Redis streams to Node.js developers, making it easy to integrate a robust message queue system into their applications. Whether you're working on distributed systems, background job processing, or other asynchronous workflows, Upstash SQS provides essential features such as message sending, receiving, automatic verification, and concurrency control.

**Key Features:**

- **Efficiency:** Leverage the speed and reliability of Redis streams for message handling.
- **Simplicity:** Dead simple implementation of distributed systems and background job processing with a clean and intuitive API.
- **Concurrency Control:** Easily manage concurrent message processing to optimize system performance.
- **Automatic Verification:** Benefit from built-in message verification mechanisms for reliable and secure communication.

## Table of Contents

- [Installation](#installation)
- [Getting Started](#getting-started)
- [Sending a Message](#sending-a-message)
- [Receiving a Message](#receiving-a-message)
- [Verifying a Message](#verifying-a-message)
- [Configuration Options](#configuration-options)
- [Examples](#examples)
  - [FIFO Example](#fifo-example)
  - [Sending Message with Delay then Poll](#sending-message-with-delay-then-poll)
  - [Manual Verification](#manual-verification)
  - [Concurrent Message Processing/Consuming](#concurrent-message-processingconsuming)

## Installation

To start using Upstash SQS, install the library via npm:

```sh
npm install @upstash/sqs
```

## Getting Started

```typescript
import Redis from "@upstash/redis";
import { Queue } from "@upstash/sqs";

const redis = new Redis();
const queue = new Queue({ redis });
```

## Sending a Message

```typescript
const payload = { key: "value" };
const delayInSeconds = 0; // Set to 0 for immediate delivery
const streamId = await queue.sendMessage(payload, delayInSeconds);
```

## Receiving a Message

```typescript
const pollingForNowMessages = 1000; // Set to 0 for non-blocking, otherwise it will try to get a message then fail if none is available
const receivedMessage = await queue.receiveMessage(pollingForNowMessages);
console.log("Received Message:", receivedMessage);
```

## Verifying a Message

```typescript
const streamId = "some_stream_id";
const verificationStatus = await queue.verifyMessage(streamId);
console.log("Verification Status:", verificationStatus);
```

## Configuration Options

When initializing the Queue instance, you can provide various configuration options:

- redis: Redis client instance (required).
- queueName: Name of the Redis stream (default: "UpstashMQ:Queue").
- concurrencyLimit: Maximum concurrent message processing allowed (default: 1).
- autoVerify: Auto-verify received messages (default: true).
- consumerGroupName: Group that holds consumers when automatically created (default: "Messages").
- visibilityTimeout: Time until recently sent messages become visible to other consumers (default: 30 seconds).

## Verifying a Message

```typescript
const queueConfig = {
  redis: new Redis(),
  queueName: "MyCustomQueue",
  concurrencyLimit: 2,
  autoVerify: false,
  consumerGroupName: "MyConsumers",
  visibilityTimeout: 60000, // 1 minute
};

const customQueue = new Queue(queueConfig);
```

## Examples

### FIFO Example

```typescript
const queue = new Queue({ redis });
await queue.sendMessage({ hello: "world1" });
await delay(100);
await queue.sendMessage({ hello: "world2" });
await delay(100);
await queue.sendMessage({ hello: "world3" });

const message1 = await queue.receiveMessage<{ hello: "world1" }>(); // Logs out { hello: "world1" }

const message2 = await queue.receiveMessage<{ hello: "world2" }>(); // Logs out { hello: "world2" }

const message3 = await queue.receiveMessage<{ hello: "world3" }>(); // Logs out { hello: "world3" }
```

### Sending Message with Delay then Poll

```typescript
const fakeValue = randomValue();
const queueName = "app-logs";

const producer = new Queue({ redis, queueName });
const consumer = new Queue({
  redis: new Redis(),
  queueName,
});
await producer.sendMessage(
  {
    dev: fakeValue,
  },
  2000
);

const receiveMessageRes = await consumer.receiveMessage<{
  dev: string;
}>(5000);
```

### Manual Verification

```typescript
const queue = new Queue({ redis, autoVerify: false });
await queue.sendMessage({ hello: "world" });

const message = await queue.receiveMessage<{ hello: "world" }>(); // Logs out { hello: "world" }
if (message) {
  await queue.verifyMessage(message.streamId); //Logs out "VERIFIED" or "NOT VERIFIED"
}
```

### Concurrent Message Processing/Consuming

If `concurrencyLimit` is not set one of the `receiveMessage()` will throw. You need to explicitly set the concurrencyLimit. Default is 1.

```typescript
const queue = new Queue({
  redis: new Redis(),
  queueName: randomValue(),
  concurrencyLimit: 2,
});

await queue.sendMessage({
  dev: randomValue(),
});

await queue.sendMessage({
  dev: randomValue(),
});

await Promise.all([queue.receiveMessage(), queue.receiveMessage()]);
```

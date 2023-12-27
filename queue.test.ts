import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Redis } from "ioredis";
import { DEFAULT_QUEUE_NAME, Queue } from "./queue";
import { delay, formatMessageQueueKey } from "./utils";

const randomValue = () => crypto.randomUUID();
const redis = new Redis();
const consumerClient = new Redis();

describe("Queue with default and customized name", () => {
  test("should return the default queue name", () => {
    const queue = new Queue({ redis });
    expect(queue.config.queueName).toEqual(DEFAULT_QUEUE_NAME);
  });

  test("should return the customized name", () => {
    const queueName = "cookie-jar";
    const queue = new Queue({ redis, queueName });
    expect(queue.config.queueName).toEqual(queueName);
  });
});

// describe("Queue with a single client", () => {
//   test("should add item to queue", async () => {
//     const queue = new Queue({ redis });

//     const sendMessageResult = await queue.sendMessage({
//       dev: "hezarfennnn",
//       age: 27,
//     });

//     const res = await redis.xrevrange(
//       formatMessageQueueKey(DEFAULT_QUEUE_NAME),
//       "+",
//       "-",
//       "COUNT",
//       1
//     );
//     await redis.xdel(formatMessageQueueKey(DEFAULT_QUEUE_NAME), res[0][0]);
//     expect(sendMessageResult).not.toBeNull();
//     expect(res[0][0]).toEqual(sendMessageResult as string);
//   });
// });

// describe("Queue with delays", () => {
//   test(
//     "should enqueue with a delay",
//     async () => {
//       const fakeValue = randomValue();
//       const queue = new Queue({ redis, queueName: "app-logs" });
//       await queue.sendMessage(
//         {
//           dev: fakeValue,
//         },
//         2
//       );

//       await delay(5000);
//       const res = await redis.xrevrange(
//         formatMessageQueueKey("app-logs"),
//         "+",
//         "-",
//         "COUNT",
//         1
//       );
//       await redis.xdel(formatMessageQueueKey("app-logs"), res[0][0]);
//       expect(res[0][1]).toEqual(["messageBody", `{"dev":"${fakeValue}"}`]);
//     },
//     { timeout: 10000 }
//   );

//   test(
//     "should poll until data arives",
//     async () => {
//       const fakeValue = randomValue();

//       const producer = new Queue({ redis, queueName: "app-logs" });
//       const consumer = new Queue({
//         redis: consumerClient,
//         queueName: "app-logs",
//       });
//       await producer.sendMessage(
//         {
//           dev: fakeValue,
//         },
//         2
//       );

//       const receiveMessageRes = await consumer.receiveMessage<{ dev: string }>(
//         "consumer-1",
//         5000
//       );

//       expect(receiveMessageRes?.body.dev).toEqual(fakeValue);
//     },
//     { timeout: 10000 }
//   );
// });

// describe("Queue with Multiple Consumers", () => {
//   const messageCount = 10;
//   const consumerCount = 5;
//   let producer: Queue;
//   let consumers: Queue[] = [];
//   let messagesSent = new Set();
//   let messagesReceived = new Map();

//   beforeAll(() => {
//     // Initialize Redis and Queue for the producer
//     const producerRedis = new Redis();
//     producer = new Queue({ redis: producerRedis, queueName: "app-logs" });

//     // Initialize Redis and Queues for consumers
//     for (let i = 0; i < consumerCount; i++) {
//       const consumerRedis = new Redis();
//       const consumer = new Queue({
//         redis: consumerRedis,
//         queueName: "app-logs",
//       });
//       consumers.push(consumer);
//     }
//   });

//   test(
//     "should process each message exactly once across all consumers",
//     async () => {
//       // Send messages
//       for (let i = 0; i < messageCount; i++) {
//         const message = `Message ${randomValue()}`;
//         await producer.sendMessage({ message });
//         messagesSent.add(message);
//       }

//       // Start consuming messages
//       const consumePromises = consumers.map((consumer, index) => {
//         return new Promise<void>(async (resolve) => {
//           for (let i = 0; i < messageCount / consumerCount; i++) {
//             const res = await consumer.receiveMessage<{ message: string }>(
//               `consumer-${index}`
//             );
//             if (res && res.body.message) {
//               const message = res.body.message;
//               if (!messagesReceived.has(message)) {
//                 messagesReceived.set(message, index);
//               }
//             }
//           }
//           resolve();
//         });
//       });

//       await Promise.all(consumePromises);

//       // Assertions
//       expect(messagesReceived.size).toBe(messageCount);
//       messagesSent.forEach((message) => {
//         expect(messagesReceived.has(message)).toBe(true);
//       });

//       // Ensure no message was processed by more than one consumer
//       expect(new Set(messagesReceived.values()).size).toBeLessThanOrEqual(
//         consumerCount
//       );
//     },
//     { timeout: 15 * 1000 }
//   );

//   afterAll(async () => {
//     // Close Redis connections and cleanup
//     await producer.config.redis.quit();
//     for (const consumer of consumers) {
//       await consumer.config.redis.quit();
//     }
//   });
// });

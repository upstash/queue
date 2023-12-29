import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { Redis } from "ioredis";

import {
  DEFAULT_AUTO_VERIFY,
  DEFAULT_CONCURRENCY_LIMIT,
  DEFAULT_CONSUMER_GROUP_NAME,
  DEFAULT_QUEUE_NAME,
  ERROR_MAP,
} from "./constants";
import { Queue } from "./queue";
import { delay, formatMessageQueueKey } from "./utils";

const randomValue = () => crypto.randomUUID().slice(0, 8);
const redis = new Redis();

describe("Queue", () => {
  afterAll(async () => {
    await redis.flushdb();
    await redis.quit();
    process.exit();
  });

  describe("Queue name default option", () => {
    test("should return the default queue name", () => {
      const queue = new Queue({ redis });
      expect(queue.config.queueName).toEqual(formatMessageQueueKey(DEFAULT_QUEUE_NAME));
    });

    test("should return the customized name", () => {
      const queueName = "cookie-jar";
      const queue = new Queue({ redis, queueName });
      expect(queue.config.queueName).toEqual(formatMessageQueueKey(queueName));
    });
  });

  describe("Consumer group name default option", () => {
    test("should return the default customerGroupName", () => {
      const queue = new Queue({ redis, queueName: randomValue() });
      expect(queue.config.consumerGroupName).toEqual(DEFAULT_CONSUMER_GROUP_NAME);
    });

    test("should return the customized customerGroupName", () => {
      const consumerGroupName = "bigger-cookie-jar";
      const queue = new Queue({
        redis,
        consumerGroupName,
        queueName: randomValue(),
      });
      expect(queue.config.consumerGroupName).toEqual(consumerGroupName);
    });
  });

  describe("Concurrency limit default option", () => {
    test("should return 0 when concurrency is default", () => {
      const queue = new Queue({ redis, queueName: randomValue() });
      expect(queue.config.concurrencyLimit).toEqual(DEFAULT_CONCURRENCY_LIMIT);
    });

    test("should return the customized concurrency limit", () => {
      const concurrencyLimit = 5;
      const queue = new Queue({
        redis,
        concurrencyLimit,
        queueName: randomValue(),
      });
      expect(queue.config.concurrencyLimit).toEqual(concurrencyLimit);
    });
  });

  describe("Auto verify default option", () => {
    test("should return 0 when concurrency is default", () => {
      const queue = new Queue({ redis, queueName: randomValue() });
      expect(queue.config.autoVerify).toEqual(DEFAULT_AUTO_VERIFY);
    });

    test("should return the customized concurrency limit", () => {
      const autoVerify = false;
      const queue = new Queue({ redis, autoVerify, queueName: randomValue() });
      expect(queue.config.autoVerify).toEqual(autoVerify);
    });
  });

  describe("Concurrency", () => {
    test("should allow only specified amount of receiveMessages concurrently", async () => {
      const consumerCount = 4;

      const consumer = new Queue({
        redis: new Redis(),
        concurrencyLimit: consumerCount,
        queueName: randomValue(),
      });

      for (let i = 0; i < consumerCount; i++) {
        await consumer.receiveMessage();
      }

      expect(consumer.concurrencyCounter).toEqual(consumerCount);
    });

    test("should throw when try to consume more than 5 at the same time", async () => {
      let errorMessage = "";
      let iterationCount = 0;
      try {
        const consumer = new Queue({
          redis: new Redis(),
          queueName: randomValue(),
          concurrencyLimit: 5,
        });

        for (let i = 0; i < 10; i++) {
          await consumer.receiveMessage();
          iterationCount++;
        }
      } catch (error) {
        errorMessage = (error as Error).message;
      }
      expect(iterationCount).toBe(5);
      expect(errorMessage).toEqual(ERROR_MAP.CONCURRENCY_LIMIT_EXCEEDED);
    });

    test("should give us 0 since all the consumers are available after successful verify", async () => {
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

      expect(queue.concurrencyCounter).toEqual(0);
    });

    test("should throw since default receive messages exceeds default limit: 1", async () => {
      let errorMessage = "";
      try {
        const queue = new Queue({
          redis: new Redis(),
          queueName: randomValue(),
        });

        await queue.sendMessage({
          dev: randomValue(),
        });
        await queue.sendMessage({
          dev: randomValue(),
        });

        await Promise.all([
          queue.receiveMessage(),
          queue.receiveMessage(),
          queue.receiveMessage(),
          queue.receiveMessage(),
        ]);
      } catch (error) {
        errorMessage = (error as Error).message;
      }

      expect(errorMessage).toEqual(ERROR_MAP.CONCURRENCY_DEFAULT_LIMIT_EXCEEDED);
    });
  });

  describe("Auto verify", () => {
    test("should auto verify the message and decrement concurrency counter", async () => {
      const queue = new Queue({
        redis: new Redis(),
        queueName: randomValue(),
      });

      await queue.sendMessage({
        dev: randomValue(),
      });

      await queue.receiveMessage();

      expect(queue.concurrencyCounter).toBe(0);
    });

    test("should verify manually and decrement the counter", async () => {
      const queue = new Queue({
        redis: new Redis(),
        queueName: randomValue(),
        autoVerify: false,
      });

      await queue.sendMessage({
        dev: randomValue(),
      });

      const receiveRes = await queue.receiveMessage();
      if (receiveRes) {
        const { streamId } = receiveRes;
        await queue.verifyMessage(streamId);
      }

      expect(queue.concurrencyCounter).toBe(0);
    });

    test("should not release concurrency since auto verify is disabled and no verifyMessage present", async () => {
      const queue = new Queue({
        redis: new Redis(),
        queueName: randomValue(),
        autoVerify: false,
      });

      await queue.sendMessage({
        dev: randomValue(),
      });

      await queue.receiveMessage();

      expect(queue.concurrencyCounter).not.toBe(0);
    });
  });

  describe("Autoclaim orphans", () => {
    test(
      "should left nothing in pending list",
      async () => {
        const queue = new Queue({
          redis,
          autoVerify: false,
          queueName: randomValue(),
          concurrencyLimit: 2,
          visibilityTimeout: 10000,
        });

        await queue.sendMessage({ hello: "world" });
        await queue.receiveMessage<{ hello: "world" }>();
        await delay(10000);
        const ackedReceive = await queue.receiveMessage<{ hello: "world" }>();
        queue.verifyMessage(ackedReceive?.streamId!);

        const xpendingRes = await queue.config.redis.xpending(
          queue.config.queueName!,
          queue.config.consumerGroupName!,
          "-",
          "+",
          1
        );

        expect(xpendingRes).toBeEmpty();
      },
      { timeout: 20000 }
    );

    test(
      "should return at least 1 pending since they are not claimed",
      async () => {
        const queue = new Queue({
          redis,
          autoVerify: false,
          queueName: randomValue(),
          concurrencyLimit: 2,
        });

        await queue.sendMessage({ hello: "world" });
        await queue.receiveMessage<{ hello: "world" }>();
        await delay(10000);
        await queue.receiveMessage<{ hello: "world" }>();

        const xpendingRes = await queue.config.redis.xpending(
          queue.config.queueName!,
          queue.config.consumerGroupName!,
          "-",
          "+",
          1
        );

        expect(xpendingRes).not.toBeEmpty();
      },
      { timeout: 20000 }
    );
  });

  describe("Queue with a single client", () => {
    test("should add item to queue", async () => {
      const queue = new Queue({ redis });

      const sendMessageResult = await queue.sendMessage({
        dev: "hezarfennnn",
        age: 27,
      });

      const res = await redis.xrevrange(
        formatMessageQueueKey(DEFAULT_QUEUE_NAME),
        "+",
        "-",
        "COUNT",
        1
      );
      await redis.xdel(formatMessageQueueKey(DEFAULT_QUEUE_NAME), res[0][0]);
      expect(sendMessageResult).not.toBeNull();
      expect(res[0][0]).toEqual(sendMessageResult as string);
    });
  });

  describe("Queue with delays", () => {
    test(
      "should enqueue with a delay",
      async () => {
        const fakeValue = randomValue();
        const queue = new Queue({ redis, queueName: "app-logs" });
        await queue.sendMessage(
          {
            dev: fakeValue,
          },
          2000
        );

        await delay(5000);
        const res = await redis.xrevrange(formatMessageQueueKey("app-logs"), "+", "-", "COUNT", 1);
        await redis.xdel(formatMessageQueueKey("app-logs"), res[0][0]);
        expect(res[0][1]).toEqual(["messageBody", `{"dev":"${fakeValue}"}`]);
      },
      { timeout: 10000 }
    );

    test(
      "should poll until data arives",
      async () => {
        const fakeValue = randomValue();

        const producer = new Queue({ redis, queueName: "app-logs" });
        const consumer = new Queue({
          redis: new Redis(),
          queueName: "app-logs",
        });
        await producer.sendMessage(
          {
            dev: fakeValue,
          },
          2
        );

        const receiveMessageRes = await consumer.receiveMessage<{
          dev: string;
        }>(5000);

        expect(receiveMessageRes?.body.dev).toEqual(fakeValue);
      },
      { timeout: 10000 }
    );
  });

  describe("Queue with Multiple Consumers", () => {
    const messageCount = 10;
    const consumerCount = 5;
    let producer: Queue;
    const consumers: Queue[] = [];
    const messagesSent = new Set();
    const messagesReceived = new Map();

    beforeAll(() => {
      // Initialize Redis and Queue for the producer
      const producerRedis = new Redis();
      producer = new Queue({ redis: producerRedis, queueName: "app-logs" });

      // Initialize Redis and Queues for consumers
      for (let i = 0; i < consumerCount; i++) {
        const consumer = new Queue({
          redis: new Redis(),
          queueName: "app-logs",
        });
        consumers.push(consumer);
      }
    });

    test(
      "should process each message exactly once across all consumers",
      async () => {
        // Send messages
        for (let i = 0; i < messageCount; i++) {
          const message = `Message ${randomValue()}`;
          await producer.sendMessage({ message });
          messagesSent.add(message);
        }

        // Start consuming messages
        const consumePromises = consumers.map((consumer, index) => {
          // biome-ignore lint/suspicious/noAsyncPromiseExecutor: <explanation>
          return new Promise<void>(async (resolve) => {
            for (let i = 0; i < messageCount / consumerCount; i++) {
              const res = await consumer.receiveMessage<{ message: string }>();
              if (res?.body.message) {
                const message = res.body.message;
                if (!messagesReceived.has(message)) {
                  messagesReceived.set(message, index);
                }
              }
            }
            resolve();
          });
        });

        await Promise.all(consumePromises);

        // Assertions
        expect(messagesReceived.size).toBe(messageCount);
        // biome-ignore lint/complexity/noForEach: <explanation>
        messagesSent.forEach((message) => {
          expect(messagesReceived.has(message)).toBe(true);
        });

        // Ensure no message was processed by more than one consumer
        expect(new Set(messagesReceived.values()).size).toBeLessThanOrEqual(consumerCount);
      },
      { timeout: 15 * 1000 }
    );

    afterAll(async () => {
      // Close Redis connections and cleanup
      await producer.config.redis.quit();
      for (const consumer of consumers) {
        await consumer.config.redis.quit();
      }
    });
  });

  describe("FIFO queue", () => {
    test("should do a FIFO queue", async () => {
      const queue = new Queue({ redis });
      await queue.sendMessage({ hello: "world1" });
      await delay(100);
      await queue.sendMessage({ hello: "world2" });
      await delay(100);
      await queue.sendMessage({ hello: "world3" });

      const message1 = await queue.receiveMessage<{ hello: "world1" }>();
      expect(message1?.body).toEqual({ hello: "world1" });

      const message2 = await queue.receiveMessage<{ hello: "world2" }>();
      expect(message2?.body).toEqual({ hello: "world2" });

      const message3 = await queue.receiveMessage<{ hello: "world3" }>();
      expect(message3?.body).toEqual({ hello: "world3" });
    });
  });
});

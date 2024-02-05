import { afterAll, describe, expect, test } from "bun:test";
import { Redis } from "@upstash/redis";

import { Queue } from ".";
import {
  DEFAULT_AUTO_VERIFY,
  DEFAULT_CONCURRENCY_LIMIT,
  DEFAULT_CONSUMER_GROUP_NAME,
  DEFAULT_QUEUE_NAME,
  ERROR_MAP,
} from "./constants";
import { delay, formatMessageQueueKey } from "./utils";

const randomValue = () => crypto.randomUUID().slice(0, 8);
const redis = Redis.fromEnv();

describe("Queue", () => {
  afterAll(async () => await redis.flushdb());

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
        redis,
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
          redis,
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
        redis,
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
          redis,
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
        redis,
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
        redis,
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
        redis,
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
        await queue.verifyMessage(ackedReceive?.streamId!);

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

  describe("Queue with delays", () => {
    test(
      "should enqueue with a delay",
      async () => {
        const fakeValue = randomValue();
        const queue = new Queue({
          redis,
          queueName: "app-logs",
          concurrencyLimit: 2,
        });
        await queue.sendMessage(
          {
            dev: fakeValue,
          },
          2000
        );
        const res = await queue.receiveMessage();
        expect(res?.body).not.toEqual({
          dev: fakeValue,
        });
        await delay(5000);
        const res1 = await queue.receiveMessage<{ dev: string }>();
        expect(res1?.body).toEqual({
          dev: fakeValue,
        });
      },
      { timeout: 10000 }
    );

    test(
      "should poll until data arives",
      async () => {
        const throwable = async () => {
          const fakeValue = randomValue();
          const producer = new Queue({ redis, queueName: "app-logs" });
          const consumer = new Queue({
            redis,
            queueName: "app-logs",
          });
          await producer.sendMessage(
            {
              dev: fakeValue,
            },
            2
          );
          await consumer.receiveMessage<{
            dev: string;
          }>(5000);
        };
        expect(throwable).toThrow();
      },
      { timeout: 10000 }
    );
  });

  describe("Reliability tests", () => {
    test("should handle multi-threaded message processing in batches", async () => {
      const queue = new Queue({
        redis,
        queueName: randomValue(),
        concurrencyLimit: 5,
      });

      // First batch of sending 5 messages
      const firstBatchSendPromises = [];
      for (let i = 0; i < 5; i++) {
        firstBatchSendPromises.push(queue.sendMessage({ data: `message-${i}` }));
      }
      await Promise.all(firstBatchSendPromises);

      // First batch of receiving 5 messages
      const firstBatchReceivePromises = [];
      for (let i = 0; i < 5; i++) {
        firstBatchReceivePromises.push(queue.receiveMessage());
      }
      await Promise.all(firstBatchReceivePromises);

      // Second batch of sending 5 messages
      const secondBatchSendPromises = [];
      for (let i = 5; i < 10; i++) {
        secondBatchSendPromises.push(queue.sendMessage({ data: `message-${i}` }));
      }
      await Promise.all(secondBatchSendPromises);

      // Second batch of receiving 5 messages
      const secondBatchReceivePromises = [];
      for (let i = 5; i < 10; i++) {
        secondBatchReceivePromises.push(queue.receiveMessage());
      }
      await Promise.all(secondBatchReceivePromises);

      expect(queue.concurrencyCounter).toBe(0); // Assuming autoVerify is enabled
    }, 30000);

    test("should handle high volume of messages in batches and track received messages", async () => {
      const batchSize = 5; // Size of each batch, based on the concurrency limit
      const queue = new Queue({
        redis,
        queueName: randomValue(),
        concurrencyLimit: batchSize,
      });

      const totalMessages = 100; // High volume of messages
      const numberOfBatches = totalMessages / batchSize;
      let totalNumberOfReceivedMessages = 0; // Counter for received messages

      for (let batch = 0; batch < numberOfBatches; batch++) {
        const sendMessagePromises = [];
        for (let i = 0; i < batchSize; i++) {
          sendMessagePromises.push(queue.sendMessage({ data: `message-${batch * batchSize + i}` }));
        }
        await Promise.all(sendMessagePromises);

        const receiveMessagePromises = [];
        for (let i = 0; i < batchSize; i++) {
          const promise = queue.receiveMessage().then((message) => {
            if (message) {
              totalNumberOfReceivedMessages++;
            }
            return message;
          });
          receiveMessagePromises.push(promise);
        }
        await Promise.all(receiveMessagePromises);
      }

      expect(totalNumberOfReceivedMessages).toBe(totalMessages);
      expect(queue.concurrencyCounter).toBe(0);
    }, 30000);
  });

  describe("FIFO queue", () => {
    test("should do a FIFO queue", async () => {
      const queue = new Queue({ redis });

      await queue.sendMessage({ hello: "world1" });
      await queue.sendMessage({ hello: "world2" });
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

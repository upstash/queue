import { sleep } from "bun";
import { getRandomSeed } from "bun:jsc";
import { describe, expect, test } from "bun:test";
import { Redis } from "ioredis";
import { Queue } from "./queue";
import { formatMessageQueueKey } from "./utils";

const randomValue = getRandomSeed();
const redis = new Redis();
const consumerClient = new Redis();

describe("Queue", () => {
  test("should add item to queue", async () => {
    const queue = new Queue({ redis, queueName: "app-logs" });

    const sendMessageResult = (await queue.sendMessage({
      dev: "hezarfennnn",
      age: 27,
    })) as string;

    const res = await redis.xrevrange(
      formatMessageQueueKey("app-logs"),
      "+",
      "-",
      "COUNT",
      1
    );
    await redis.xdel(formatMessageQueueKey("app-logs"), res[0][0]);
    expect(res[0][0]).toEqual(sendMessageResult);
  });

  test(
    "should enqueue with a delay",
    async () => {
      const queue = new Queue({ redis, queueName: "app-logs" });
      await queue.sendMessage(
        {
          dev: randomValue,
        },
        2
      );

      await sleep(5000);
      const res = await redis.xrevrange(
        formatMessageQueueKey("app-logs"),
        "+",
        "-",
        "COUNT",
        1
      );
      expect(res[0][1]).toEqual(["messageBody", `{\"dev\":${randomValue}}`]);
    },
    { timeout: 10000 }
  );

  test("should try to read from stream with consumer group", async () => {
    const queue = new Queue({ redis, queueName: "app-logs" });
    const receiveMessageRes = await queue.receiveMessage("consumer-1");

    expect(receiveMessageRes?.streamId ?? "").toBeTruthy();
  });

  test(
    "should poll until data arives",
    async () => {
      const producer = new Queue({ redis, queueName: "app-logs" });
      const consumer = new Queue({
        redis: consumerClient,
        queueName: "app-logs",
      });
      await producer.sendMessage(
        {
          dev: randomValue,
        },
        2
      );

      const receiveMessageRes = await consumer.receiveMessage(
        "consumer-1",
        5000
      );

      expect(receiveMessageRes?.body.messageBody.dev).toEqual(randomValue);
    },
    { timeout: 10000 }
  );
});

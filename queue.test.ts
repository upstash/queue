import { beforeEach, expect, test } from "bun:test";
import { Redis } from "ioredis";
import { Queue } from "./queue";
import { formatMessageQueueKey } from "./utils";
import { sleep } from "bun";

const redis = new Redis();

// test("should add item to queue", async () => {
//   const queue = new Queue({ redis, queueName: "app-logs" });

//   const sendMessageResult = (await queue.sendMessage({
//     dev: "hezarfennnn",
//     age: 27,
//   })) as string;

//   const res = await redis.xrevrange(
//     formatMessageQueueKey("app-logs"),
//     "+",
//     "-",
//     "COUNT",
//     1
//   );
//   expect(res[0][0]).toEqual(sendMessageResult);
// });

test("should try to read from stream with consumer group", async () => {
  const queue = new Queue({ redis, queueName: "app-logs" });
  const sendMessageRes = await queue.sendMessage({
    dev: "hezarfen",
    age: 27,
  });
  const receiveMessageRes = await queue.receiveMessage("customConsumerName");
  expect(sendMessageRes).toEqual(receiveMessageRes?.streamId ?? "");
});

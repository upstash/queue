import { EventEmitter } from "events";
import Redis from "ioredis";
import { formatMessageQueueKey } from "./utils";

const CONSUMER_GROUP_NAME = "messages";
export type QueueConfig = {
  redis: Redis;
  queueName: string;
};

export class Queue extends EventEmitter {
  config: QueueConfig;

  constructor(config: QueueConfig) {
    super();
    this.config = {
      redis: config.redis,
      queueName: config.queueName,
    };
    this.initializeConsumerGroup();
  }

  private createStreamKey() {
    return formatMessageQueueKey(this.config.queueName);
  }

  private async initializeConsumerGroup() {
    try {
      await this.config.redis.xgroup(
        "CREATE",
        this.createStreamKey(),
        CONSUMER_GROUP_NAME,
        "$",
        "MKSTREAM"
      );
    } catch (error) {
      if (
        (error as Error).message !==
        "BUSYGROUP Consumer Group name already exists"
      ) {
        this.emit("error", error);
      }
      // If the group already exists, it's not necessarily an error in this context.
    }
  }

  async sendMessage<T extends {}>(payload: T) {
    const { redis } = this.config;
    try {
      const flattenedPayload = Object.entries({
        messageBody: JSON.stringify(payload),
      }).flat() as string[];

      const res = await redis.xadd(
        this.createStreamKey(),
        "*",
        ...flattenedPayload
      );
      return res;
    } catch (error) {
      this.emit("error", error);
      console.error("Error in sendMessage:", error);
    }
  }

  async receiveMessage(customConsumerName: string) {
    const { redis } = this.config;
    try {
      const res = await redis.xreadgroup(
        "GROUP",
        CONSUMER_GROUP_NAME,
        customConsumerName,
        "COUNT",
        1,
        "STREAMS",
        this.createStreamKey(),
        ">"
      );

      //TODO: type should be fixed
      const resultBody = res[0][1][0] as [string, [string, string]];

      const resultObject = {
        streamId: resultBody[0],
        body: {
          [resultBody[1][0]]: JSON.parse(resultBody[1][1]),
        },
      };
      if (resultObject) {
        //TODO: apply retry here. if retry also fails release this
        await this.verifyDelivery(redis, resultObject);
      }

      return resultObject;
    } catch (error) {
      console.log(`receiveMessage ${(error as Error).message}`);
    }
  }

  private async verifyDelivery(
    redis: Redis,
    resultObject: { streamId: string; body: { [x: string]: any } }
  ) {
    try {
      return await redis.xack(
        this.createStreamKey(),
        CONSUMER_GROUP_NAME,
        resultObject.streamId
      );
    } catch (error) {
      this.emit("error", error);
      console.error("Error in verifyDelivery:", error);
    }
  }
}

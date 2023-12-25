import { EventEmitter } from "events";
import Redis from "ioredis";
import { formatMessageQueueKey } from "./utils";

const MAX_RETRY_COUNT = 3; // Maximum number of retries
const RETRY_DELAY_MS = 1500; // Delay between retries in milliseconds

const CONSUMER_GROUP_NAME = "messages";

export type QueueConfig = {
  redis: Redis;
  queueName: string;
};

export class Queue extends EventEmitter {
  config: QueueConfig;
  messageTimeouts = new Set();

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

  async sendMessage<T extends {}>(payload: T, delayInSeconds: number = 0) {
    const { redis } = this.config;
    try {
      const flattenedPayload = Object.entries({
        messageBody: JSON.stringify(payload),
      }).flat() as string[];

      const streamKey = this.createStreamKey();

      if (delayInSeconds > 0) {
        const timeoutId = setTimeout(() => {
          redis
            .xadd(streamKey, "*", ...flattenedPayload)
            .then((res) => {
              this.messageTimeouts.delete(timeoutId);
            })
            .catch((error) => {
              console.error("Error in delayed xadd:", error);
              this.emit("error", error);
            });
        }, delayInSeconds * 1000);
        this.messageTimeouts.add(timeoutId);
      } else {
        return await redis.xadd(streamKey, "*", ...flattenedPayload);
      }
    } catch (error) {
      this.emit("error", error);
      console.error("Error in sendMessage:", error);
    }
  }

  async receiveMessage(consumerName: string, blockTimeMs = 5000) {
    const { redis } = this.config;
    try {
      const res = await redis.xreadgroup(
        "GROUP",
        CONSUMER_GROUP_NAME,
        consumerName,
        "COUNT",
        1,
        "BLOCK",
        blockTimeMs,
        "STREAMS",
        this.createStreamKey(),
        ">"
      );
      //@ts-ignore
      if (res && res.length > 0 && res[0][1].length > 0) {
        // Message found, process and return
        //@ts-ignore
        const resultBody = res[0][1][0] as [string, [string, string]];
        const resultObject = {
          streamId: resultBody[0],
          body: {
            [resultBody[1][0]]: JSON.parse(resultBody[1][1]),
          },
        };
        await this.verifyDelivery(redis, resultObject);
        return resultObject;
      }

      return null;
    } catch (error) {
      console.log(`receiveMessage ${(error as Error).message}`);
    }
  }

  private async verifyDelivery(
    redis: Redis,
    resultObject: { streamId: string; body: { [x: string]: any } }
  ) {
    let retryCount = 0;

    const attemptAck = async (): Promise<boolean> => {
      try {
        await redis.xack(
          this.createStreamKey(),
          CONSUMER_GROUP_NAME,
          resultObject.streamId
        );
        return true;
      } catch (error) {
        if (retryCount < MAX_RETRY_COUNT) {
          retryCount++;
          console.log(
            `Retrying acknowledgment (${retryCount}/${MAX_RETRY_COUNT}).`
          );
          await new Promise((resolve) => setTimeout(resolve, RETRY_DELAY_MS));
          return attemptAck();
        } else {
          this.emit("error", error);
          console.error("Final attempt failed in verifyDelivery:", error);
          return false;
        }
      }
    };

    return attemptAck();
  }
}

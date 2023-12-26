import { EventEmitter } from "events";
import Redis from "ioredis";
import {
  formatMessageQueueKey,
  parseRedisStreamMessage,
  retryWithBackoff,
} from "./utils";

//Better visibilty control instead of relying only for pending state
//Move failed acked items to DLQ,then delete it from current stream

const CONSUMER_GROUP_NAME = "messages";

export type QueueConfig = {
  redis: Redis;
  queueName: string;
};

export class Queue extends EventEmitter {
  config: QueueConfig;
  private messageTimeouts = new Set<NodeJS.Timer>();

  constructor(config: QueueConfig) {
    super();
    this.config = {
      redis: config.redis,
      queueName: config.queueName,
    };
    this.initializeConsumerGroup();
    this.setupShutdownHandler();
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

      const _sendMessage = () =>
        redis.xadd(streamKey, "*", ...flattenedPayload);

      if (delayInSeconds > 0) {
        let streamIdResult: string | null = null;

        const timeoutId = setTimeout(() => {
          retryWithBackoff(_sendMessage)
            .then((res) => {
              streamIdResult = res;
              this.messageTimeouts.delete(timeoutId);
            })
            .catch((error) => {
              this.emit("error", error);
            });
        }, delayInSeconds * 1000);
        this.messageTimeouts.add(timeoutId);
        return streamIdResult;
      } else {
        return await retryWithBackoff(_sendMessage);
      }
    } catch (error) {
      this.emit("error", error);
      console.error("Error in sendMessage:", error);
      return null;
    }
  }

  async receiveMessage<StreamResult>(consumerName: string, blockTimeMs = 0) {
    const { redis } = this.config;

    const receiveAndProcessMessage = async () => {
      try {
        const xreadRes = await redis.xreadgroup(
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
        const parsedMessage = parseRedisStreamMessage<StreamResult>(xreadRes);
        if (!parsedMessage) return null;

        await this.verifyMessage<StreamResult>(redis, parsedMessage);
        return parsedMessage;
      } catch (error) {
        this.emit(
          "receiveError",
          `Error receiving message: ${(error as Error).message}`
        );
        throw error;
      }
    };

    try {
      return await retryWithBackoff(receiveAndProcessMessage);
    } catch (finalError) {
      console.error(
        `Final attempt to receive message failed: ${
          (finalError as Error).message
        }`
      );
      throw finalError;
    }
  }

  private async verifyMessage<StreamResult>(
    redis: Redis,
    resultObject: { streamId: string; body: StreamResult }
  ) {
    const attemptAck = async () => {
      await redis.xack(
        this.createStreamKey(),
        CONSUMER_GROUP_NAME,
        resultObject.streamId
      );
    };

    try {
      await retryWithBackoff(attemptAck);
    } catch (finalError) {
      console.error(
        `Final attempt to acknowledge message failed: ${
          (finalError as Error).message
        }`
      );
      this.emit("error", finalError);
      //Return null to prevent another retry within receiveMessage
      return null;
    }
  }

  private setupShutdownHandler() {
    process.on("SIGINT", this.shutdown.bind(this));
    process.on("SIGTERM", this.shutdown.bind(this));
  }

  private async shutdown() {
    console.log("Shutting down gracefully...");
    this.messageTimeouts.forEach((timeoutId) => clearTimeout(timeoutId));
    // Add any other cleanup logic here
    process.exit(0);
  }
}

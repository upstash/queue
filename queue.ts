import { EventEmitter } from "events";
import Redis from "ioredis";
import { formatMessageQueueKey, retryWithBackoff } from "./utils";

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
        const timeoutId = setTimeout(() => {
          retryWithBackoff(_sendMessage)
            .then(() => {
              this.messageTimeouts.delete(timeoutId);
            })
            .catch((error) => {
              this.emit("error", error);
            });
        }, delayInSeconds * 1000);
        this.messageTimeouts.add(timeoutId);
      } else {
        return await retryWithBackoff(_sendMessage);
      }
    } catch (error) {
      this.emit("error", error);
      console.error("Error in sendMessage:", error);
    }
  }

  async receiveMessage(consumerName: string, blockTimeMs = 5000) {
    const { redis } = this.config;

    // Wrapper function for the retryable part of receiveMessage
    const receiveAndProcessMessage = async () => {
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
        // Check if a message was received
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
          await this.verifyMessage(redis, resultObject);
          return resultObject;
        }
        return null;
      } catch (error) {
        this.emit(
          "receiveError",
          `Error receiving message: ${(error as Error).message}`
        );
        throw error; // Rethrow the error to be caught by retryWithBackoff
      }
    };

    // Use retryWithBackoff to retry the receiveAndProcessMessage function
    try {
      return await retryWithBackoff(receiveAndProcessMessage);
    } catch (finalError) {
      console.error(
        `Final attempt to receive message failed: ${
          (finalError as Error).message
        }`
      );
      throw finalError; // Optional: rethrow or handle the error after all retries fail
    }
  }

  private async verifyMessage(
    redis: Redis,
    resultObject: { streamId: string; body: { [x: string]: any } }
  ) {
    const attemptAck = async () => {
      await redis.xack(
        this.createStreamKey(),
        CONSUMER_GROUP_NAME,
        resultObject.streamId
      );
    };

    try {
      // Use retryWithBackoff to retry the acknowledgment
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

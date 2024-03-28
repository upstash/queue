import { Queue } from "@upstash/queue";
import { Redis } from "@upstash/redis";

const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL as string,
  token: process.env.UPSTASH_REDIS_REST_TOKEN as string,
});

type MessageBody = {
  message: string;
};
export async function POST(req: Request) {
  const { queueName } = await req.json();

  const queue = new Queue({
    redis: redis,
    concurrencyLimit: 5,
    queueName: queueName,
  });

  const receiveResponse = await queue.receiveMessage<MessageBody>();

  return Response.json({
    id: receiveResponse?.streamId,
    message: receiveResponse?.body.message,
  });
}

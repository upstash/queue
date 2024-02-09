"use client";
import React, { useState, useEffect } from "react";

import { Queue } from "./../../../src";
import * as uuid from "uuid";
import { Redis } from "@upstash/redis";
import { TransitionGroup, CSSTransition } from "react-transition-group";

type MessageBody = {
  content: string;
};

type MessageWithId = {
  id: string;
  body: MessageBody;
};

const queueName = `sqs-${uuid.v4().substring(0, 18)}`;

export default function Home() {
  const [messageInput, setMessageInput] = useState<string>(
    uuid.v4().substring(0, 18)
  );
  const [queueMessages, setQueueMessages] = useState<MessageWithId[]>([]);

  const send = async () => {
    fetch("/api/send", {
      method: "POST",
      body: JSON.stringify({ message: messageInput, queueName: queueName }),
    }).then(async (res) => {
      const data = await res.json();
      if (data.messageId) {
        setQueueMessages([
          ...queueMessages,
          {
            id: data.messageId,
            body: { content: messageInput },
          } as MessageWithId,
        ]);
      }
    });
  };

  const receive = async () => {
    fetch("/api/receive", {
      method: "POST",
      body: JSON.stringify({ queueName: queueName }),
    }).then(async (res) => {
      const data = await res.json();

      if (data.id && data.message) {
        setQueueMessages(queueMessages.filter((msg) => msg.id !== data.id));
      }
    });
  };
  return (
    <main>
      <header>
        <h1 className="text-4xl font-bold">
          Welcome to <span className="text-primary-500">@upstash/sqs</span>
        </h1>

        <p className="mt-4">
          This is an example of how to use @upstash/sqs as a FIFO queue in your
          Next.js application.
        </p>

        <p className="mt-4">
          You can create and consume messages from the queue using the buttons.
        </p>
      </header>

      <hr className="my-10" />
      <div className="h-[55vh]">
        <div className="p-4 bg-zinc-100 flex flex-col gap-2 rounded-xl w-96 mx-auto max-h-full overflow-y-scroll">
          <p className="text-lg font-bold">Queue</p>
          <TransitionGroup className="flex flex-col gap-2 h-min">
            {queueMessages.length > 0 &&
              queueMessages.map((message, index) => {
                return (
                  <CSSTransition
                    key={message.id}
                    timeout={250}
                    classNames="message"
                  >
                    <div className="flex flex-row items-center justify-between gap-4 w-full transform transition-all duration-500 ease-in-out">
                      <div className="bg-white px-2 h-full rounded-xl w-10 flex items-center justify-center text-sm">
                        {index}
                      </div>

                      <div className="bg-white  p-4 rounded-xl text-slate-900 shadow-sm flex flex-col gap-2  w-80">
                        <p className="font-semibold text-sm">
                          ID:{" "}
                          <span className="text-zinc-400  font-normal">
                            {message.id}
                          </span>
                        </p>

                        <p className="font-semibold">
                          Content:{" "}
                          <span className="text-zinc-800  font-normal">
                            {message.body.content}
                          </span>
                        </p>
                      </div>
                    </div>
                  </CSSTransition>
                );
              })}
          </TransitionGroup>
          {queueMessages.length === 0 && <p>No messages in the queue...</p>}
        </div>
      </div>
      <hr className="my-10" />

      <div className="flex flex-row gap-6 items-end w-full">
        <div className="flex flex-col w-full gap-2">
          <div className="w-full flex flex-row justify-between gap-2">
            <input
              type="text"
              className=" shadow-sm border border-zinc-400 rounded-xl px-4 py-2 w-full"
              placeholder="Enter message..."
              value={messageInput}
              onChange={(e) => setMessageInput(e.target.value)}
            />
            <button
              className="bg-zinc-400 text-white px-4 py-2 rounded-xl hover:bg-zinc-300"
              onClick={() => {
                const randomWord = uuid.v4().substring(0, 18);
                setMessageInput(randomWord);
              }}
            >
              Random
            </button>
          </div>
          <div className="flex justify-center  w-full gap-2">
            <button
              className="bg-emerald-500 text-white px-4 py-2 rounded-xl hover:bg-emerald-400 transition-all ease-in-out w-full"
              onClick={() => {
                send();
              }}
            >
              Produce
            </button>
          </div>
        </div>
        <button
          className="bg-blue-500 text-white px-4 py-2 rounded-xl hover:bg-blue-400 transition-all ease-in-out w-full h-1/2"
          onClick={() => {
            receive();
          }}
        >
          Consume
        </button>
      </div>
    </main>
  );
}

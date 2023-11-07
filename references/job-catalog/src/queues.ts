import { createExpressServer } from "@trigger.dev/express";
import { TriggerClient } from "@trigger.dev/sdk";
import { z } from "zod";

export const client = new TriggerClient({
  id: "job-catalog",
  apiKey: process.env["TRIGGER_API_KEY"],
  apiUrl: process.env["TRIGGER_API_URL"],
  verbose: false,
  ioLogLocalEnabled: true,
});

const queue = client.defineQueue({
  id: "my-queue",
  name: "Example Queue",
  icon: "route-2",
  schema: z.object({
    foo: z.string(),
  }),
  // steps: [
  //   {
  //     key: "filter-1",
  //     type: "FILTER",
  //     config: {
  //       foo: ["test"],
  //     },
  //   },
  // ],
});

const queueTwo = client.defineQueue({
  id: "my-queue-two",
  name: "Example Queue 2",
  icon: "route",
  schema: z.object({
    foo: z.string(),
  }),
  steps: [
    // {
    //   key: "filter-1",
    //   type: "FILTER",
    //   config: {
    //     foo: ["test"],
    //   },
    // },
  ],
});

client.defineJob({
  id: "queue-example",
  name: "Queue1: Push to Queues",
  version: "1.0.0",
  trigger: queue.pipeline([
    // queue.createFilter("filter-1", {
    //   foo: ["bar"],
    // }),
    // queue.createFilter("filter-2", {
    //   foo: [123],
    // }),
    // queue.createFilter("filter-3", {
    //   foo: ["123"],
    //   test: ["test"]
    // })
  ]),
  run: async (payload, io, ctx) => {
    await io.runTask("queue-1", () =>
      queue.push({
        payload: {
          foo: "foo",
        },
      })
    );

    await io.runTask("queue-2", () =>
      queueTwo.push({
        payload: {
          foo: "bar",
        },
      })
    );
  },
});

client.defineJob({
  id: "queue-example-2",
  name: "Queue1: Receive Foo",
  version: "1.0.0",
  trigger: queue.pipeline([
    queue.createFilter("filter-1", {
      foo: ["foo"],
    }),
  ]),
  run: async (payload, io, ctx) => {
    return payload;
  },
});

client.defineJob({
  id: "queue-example-3",
  name: "Queue2: Receive Bar",
  version: "1.0.0",
  trigger: queueTwo.pipeline([
    queueTwo.createFilter("filter-1", {
      foo: ["bar"],
    }),
  ]),
  run: async (payload, io, ctx) => {
    return payload;
  },
});

client.defineJob({
  id: "queue-example-4",
  name: "Queue1: Receive All",
  version: "1.0.0",
  trigger: queue.trigger(),
  run: async (payload, io, ctx) => {
    return payload;
  },
});

createExpressServer(client);

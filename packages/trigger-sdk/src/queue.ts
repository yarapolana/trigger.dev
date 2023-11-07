import { DisplayProperty, EventFilter, QueueMetadata, TriggerMetadata } from "@trigger.dev/core";
import { ParsedPayloadSchemaError } from "./errors";
import { Job } from "./job";
import { TriggerClient } from "./triggerClient";
import {
  EventSpecification,
  EventSpecificationExample,
  EventTypeFromSpecification,
  SchemaParser,
  Trigger,
} from "./types";
import { formatSchemaErrors } from "./utils/formatSchemaErrors";
import { slugifyId } from "./utils";
import { SendEventOptions } from "@trigger.dev/core";
import { SendEvent } from "@trigger.dev/core";
import { FilterStep } from "./triggers/eventTrigger";

type QueueOptions<TEventSpecification extends EventSpecification<any>> = {
  id: string;
  name?: string;
  batch: boolean;
  client: TriggerClient;
  enabled: boolean;
  event: TEventSpecification;
  steps?: FilterStep<EventTypeFromSpecification<TEventSpecification>>[];
};

export type EventOptions<TEvent> = {
  filter?: EventFilter<TEvent>;
};

export class Queue<TEventSpecification extends EventSpecification<any>> {
  #client: TriggerClient;
  #event: TEventSpecification;

  constructor(private readonly options: QueueOptions<TEventSpecification>) {
    this.#client = options.client;
    this.#event = options.event;
  }

  get id() {
    return this.options.id;
  }

  createFilter<TEvent = EventTypeFromSpecification<TEventSpecification>>(
    key: string,
    filter: EventFilter<TEvent>
  ): FilterStep<TEvent> {
    return {
      type: "FILTER" as const,
      key,
      config: filter,
    };
  }

  pipeline(
    steps: FilterStep<EventTypeFromSpecification<TEventSpecification>>[]
  ): EventQueueTrigger<TEventSpecification> {
    return new EventQueueTrigger({
      queueId: this.id,
      event: this.#event,
      pipeline: steps,
    });
  }

  trigger(
    options?: EventOptions<EventTypeFromSpecification<TEventSpecification>>
  ): EventQueueTrigger<TEventSpecification> {
    return new EventQueueTrigger({
      queueId: this.id,
      event: this.#event,
      filter: options?.filter,
    });
  }

  push(event: Omit<SendEvent, "name">, options?: Omit<SendEventOptions, "queueId">) {
    return this.#client.sendEvent(
      {
        ...event,
        name: `queue:${this.id}`,
      },
      {
        ...options,
        queueId: this.id,
      }
    );
  }

  toJSON(): QueueMetadata {
    return {
      id: this.id,
      name: this.options.name ?? this.id,
      icon: this.#event.icon,
      version: "1",
      enabled: this.options.enabled,
      event: this.#event,
      pipeline: this.options.steps ?? [],
    };
  }
}

type TriggerOptions<TEventSpecification extends EventSpecification<any>> = {
  queueId: string;
  event: TEventSpecification;
  filter?: EventFilter<EventTypeFromSpecification<TEventSpecification>>;
  pipeline?: FilterStep<EventTypeFromSpecification<TEventSpecification>>[];
};

class EventQueueTrigger<TEventSpecification extends EventSpecification<any>>
  implements Trigger<TEventSpecification>
{
  constructor(private readonly options: TriggerOptions<TEventSpecification>) {}

  toJSON(): TriggerMetadata {
    return {
      type: "static",
      title: this.options.queueId,
      pipeline: this.options.pipeline,
      properties: this.options.event.properties,
      rule: {
        event: this.options.event.name,
        payload: this.options.filter ?? {},
        source: this.options.event.source,
      },
    };
  }

  get event() {
    return this.options.event;
  }

  attachToJob(triggerClient: TriggerClient, job: Job<Trigger<TEventSpecification>, any>): void {}

  get preprocessRuns() {
    return false;
  }

  async verifyPayload(payload: ReturnType<TEventSpecification["parsePayload"]>) {
    return { success: true as const };
  }
}

export type CreateQueueOptions<TEvent> = {
  id: string;
  enabled?: boolean;
  source?: string;
  name?: string;
  icon?: string;
  examples?: EventSpecificationExample[];
  properties?: DisplayProperty[];
  schema?: SchemaParser<TEvent>;
  batch?: boolean;
  steps?: FilterStep<TEvent>[];
};

export function createQueue<TEvent extends any = any>(
  client: TriggerClient,
  options: CreateQueueOptions<TEvent>
): Queue<EventSpecification<TEvent>> {
  const id = slugifyId(options.id);

  return new Queue({
    id,
    enabled: options.enabled !== false,
    client,
    batch: !!options.batch,
    steps: options.steps,
    event: {
      name: `queue:${id}`,
      title: options.name ?? "Event Queue",
      source: options.source ?? "trigger.dev",
      icon: options.icon ?? "route-2",
      properties: options.properties,
      examples: options.examples,
      parsePayload: (rawPayload: any) => {
        if (options.schema) {
          const results = options.schema.safeParse(rawPayload);

          if (!results.success) {
            throw new ParsedPayloadSchemaError(formatSchemaErrors(results.error.issues));
          }

          return results.data;
        }

        return rawPayload as any;
      },
    },
  });
}

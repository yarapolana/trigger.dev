import { Attributes, Link, TraceFlags } from "@opentelemetry/api";
import { RandomIdGenerator } from "@opentelemetry/sdk-trace-base";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";
import {
  ExceptionEventProperties,
  ExceptionSpanEvent,
  PRIMARY_VARIANT,
  SemanticInternalAttributes,
  SpanEvent,
  SpanEvents,
  SpanMessagingEvent,
  TaskEventStyle,
  correctErrorStackTrace,
  createPacketAttributesAsJson,
  flattenAttributes,
  NULL_SENTINEL,
  isExceptionSpanEvent,
  omit,
  unflattenAttributes,
} from "@trigger.dev/core/v3";
import { Prisma, TaskEvent, TaskEventStatus, type TaskEventKind } from "@trigger.dev/database";
import Redis, { RedisOptions } from "ioredis";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:stream";
import { $replica, PrismaClient, PrismaReplicaClient, prisma } from "~/db.server";
import { env } from "~/env.server";
import { AuthenticatedEnvironment } from "~/services/apiAuth.server";
import { logger } from "~/services/logger.server";
import { DynamicFlushScheduler } from "./dynamicFlushScheduler.server";
import { singleton } from "~/utils/singleton";
import { Gauge } from "prom-client";
import { metricsRegister } from "~/metrics.server";

export type CreatableEvent = Omit<
  Prisma.TaskEventCreateInput,
  "id" | "createdAt" | "properties" | "metadata" | "style" | "output" | "payload"
> & {
  properties: Attributes;
  metadata: Attributes | undefined;
  style: Attributes | undefined;
  output: Attributes | string | boolean | number | undefined;
  payload: Attributes | string | boolean | number | undefined;
};

export type CreatableEventKind = TaskEventKind;
export type CreatableEventStatus = TaskEventStatus;
export type CreatableEventEnvironmentType = CreatableEvent["environmentType"];

export type TraceAttributes = Partial<
  Pick<
    CreatableEvent,
    | "attemptId"
    | "isError"
    | "isCancelled"
    | "runId"
    | "runIsTest"
    | "output"
    | "outputType"
    | "metadata"
    | "properties"
    | "style"
    | "queueId"
    | "queueName"
    | "batchId"
    | "payload"
    | "payloadType"
    | "idempotencyKey"
  >
>;

export type SetAttribute<T extends TraceAttributes> = (key: keyof T, value: T[keyof T]) => void;

export type TraceEventOptions = {
  kind?: CreatableEventKind;
  context?: Record<string, string | undefined>;
  spanParentAsLink?: boolean;
  parentAsLinkType?: "trigger" | "replay";
  spanIdSeed?: string;
  attributes: TraceAttributes;
  environment: AuthenticatedEnvironment;
  taskSlug: string;
  startTime?: bigint;
  endTime?: Date;
  immediate?: boolean;
};

export type EventBuilder = {
  traceId: string;
  spanId: string;
  setAttribute: SetAttribute<TraceAttributes>;
};

export type EventRepoConfig = {
  batchSize: number;
  batchInterval: number;
  redis: RedisOptions;
  retentionInDays: number;
};

export type QueryOptions = Prisma.TaskEventWhereInput;

export type TaskEventRecord = TaskEvent;

export type QueriedEvent = Prisma.TaskEventGetPayload<{
  select: {
    id: true;
    spanId: true;
    parentId: true;
    runId: true;
    idempotencyKey: true;
    message: true;
    style: true;
    startTime: true;
    duration: true;
    isError: true;
    isPartial: true;
    isCancelled: true;
    level: true;
    events: true;
  };
}>;

export type PreparedEvent = Omit<QueriedEvent, "events" | "style" | "duration"> & {
  duration: number;
  events: SpanEvents;
  style: TaskEventStyle;
};

export type SpanLink =
  | {
      type: "run";
      icon?: string;
      title: string;
      runId: string;
    }
  | {
      type: "span";
      icon?: string;
      title: string;
      traceId: string;
      spanId: string;
    };

export type SpanSummary = {
  recordId: string;
  id: string;
  parentId: string | undefined;
  runId: string;
  data: {
    message: string;
    style: TaskEventStyle;
    events: SpanEvents;
    startTime: Date;
    duration: number;
    isError: boolean;
    isPartial: boolean;
    isCancelled: boolean;
    level: NonNullable<CreatableEvent["level"]>;
  };
};

export type TraceSummary = { rootSpan: SpanSummary; spans: Array<SpanSummary> };

export type UpdateEventOptions = {
  attributes: TraceAttributes;
  endTime?: Date;
  immediate?: boolean;
};

export class EventRepository {
  private readonly _flushScheduler: DynamicFlushScheduler<CreatableEvent>;
  private _randomIdGenerator = new RandomIdGenerator();
  private _redisPublishClient: Redis;
  private _subscriberCount = 0;

  get subscriberCount() {
    return this._subscriberCount;
  }

  constructor(
    private db: PrismaClient = prisma,
    private readReplica: PrismaReplicaClient = $replica,
    private readonly _config: EventRepoConfig
  ) {
    this._flushScheduler = new DynamicFlushScheduler({
      batchSize: _config.batchSize,
      flushInterval: _config.batchInterval,
      callback: this.#flushBatch.bind(this),
    });

    this._redisPublishClient = new Redis(this._config.redis);
  }

  async insert(event: CreatableEvent) {
    this._flushScheduler.addToBatch([event]);
  }

  async insertImmediate(event: CreatableEvent) {
    await this.db.taskEvent.create({
      data: event as Prisma.TaskEventCreateInput,
    });

    this.#publishToRedis([event]);
  }

  async insertMany(events: CreatableEvent[]) {
    this._flushScheduler.addToBatch(events);
  }

  async insertManyImmediate(events: CreatableEvent[]) {
    return await this.#flushBatch(events);
  }

  async completeEvent(spanId: string, options?: UpdateEventOptions) {
    const events = await this.queryIncompleteEvents({ spanId });

    if (events.length === 0) {
      return;
    }

    const event = events[0];

    const output = options?.attributes.output
      ? await createPacketAttributesAsJson(
          options?.attributes.output,
          options?.attributes.outputType ?? "application/json"
        )
      : undefined;

    logger.debug("Completing event", {
      spanId,
      eventId: event.id,
    });

    await this.insert({
      ...omit(event, "id"),
      isPartial: false,
      isError: options?.attributes.isError ?? false,
      isCancelled: false,
      status: options?.attributes.isError ? "ERROR" : "OK",
      links: event.links ?? [],
      events: event.events ?? [],
      duration: calculateDurationFromStart(event.startTime, options?.endTime),
      properties: event.properties as Attributes,
      metadata: event.metadata as Attributes,
      style: event.style as Attributes,
      output: output,
      outputType:
        options?.attributes.outputType === "application/store" ||
        options?.attributes.outputType === "text/plain"
          ? options?.attributes.outputType
          : "application/json",
      payload: event.payload as Attributes,
      payloadType: event.payloadType,
    });
  }

  async cancelEvent(event: TaskEventRecord, cancelledAt: Date, reason: string) {
    if (!event.isPartial) {
      return;
    }

    await this.insertImmediate({
      ...omit(event, "id"),
      isPartial: false,
      isError: false,
      isCancelled: true,
      status: "ERROR",
      links: event.links ?? [],
      events: [
        {
          name: "cancellation",
          time: cancelledAt,
          properties: {
            reason,
          },
        },
        ...((event.events as any[]) ?? []),
      ],
      duration: calculateDurationFromStart(event.startTime, cancelledAt),
      properties: event.properties as Attributes,
      metadata: event.metadata as Attributes,
      style: event.style as Attributes,
      output: event.output as Attributes,
      outputType: event.outputType,
      payload: event.payload as Attributes,
      payloadType: event.payloadType,
    });
  }

  async crashEvent({
    event,
    crashedAt,
    exception,
  }: {
    event: TaskEventRecord;
    crashedAt: Date;
    exception: ExceptionEventProperties;
  }) {
    if (!event.isPartial) {
      return;
    }

    await this.insertImmediate({
      ...omit(event, "id"),
      isPartial: false,
      isError: true,
      isCancelled: false,
      status: "ERROR",
      links: event.links ?? [],
      events: [
        {
          name: "exception",
          time: crashedAt,
          properties: {
            exception,
          },
        } satisfies ExceptionSpanEvent,
        ...((event.events as any[]) ?? []),
      ],
      duration: calculateDurationFromStart(event.startTime, crashedAt),
      properties: event.properties as Attributes,
      metadata: event.metadata as Attributes,
      style: event.style as Attributes,
      output: event.output as Attributes,
      outputType: event.outputType,
      payload: event.payload as Attributes,
      payloadType: event.payloadType,
    });
  }

  async queryEvents(queryOptions: QueryOptions): Promise<TaskEventRecord[]> {
    return await this.db.taskEvent.findMany({
      where: queryOptions,
    });
  }

  async queryIncompleteEvents(queryOptions: QueryOptions) {
    // First we will find all the events that match the query options (selecting minimal data).
    const taskEvents = await this.db.taskEvent.findMany({
      where: queryOptions,
      select: {
        spanId: true,
        isPartial: true,
        isCancelled: true,
      },
    });

    const filteredTaskEvents = taskEvents.filter((event) => {
      // Event must be partial
      if (!event.isPartial) return false;

      // If the event is cancelled, it is not incomplete
      if (event.isCancelled) return false;

      // There must not be another complete event with the same spanId
      const hasCompleteDuplicate = taskEvents.some(
        (otherEvent) =>
          otherEvent.spanId === event.spanId && !otherEvent.isPartial && !otherEvent.isCancelled
      );

      return !hasCompleteDuplicate;
    });

    return this.queryEvents({
      spanId: {
        in: filteredTaskEvents.map((event) => event.spanId),
      },
    });
  }

  public async getTraceSummary(traceId: string): Promise<TraceSummary | undefined> {
    const events = await this.readReplica.taskEvent.findMany({
      select: {
        id: true,
        spanId: true,
        parentId: true,
        runId: true,
        idempotencyKey: true,
        message: true,
        style: true,
        startTime: true,
        duration: true,
        isError: true,
        isPartial: true,
        isCancelled: true,
        level: true,
        events: true,
      },
      where: {
        traceId,
      },
      orderBy: {
        startTime: "asc",
      },
    });

    const preparedEvents = removeDuplicateEvents(events.map(prepareEvent));

    const spans = preparedEvents.map((event) => {
      const ancestorCancelled = isAncestorCancelled(preparedEvents, event.spanId);
      const duration = calculateDurationIfAncestorIsCancelled(
        preparedEvents,
        event.spanId,
        event.duration
      );

      return {
        recordId: event.id,
        id: event.spanId,
        parentId: event.parentId ?? undefined,
        runId: event.runId,
        idempotencyKey: event.idempotencyKey,
        data: {
          message: event.message,
          style: event.style,
          duration,
          isError: event.isError,
          isPartial: ancestorCancelled ? false : event.isPartial,
          isCancelled: event.isCancelled === true ? true : event.isPartial && ancestorCancelled,
          startTime: getDateFromNanoseconds(event.startTime),
          level: event.level,
          events: event.events,
        },
      };
    });

    const rootSpanId = events.find((event) => !event.parentId);
    if (!rootSpanId) {
      return;
    }

    const rootSpan = spans.find((span) => span.id === rootSpanId.spanId);

    if (!rootSpan) {
      return;
    }

    return {
      rootSpan,
      spans,
    };
  }

  // A Span can be cancelled if it is partial and has a parent that is cancelled
  // And a span's duration, if it is partial and has a cancelled parent, is the time between the start of the span and the time of the cancellation event of the parent
  public async getSpan(spanId: string, traceId: string) {
    const traceSummary = await this.getTraceSummary(traceId);

    const span = traceSummary?.spans.find((span) => span.id === spanId);

    if (!span) {
      return;
    }

    const fullEvent = await this.readReplica.taskEvent.findUnique({
      where: {
        id: span.recordId,
      },
    });

    if (!fullEvent) {
      return;
    }

    const output = rehydrateJson(fullEvent.output);
    const payload = rehydrateJson(fullEvent.payload);

    const show = rehydrateShow(fullEvent.properties);

    const properties = sanitizedAttributes(fullEvent.properties);

    const messagingEvent = SpanMessagingEvent.optional().safeParse((properties as any)?.messaging);

    const links: SpanLink[] = [];

    if (messagingEvent.success && messagingEvent.data) {
      if (messagingEvent.data.message && "id" in messagingEvent.data.message) {
        if (messagingEvent.data.message.id.startsWith("run_")) {
          links.push({
            type: "run",
            icon: "runs",
            title: `Run ${messagingEvent.data.message.id}`,
            runId: messagingEvent.data.message.id,
          });
        }
      }
    }

    const backLinks = fullEvent.links as any as Link[] | undefined;

    if (backLinks && backLinks.length > 0) {
      backLinks.forEach((l) => {
        const title = String(
          l.attributes?.[SemanticInternalAttributes.LINK_TITLE] ?? "Triggered by"
        );

        links.push({
          type: "span",
          icon: "trigger",
          title,
          traceId: l.context.traceId,
          spanId: l.context.spanId,
        });
      });
    }

    const events = transformEvents(span.data.events, fullEvent.metadata as Attributes);

    return {
      ...fullEvent,
      ...span.data,
      payload,
      output,
      properties,
      events,
      show,
      links,
    };
  }

  public async recordEvent(message: string, options: TraceEventOptions) {
    const propagatedContext = extractContextFromCarrier(options.context ?? {});

    const startTime = options.startTime ?? getNowInNanoseconds();
    const duration = options.endTime ? calculateDurationFromStart(startTime, options.endTime) : 100;

    const traceId = propagatedContext?.traceparent?.traceId ?? this.generateTraceId();
    const parentId = propagatedContext?.traceparent?.spanId;
    const tracestate = propagatedContext?.tracestate;
    const spanId = options.spanIdSeed
      ? this.#generateDeterministicSpanId(traceId, options.spanIdSeed)
      : this.generateSpanId();

    const metadata = {
      [SemanticInternalAttributes.ENVIRONMENT_ID]: options.environment.id,
      [SemanticInternalAttributes.ENVIRONMENT_TYPE]: options.environment.type,
      [SemanticInternalAttributes.ORGANIZATION_ID]: options.environment.organizationId,
      [SemanticInternalAttributes.PROJECT_ID]: options.environment.projectId,
      [SemanticInternalAttributes.PROJECT_REF]: options.environment.project.externalRef,
      [SemanticInternalAttributes.RUN_ID]: options.attributes.runId,
      [SemanticInternalAttributes.RUN_IS_TEST]: options.attributes.runIsTest ?? false,
      [SemanticInternalAttributes.BATCH_ID]: options.attributes.batchId ?? undefined,
      [SemanticInternalAttributes.TASK_SLUG]: options.taskSlug,
      [SemanticResourceAttributes.SERVICE_NAME]: "api server",
      [SemanticResourceAttributes.SERVICE_NAMESPACE]: "trigger.dev",
      ...options.attributes.metadata,
    };

    const style = {
      [SemanticInternalAttributes.STYLE_ICON]: "play",
    };

    if (!options.attributes.runId) {
      throw new Error("runId is required");
    }

    const event: CreatableEvent = {
      traceId,
      spanId,
      parentId,
      tracestate,
      message: message,
      serviceName: "api server",
      serviceNamespace: "trigger.dev",
      level: "TRACE",
      kind: options.kind,
      status: "OK",
      startTime,
      isPartial: false,
      duration, // convert to nanoseconds
      environmentId: options.environment.id,
      environmentType: options.environment.type,
      organizationId: options.environment.organizationId,
      projectId: options.environment.projectId,
      projectRef: options.environment.project.externalRef,
      runId: options.attributes.runId,
      runIsTest: options.attributes.runIsTest ?? false,
      taskSlug: options.taskSlug,
      queueId: options.attributes.queueId,
      queueName: options.attributes.queueName,
      batchId: options.attributes.batchId ?? undefined,
      properties: {
        ...style,
        ...(flattenAttributes(metadata, SemanticInternalAttributes.METADATA) as Record<
          string,
          string
        >),
        ...options.attributes.properties,
      },
      metadata: metadata,
      style: stripAttributePrefix(style, SemanticInternalAttributes.STYLE),
      output: undefined,
      outputType: undefined,
      payload: undefined,
      payloadType: undefined,
    };

    if (options.immediate) {
      await this.insertImmediate(event);
    } else {
      this._flushScheduler.addToBatch([event]);
    }

    return event;
  }

  public async traceEvent<TResult>(
    message: string,
    options: TraceEventOptions & { incomplete?: boolean },
    callback: (
      e: EventBuilder,
      traceContext: Record<string, string | undefined>
    ) => Promise<TResult>
  ): Promise<TResult> {
    const propagatedContext = extractContextFromCarrier(options.context ?? {});

    const start = process.hrtime.bigint();
    const startTime = getNowInNanoseconds();

    const traceId = options.spanParentAsLink
      ? this.generateTraceId()
      : propagatedContext?.traceparent?.traceId ?? this.generateTraceId();
    const parentId = options.spanParentAsLink ? undefined : propagatedContext?.traceparent?.spanId;
    const tracestate = options.spanParentAsLink ? undefined : propagatedContext?.tracestate;
    const spanId = options.spanIdSeed
      ? this.#generateDeterministicSpanId(traceId, options.spanIdSeed)
      : this.generateSpanId();

    const traceContext = {
      traceparent: `00-${traceId}-${spanId}-01`,
    };

    const links: Link[] =
      options.spanParentAsLink && propagatedContext?.traceparent
        ? [
            {
              context: {
                traceId: propagatedContext.traceparent.traceId,
                spanId: propagatedContext.traceparent.spanId,
                traceFlags: TraceFlags.SAMPLED,
              },
              attributes: {
                [SemanticInternalAttributes.LINK_TITLE]:
                  options.parentAsLinkType === "replay" ? "Replay of" : "Triggered by",
              },
            },
          ]
        : [];

    const eventBuilder = {
      traceId,
      spanId,
      setAttribute: (key: keyof TraceAttributes, value: TraceAttributes[keyof TraceAttributes]) => {
        if (value) {
          // We need to merge the attributes with the existing attributes
          const existingValue = options.attributes[key];

          if (existingValue && typeof existingValue === "object" && typeof value === "object") {
            // @ts-ignore
            options.attributes[key] = { ...existingValue, ...value };
          } else {
            // @ts-ignore
            options.attributes[key] = value;
          }
        }
      },
    };

    const result = await callback(eventBuilder, traceContext);

    const duration = process.hrtime.bigint() - start;

    const metadata = {
      [SemanticInternalAttributes.ENVIRONMENT_ID]: options.environment.id,
      [SemanticInternalAttributes.ENVIRONMENT_TYPE]: options.environment.type,
      [SemanticInternalAttributes.ORGANIZATION_ID]: options.environment.organizationId,
      [SemanticInternalAttributes.PROJECT_ID]: options.environment.projectId,
      [SemanticInternalAttributes.PROJECT_REF]: options.environment.project.externalRef,
      [SemanticInternalAttributes.RUN_ID]: options.attributes.runId,
      [SemanticInternalAttributes.RUN_IS_TEST]: options.attributes.runIsTest ?? false,
      [SemanticInternalAttributes.BATCH_ID]: options.attributes.batchId ?? undefined,
      [SemanticInternalAttributes.TASK_SLUG]: options.taskSlug,
      [SemanticResourceAttributes.SERVICE_NAME]: "api server",
      [SemanticResourceAttributes.SERVICE_NAMESPACE]: "trigger.dev",
      ...options.attributes.metadata,
    };

    const style = {
      [SemanticInternalAttributes.STYLE_ICON]: "task",
      [SemanticInternalAttributes.STYLE_VARIANT]: PRIMARY_VARIANT,
      ...options.attributes.style,
    };

    if (!options.attributes.runId) {
      throw new Error("runId is required");
    }

    const event: CreatableEvent = {
      traceId,
      spanId,
      parentId,
      tracestate,
      duration: options.incomplete ? 0 : duration,
      isPartial: options.incomplete,
      message: message,
      serviceName: "api server",
      serviceNamespace: "trigger.dev",
      level: "TRACE",
      kind: options.kind,
      status: "OK",
      startTime,
      environmentId: options.environment.id,
      environmentType: options.environment.type,
      organizationId: options.environment.organizationId,
      projectId: options.environment.projectId,
      projectRef: options.environment.project.externalRef,
      runId: options.attributes.runId,
      runIsTest: options.attributes.runIsTest ?? false,
      taskSlug: options.taskSlug,
      queueId: options.attributes.queueId,
      queueName: options.attributes.queueName,
      batchId: options.attributes.batchId ?? undefined,
      properties: {
        ...(flattenAttributes(metadata, SemanticInternalAttributes.METADATA) as Record<
          string,
          string
        >),
        ...flattenAttributes(options.attributes.properties),
      },
      metadata: metadata,
      style: stripAttributePrefix(style, SemanticInternalAttributes.STYLE),
      output: undefined,
      outputType: undefined,
      links: links as unknown as Prisma.InputJsonValue,
      payload: options.attributes.payload,
      payloadType: options.attributes.payloadType,
      idempotencyKey: options.attributes.idempotencyKey,
    };

    if (options.immediate) {
      await this.insertImmediate(event);
    } else {
      this._flushScheduler.addToBatch([event]);
    }

    return result;
  }

  async subscribeToTrace(traceId: string) {
    const redis = new Redis(this._config.redis);

    const channel = `events:${traceId}:*`;

    // Subscribe to the channel.
    await redis.psubscribe(channel);

    // Increment the subscriber count.
    this._subscriberCount++;

    const eventEmitter = new EventEmitter();

    // Define the message handler.
    redis.on("pmessage", (pattern, channelReceived, message) => {
      if (channelReceived.startsWith(`events:${traceId}:`)) {
        eventEmitter.emit("message", message);
      }
    });

    // Return a function that can be used to unsubscribe.
    const unsubscribe = async () => {
      await redis.punsubscribe(channel);
      redis.quit();
      this._subscriberCount--;
    };

    return {
      unsubscribe,
      eventEmitter,
    };
  }

  async #flushBatch(batch: CreatableEvent[]) {
    const events = excludePartialEventsWithCorrespondingFullEvent(batch);

    await this.db.taskEvent.createMany({
      data: events as Prisma.TaskEventCreateManyInput[],
    });

    this.#publishToRedis(events);
  }

  async #publishToRedis(events: CreatableEvent[]) {
    if (events.length === 0) return;
    const uniqueTraceSpans = new Set(events.map((e) => `events:${e.traceId}:${e.spanId}`));
    for (const id of uniqueTraceSpans) {
      await this._redisPublishClient.publish(id, new Date().toISOString());
    }
  }

  public generateTraceId() {
    return this._randomIdGenerator.generateTraceId();
  }

  public generateSpanId() {
    return this._randomIdGenerator.generateSpanId();
  }

  public async truncateEvents() {
    await this.db.taskEvent.deleteMany({
      where: {
        createdAt: {
          lt: new Date(Date.now() - this._config.retentionInDays * 24 * 60 * 60 * 1000),
        },
      },
    });
  }

  /**
   * Returns a deterministically random 8-byte span ID formatted/encoded as a 16 lowercase hex
   * characters corresponding to 64 bits, based on the trace ID and seed.
   */
  #generateDeterministicSpanId(traceId: string, seed: string) {
    const hash = createHash("sha1");
    hash.update(traceId);
    hash.update(seed);
    const buffer = hash.digest();
    let hexString = "";
    for (let i = 0; i < 8; i++) {
      const val = buffer.readUInt8(i);
      const str = val.toString(16).padStart(2, "0");
      hexString += str;
    }
    return hexString;
  }
}

export const eventRepository = singleton("eventRepo", initializeEventRepo);

function initializeEventRepo() {
  const repo = new EventRepository(prisma, $replica, {
    batchSize: env.EVENTS_BATCH_SIZE,
    batchInterval: env.EVENTS_BATCH_INTERVAL,
    retentionInDays: env.EVENTS_DEFAULT_LOG_RETENTION,
    redis: {
      port: env.REDIS_PORT,
      host: env.REDIS_HOST,
      username: env.REDIS_USERNAME,
      password: env.REDIS_PASSWORD,
      enableAutoPipelining: true,
      ...(env.REDIS_TLS_DISABLED === "true" ? {} : { tls: {} }),
    },
  });

  new Gauge({
    name: "event_repository_subscriber_count",
    help: "Number of event repository subscribers",
    collect() {
      this.set(repo.subscriberCount);
    },
    registers: [metricsRegister],
  });

  return repo;
}

export function stripAttributePrefix(attributes: Attributes, prefix: string) {
  const result: Attributes = {};

  for (const [key, value] of Object.entries(attributes)) {
    if (key.startsWith(prefix)) {
      result[key.slice(prefix.length + 1)] = value;
    } else {
      result[key] = value;
    }
  }
  return result;
}

/**
 * Filters out partial events from a batch of creatable events, excluding those that have a corresponding full event.
 * @param batch - The batch of creatable events to filter.
 * @returns The filtered array of creatable events, excluding partial events with corresponding full events.
 */
function excludePartialEventsWithCorrespondingFullEvent(batch: CreatableEvent[]): CreatableEvent[] {
  const partialEvents = batch.filter((event) => event.isPartial);
  const fullEvents = batch.filter((event) => !event.isPartial);

  return fullEvents.concat(
    partialEvents.filter((partialEvent) => {
      return !fullEvents.some((fullEvent) => fullEvent.spanId === partialEvent.spanId);
    })
  );
}

function extractContextFromCarrier(carrier: Record<string, string | undefined>) {
  const traceparent = carrier["traceparent"];
  const tracestate = carrier["tracestate"];

  return {
    traceparent: parseTraceparent(traceparent),
    tracestate,
  };
}

function parseTraceparent(traceparent?: string): { traceId: string; spanId: string } | undefined {
  if (!traceparent) {
    return undefined;
  }

  const parts = traceparent.split("-");

  if (parts.length !== 4) {
    return undefined;
  }

  const [version, traceId, spanId, flags] = parts;

  if (version !== "00") {
    return undefined;
  }

  return { traceId, spanId };
}

function prepareEvent(event: QueriedEvent): PreparedEvent {
  return {
    ...event,
    duration: Number(event.duration),
    events: parseEventsField(event.events),
    style: parseStyleField(event.style),
  };
}

function parseEventsField(events: Prisma.JsonValue): SpanEvents {
  const eventsUnflattened = events
    ? (events as any[]).map((e) => ({
        ...e,
        properties: unflattenAttributes(e.properties as Attributes),
      }))
    : undefined;

  const spanEvents = SpanEvents.safeParse(eventsUnflattened);

  if (spanEvents.success) {
    return spanEvents.data;
  }

  return [];
}

function parseStyleField(style: Prisma.JsonValue): TaskEventStyle {
  const parsedStyle = TaskEventStyle.safeParse(unflattenAttributes(style as Attributes));

  if (parsedStyle.success) {
    return parsedStyle.data;
  }

  return {};
}

function isAncestorCancelled(events: PreparedEvent[], spanId: string) {
  const event = events.find((event) => event.spanId === spanId);

  if (!event) {
    return false;
  }

  if (event.isCancelled) {
    return true;
  }

  if (event.parentId) {
    return isAncestorCancelled(events, event.parentId);
  }

  return false;
}

function calculateDurationIfAncestorIsCancelled(
  events: PreparedEvent[],
  spanId: string,
  defaultDuration: number
) {
  const event = events.find((event) => event.spanId === spanId);

  if (!event) {
    return defaultDuration;
  }

  if (event.isCancelled) {
    return defaultDuration;
  }

  if (!event.isPartial) {
    return defaultDuration;
  }

  if (event.parentId) {
    const cancelledAncestor = findFirstCancelledAncestor(events, event.parentId);

    if (cancelledAncestor) {
      // We need to get the cancellation time from the cancellation span event
      const cancellationEvent = cancelledAncestor.events.find(
        (event) => event.name === "cancellation"
      );

      if (cancellationEvent) {
        return calculateDurationFromStart(event.startTime, cancellationEvent.time);
      }
    }
  }

  return defaultDuration;
}

function findFirstCancelledAncestor(events: PreparedEvent[], spanId: string) {
  const event = events.find((event) => event.spanId === spanId);
  if (!event) {
    return;
  }

  if (event.isCancelled) {
    return event;
  }

  if (event.parentId) {
    return findFirstCancelledAncestor(events, event.parentId);
  }

  return;
}

// Prioritize spans with the same id, keeping the completed spans over partial spans
// Completed spans are either !isPartial or isCancelled
function removeDuplicateEvents(events: PreparedEvent[]) {
  const dedupedEvents = new Map<string, PreparedEvent>();

  for (const event of events) {
    const existingEvent = dedupedEvents.get(event.spanId);

    if (!existingEvent) {
      dedupedEvents.set(event.spanId, event);
      continue;
    }

    if (event.isCancelled || !event.isPartial) {
      dedupedEvents.set(event.spanId, event);
    }
  }

  return Array.from(dedupedEvents.values());
}

function isEmptyJson(json: Prisma.JsonValue) {
  if (json === null) {
    return true;
  }

  return false;
}

function sanitizedAttributes(json: Prisma.JsonValue) {
  if (json === null || json === undefined) {
    return;
  }

  const withoutPrivateProperties = removePrivateProperties(json as Attributes);
  if (!withoutPrivateProperties) {
    return;
  }

  return unflattenAttributes(withoutPrivateProperties);
}
// removes keys that start with a $ sign. If there are no keys left, return undefined
function removePrivateProperties(
  attributes: Attributes | undefined | null
): Attributes | undefined {
  if (!attributes) {
    return undefined;
  }

  const result: Attributes = {};

  for (const [key, value] of Object.entries(attributes)) {
    if (key.startsWith("$")) {
      continue;
    }

    result[key] = value;
  }

  if (Object.keys(result).length === 0) {
    return undefined;
  }

  return result;
}

function transformEvents(events: SpanEvents, properties: Attributes): SpanEvents {
  return (events ?? []).map((event) => transformEvent(event, properties));
}

function transformEvent(event: SpanEvent, properties: Attributes): SpanEvent {
  if (isExceptionSpanEvent(event)) {
    return {
      ...event,
      properties: {
        exception: transformException(event.properties.exception, properties),
      },
    };
  }

  return event;
}

function transformException(
  exception: ExceptionEventProperties,
  properties: Attributes
): ExceptionEventProperties {
  const projectDirAttributeValue = properties[SemanticInternalAttributes.PROJECT_DIR];

  if (typeof projectDirAttributeValue !== "string") {
    return exception;
  }

  return {
    ...exception,
    stacktrace: exception.stacktrace
      ? correctErrorStackTrace(exception.stacktrace, projectDirAttributeValue, {
          removeFirstLine: true,
        })
      : undefined,
  };
}

function filteredAttributes(attributes: Attributes, prefix: string): Attributes {
  const result: Attributes = {};

  for (const [key, value] of Object.entries(attributes)) {
    if (key.startsWith(prefix)) {
      result[key] = value;
    }
  }

  return result;
}

function calculateDurationFromStart(startTime: bigint, endTime: Date = new Date()) {
  return Number(BigInt(endTime.getTime() * 1_000_000) - startTime);
}

function getNowInNanoseconds(): bigint {
  return BigInt(new Date().getTime() * 1_000_000);
}

function getDateFromNanoseconds(nanoseconds: bigint) {
  return new Date(Number(nanoseconds) / 1_000_000);
}

function rehydrateJson(json: Prisma.JsonValue): any {
  if (json === null) {
    return undefined;
  }

  if (json === NULL_SENTINEL) {
    return null;
  }

  if (typeof json === "string") {
    return json;
  }

  if (typeof json === "number") {
    return json;
  }

  if (typeof json === "boolean") {
    return json;
  }

  if (Array.isArray(json)) {
    return json.map((item) => rehydrateJson(item));
  }

  if (typeof json === "object") {
    return unflattenAttributes(json as Attributes);
  }

  return null;
}

function rehydrateShow(properties: Prisma.JsonValue): { actions?: boolean } | undefined {
  if (properties === null || properties === undefined) {
    return;
  }

  if (typeof properties !== "object") {
    return;
  }

  if (Array.isArray(properties)) {
    return;
  }

  const actions = properties[SemanticInternalAttributes.SHOW_ACTIONS];

  if (typeof actions === "boolean") {
    return { actions };
  }

  return;
}

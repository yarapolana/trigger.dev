import type { RawEvent, SendEventOptions } from "@trigger.dev/core";
import { $transaction, PrismaClientOrTransaction, PrismaErrorSchema, prisma } from "~/db.server";
import type { AuthenticatedEnvironment } from "~/services/apiAuth.server";
import { workerQueue } from "~/services/worker.server";
import { logger } from "../logger.server";
import { EventRecord, ExternalAccount, Prisma, Queue } from "@trigger.dev/database";

type UpdateEventInput = {
  tx: PrismaClientOrTransaction;
  existingEventLog: EventRecord;
  reqEvent: RawEvent;
  queueId: EventRecord["queueId"];
  deliverAt?: Date;
};

type CreateEventInput = {
  tx: PrismaClientOrTransaction;
  event: RawEvent;
  queueId: EventRecord["queueId"];
  environment: AuthenticatedEnvironment;
  deliverAt?: Date;
  sourceContext?: { id: string; metadata?: any };
  externalAccount?: ExternalAccount;
  eventSource?: EventSource;
};

type EventSource = {
  httpEndpointId?: string;
  httpEndpointEnvironmentId?: string;
};

const EVENT_UPDATE_THRESHOLD_WINDOW_IN_MSECS = 5 * 1000; // 5 seconds

export class IngestSendEvent {
  #prismaClient: PrismaClientOrTransaction;

  constructor(prismaClient: PrismaClientOrTransaction = prisma, private deliverEvents = true) {
    this.#prismaClient = prismaClient;
  }

  #calculateDeliverAt(options?: SendEventOptions) {
    // If deliverAt is a string and a valid date, convert it to a Date object
    if (options?.deliverAt) {
      return options?.deliverAt;
    }

    // deliverAfter is the number of seconds to wait before delivering the event
    if (options?.deliverAfter) {
      return new Date(Date.now() + options.deliverAfter * 1000);
    }

    return undefined;
  }

  public async call(
    environment: AuthenticatedEnvironment,
    event: RawEvent,
    options?: SendEventOptions,
    sourceContext?: { id: string; metadata?: any },
    eventSource?: EventSource
  ) {
    try {
      const deliverAt = this.#calculateDeliverAt(options);

      return await $transaction(this.#prismaClient, async (tx) => {
        const externalAccount = options?.accountId
          ? await tx.externalAccount.upsert({
              where: {
                environmentId_identifier: {
                  environmentId: environment.id,
                  identifier: options.accountId,
                },
              },
              create: {
                environmentId: environment.id,
                organizationId: environment.organizationId,
                identifier: options.accountId,
              },
              update: {},
            })
          : undefined;

        const existingEventLog = await tx.eventRecord.findUnique({
          where: {
            eventId_environmentId: {
              eventId: event.id,
              environmentId: environment.id,
            },
          },
        });

        let queueId: EventRecord["queueId"] = null;

        if (options?.queueId) {
          const queue = await tx.queue.findUniqueOrThrow({
            where: {
              projectId_slug: {
                projectId: environment.projectId,
                slug: options.queueId,
              },
            },
          });

          queueId = queue.id;
        }

        const eventLog = await (existingEventLog
          ? this.updateEvent({ tx, existingEventLog, reqEvent: event, queueId, deliverAt })
          : this.createEvent({
              tx,
              event,
              queueId,
              environment,
              deliverAt,
              sourceContext,
              externalAccount,
              eventSource,
            }));

        return eventLog;
      });
    } catch (error) {
      const prismaError = PrismaErrorSchema.safeParse(error);

      if (!prismaError.success) {
        logger.debug("Error parsing prisma error", {
          error,
          parseError: prismaError.error.format(),
        });

        throw error;
      }

      throw error;
    }
  }

  private async createEvent({
    tx,
    event,
    queueId,
    environment,
    deliverAt,
    sourceContext,
    externalAccount,
    eventSource,
  }: CreateEventInput) {
    const eventLog = await tx.eventRecord.create({
      data: {
        organizationId: environment.organizationId,
        projectId: environment.projectId,
        environmentId: environment.id,
        eventId: event.id,
        queueId,
        name: event.name,
        timestamp: event.timestamp ?? new Date(),
        payload: event.payload ?? {},
        payloadType: event.payloadType,
        context: event.context ?? {},
        source: event.source ?? "trigger.dev",
        sourceContext,
        deliverAt: deliverAt,
        externalAccountId: externalAccount ? externalAccount.id : undefined,
        httpEndpointId: eventSource?.httpEndpointId,
        httpEndpointEnvironmentId: eventSource?.httpEndpointEnvironmentId,
      },
      include: {
        queue: {
          include: {
            pipeline: true,
          },
        },
      },
    });

    await this.enqueueWorkerEvent(tx, eventLog);

    return eventLog;
  }

  private async updateEvent({
    tx,
    existingEventLog,
    reqEvent,
    queueId,
    deliverAt,
  }: UpdateEventInput) {
    if (!this.shouldUpdateEvent(existingEventLog)) {
      logger.debug(`not updating event for event id: ${existingEventLog.eventId}`);
      return existingEventLog;
    }

    const updatedEventLog = await tx.eventRecord.update({
      where: {
        eventId_environmentId: {
          eventId: existingEventLog.eventId,
          environmentId: existingEventLog.environmentId,
        },
      },
      data: {
        payload: reqEvent.payload ?? existingEventLog.payload,
        payloadType: reqEvent.payloadType,
        context: reqEvent.context ?? existingEventLog.context,
        queueId,
        deliverAt: deliverAt ?? new Date(),
      },
      include: {
        queue: {
          include: {
            pipeline: true,
          },
        },
      },
    });

    await this.enqueueWorkerEvent(tx, updatedEventLog);

    return updatedEventLog;
  }

  private shouldUpdateEvent(eventLog: EventRecord) {
    const thresholdTime = new Date(Date.now() + EVENT_UPDATE_THRESHOLD_WINDOW_IN_MSECS);

    return eventLog.deliverAt >= thresholdTime;
  }

  private async enqueueWorkerEvent(
    tx: PrismaClientOrTransaction,
    eventLog: EventRecordWithPipeline
  ) {
    const hasQueuePipeline = !!eventLog.queue?.pipeline.length;

    if (hasQueuePipeline && eventLog.queue) {
      return workerQueue.enqueue("createPipeline", {
        type: "QUEUE",
        queueId: eventLog.queue.id,
        eventRecordId: eventLog.id,
      });
    }

    if (this.deliverEvents) {
      // Produce a message to the event bus
      return workerQueue.enqueue(
        "deliverEvent",
        {
          id: eventLog.id,
        },
        { runAt: eventLog.deliverAt, tx, jobKey: `event:${eventLog.id}` }
      );
    }
  }
}

type EventRecordWithPipeline = Prisma.EventRecordGetPayload<{
  include: {
    queue: {
      include: {
        pipeline: true;
      };
    };
  };
}>;

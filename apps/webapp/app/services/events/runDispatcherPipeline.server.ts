import type {
  EventDispatcherPipelineStep,
  EventRecord,
  PipelineStepType,
} from "@trigger.dev/database";
import { EventFilterSchema } from "@trigger.dev/core";
import { $transaction, PrismaClientOrTransaction, prisma } from "~/db.server";
import { logger } from "~/services/logger.server";
import { workerQueue } from "../worker.server";
import { EventMatcher } from "./deliverEvent.server";

export class RunDispatcherPipeline {
  #prismaClient: PrismaClientOrTransaction;

  constructor(prismaClient: PrismaClientOrTransaction = prisma) {
    this.#prismaClient = prismaClient;
  }

  public async call(dispatcherId: string, eventRecordId: string) {
    await $transaction(
      this.#prismaClient,
      async (tx) => {
        const eventDispatcher = await tx.eventDispatcher.findUniqueOrThrow({
          where: {
            id: dispatcherId,
          },
          include: {
            pipeline: true,
          },
        });

        const eventRecord = await tx.eventRecord.findUniqueOrThrow({
          where: {
            id: eventRecordId,
          },
          include: {
            environment: {
              include: {
                organization: true,
                project: true,
              },
            },
          },
        });

        for (const step of eventDispatcher.pipeline) {
          const success = await this.#processStep(step, eventRecord);

          if (!success) {
            return;
          }
        }

        return workerQueue.enqueue(
          "events.invokeDispatcher",
          {
            id: eventDispatcher.id,
            eventRecordId: eventRecord.id,
          },
          { tx }
        );
      },
      { timeout: 10000 }
    );
  }

  async #processStep(step: EventDispatcherPipelineStep, eventRecord: EventRecord) {
    switch (step.type) {
      case "FILTER": {
        const payloadFilter = EventFilterSchema.safeParse(step.config ?? {});

        if (!payloadFilter.success) {
          logger.error("Invalid event filter", { payloadFilter });
          return false;
        }

        const eventMatcher = new EventMatcher(eventRecord);

        const eventMatchesFilter = eventMatcher.matches({
          payload: payloadFilter.data,
        });

        if (!eventMatchesFilter) {
          return false;
        }

        return true;
      }
      default: {
        logger.error("Unsupported pipeline step type", { step });
        return false;
      }
    }
  }
}

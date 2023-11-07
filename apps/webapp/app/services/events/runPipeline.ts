import type {
  EventDispatcherPipelineStep,
  EventRecord,
  PipelineRun,
  QueuePipelineStep,
} from "@trigger.dev/database";
import { EventFilterSchema, assertExhaustive, eventFilterMatches } from "@trigger.dev/core";
import {
  $transaction,
  PrismaClientOrTransaction,
  PrismaTransactionClient,
  prisma,
} from "~/db.server";
import { logger } from "~/services/logger.server";
import { workerQueue } from "../worker.server";
import { z } from "zod";
import { formatUnknownError } from "~/utils/formatErrors.server";

export class RunPipeline {
  #prismaClient: PrismaClientOrTransaction;

  constructor(prismaClient: PrismaClientOrTransaction = prisma) {
    this.#prismaClient = prismaClient;
  }

  public async call(id: string) {
    await $transaction(
      this.#prismaClient,
      async (tx) => {
        const run = await tx.pipelineRun.findUniqueOrThrow({
          where: {
            id,
          },
        });

        if (run.status === "FAILURE" || run.status === "SUCCESS") {
          return;
        }

        if (run.nextStepIndex === null) {
          return;
        }

        const nextPipelineStepId = run.steps[run.nextStepIndex];

        if (!nextPipelineStepId) {
          return;
        }

        let nextStep: EventDispatcherPipelineStep | QueuePipelineStep;

        switch (run.type) {
          case "DISPATCHER": {
            nextStep = await tx.eventDispatcherPipelineStep.findUniqueOrThrow({
              where: {
                id: run.steps[run.nextStepIndex],
              },
            });

            break;
          }
          case "QUEUE": {
            nextStep = await tx.queuePipelineStep.findUniqueOrThrow({
              where: {
                id: run.steps[run.nextStepIndex],
              },
            });

            break;
          }
          default: {
            assertExhaustive(run.type);
          }
        }

        try {
          await this.#processStep(tx, nextStep, run);
        } catch (error) {
          return this.#failRunWithError(run, error);
        }

        if (run.steps.length < run.nextStepIndex + 1) {
          // there are steps remaining, so enqueue the next one
          await tx.pipelineRun.update({
            where: {
              id: run.id,
            },
            data: {
              status: "STARTED", // FIXME: should set this before starting the first step instead
              nextStepIndex: run.nextStepIndex + 1,
            },
          });

          return workerQueue.enqueue(
            "runPipeline",
            {
              id,
              // TODO: could pass stepIndex here instead of using nextStepIndex field
            },
            { tx }
          );
        }

        return this.#completeRun(tx, run);
      },
      { timeout: 10000 }
    );
  }

  async #updateRunOutput(tx: PrismaTransactionClient, run: PipelineRun, output: any) {
    return tx.pipelineRun.update({
      where: {
        id: run.id,
      },
      data: {
        output,
      },
    });
  }

  async #processStep(
    tx: PrismaTransactionClient,
    step: EventDispatcherPipelineStep | QueuePipelineStep,
    run: PipelineRun
  ) {
    switch (step.type) {
      case "FILTER": {
        const matchesFilter = this.#processFilterStep(step, run);

        if (!matchesFilter) {
          throw new Error("Data does not match filter");
        }

        return;
      }
      case "WEBHOOK": {
        // TODO: implement
      }
      default: {
        throw new Error(`Unsupported pipeline step type: ${step.type}`);
      }
    }
  }

  #processFilterStep(step: EventDispatcherPipelineStep | QueuePipelineStep, run: PipelineRun) {
    const payloadFilter = EventFilterSchema.safeParse(step.config ?? {});

    if (!payloadFilter.success) {
      logger.error("Invalid event filter", { payloadFilter });
      return false;
    }

    const payloadMatches = eventFilterMatches(run.output, payloadFilter.data);

    if (!payloadMatches) {
      return false;
    }

    return true;
  }

  async #completeRun(tx: PrismaTransactionClient, run: PipelineRun) {
    const inputEvent = await tx.eventRecord.findUniqueOrThrow({
      where: {
        id: run.inputEventId,
      },
    });

    const { id: _discardId, eventId: _discardEventId, ...inputEventWithoutIds } = inputEvent;

    const outputEventData = {
      ...inputEventWithoutIds,
      eventId: `${inputEvent.eventId}:pipeline:${run.id}`,
      payload: run.output ?? {},
      context: inputEvent.context ?? {},
      sourceContext: inputEvent.sourceContext ?? {},
      timestamp: new Date(),
    };

    const succeedPipelineRun = (outputEventId: string) =>
      tx.pipelineRun.update({
        where: {
          id: run.id,
        },
        data: {
          status: "SUCCESS",
          nextStepIndex: null,
          outputEvent: {
            connect: {
              id: outputEventId,
            },
          },
        },
      });

    switch (run.type) {
      case "DISPATCHER": {
        // FIXME: this failed but nothing was rolled back, nextStepIndex was null
        const outputEvent = await tx.eventRecord.create({
          data: {
            ...outputEventData,
            // TODO: check we really need this
            shouldProcessDispatcherPipeline: false,
            shouldProcessQueuePipeline: false,
          },
        });

        const DispatcherMetadataSchema = z.object({
          dispatcherId: z.string(),
        });

        const { dispatcherId } = DispatcherMetadataSchema.parse(run.metadata);

        await succeedPipelineRun(outputEvent.id);

        return workerQueue.enqueue(
          "events.invokeDispatcher",
          {
            id: dispatcherId,
            eventRecordId: outputEvent.id,
          },
          { tx }
        );
      }
      case "QUEUE": {
        // FIXME: this failed but nothing was rolled back, nextStepIndex was null
        const outputEvent = await tx.eventRecord.create({
          data: {
            ...outputEventData,
            // TODO: check we really need this
            shouldProcessQueuePipeline: false,
          },
        });

        await succeedPipelineRun(outputEvent.id);

        return workerQueue.enqueue(
          "deliverEvent",
          {
            id: outputEvent.id,
          },
          { runAt: outputEvent.deliverAt, tx, jobKey: `event:${outputEvent.id}` }
        );
      }
      default: {
        assertExhaustive(run.type);
      }
    }
  }

  async #failRunWithError(run: PipelineRun, error: unknown) {
    logger.error("Pipeline Run failure", {
      run,
      error: error instanceof Error ? error.message : error,
    });

    return this.#prismaClient.pipelineRun.update({
      where: {
        id: run.id,
      },
      data: {
        status: "FAILURE",
        error: formatUnknownError(error),
      },
    });
  }
}

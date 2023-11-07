import { $transaction, PrismaClientOrTransaction, prisma } from "~/db.server";
import { workerQueue } from "../worker.server";
import { assertExhaustive } from "@trigger.dev/core";
import { PipelineRun, PipelineRunType } from "@trigger.dev/database";

export class CreatePipelineService {
  #prismaClient: PrismaClientOrTransaction;

  constructor(prismaClient: PrismaClientOrTransaction = prisma) {
    this.#prismaClient = prismaClient;
  }

  public async call(
    pipelineType: PipelineRunType,
    eventRecordId: string,
    assetId: string,
  ) {
    await $transaction(
      this.#prismaClient,
      async (tx) => {
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

        let pipelineRun: PipelineRun;

        switch (pipelineType) {
          case "DISPATCHER": {
            const eventDispatcher = await tx.eventDispatcher.findUniqueOrThrow({
              where: {
                id: assetId,
              },
              include: {
                pipeline: true,
              },
            });

            pipelineRun = await tx.pipelineRun.create({
              data: {
                type: pipelineType,
                projectId: eventRecord.projectId,
                inputEventId: eventRecord.id,
                metadata: {
                  dispatcherId: eventDispatcher.id,
                },
                output: eventRecord.payload ?? {},
                steps: eventDispatcher.pipeline.map((step) => step.id),
              },
            });

            break;
          }
          case "QUEUE": {
            const queue = await tx.queue.findUniqueOrThrow({
              where: {
                id: assetId,
              },
              include: {
                pipeline: true,
              },
            });

            pipelineRun = await tx.pipelineRun.create({
              data: {
                type: pipelineType,
                projectId: eventRecord.projectId,
                inputEventId: eventRecord.id,
                metadata: {
                  queueId: queue.id,
                },
                output: eventRecord.payload ?? {},
                steps: queue.pipeline.map((step) => step.id),
              },
            });

            break;
          }
          default:
            assertExhaustive(pipelineType);
        }

        return workerQueue.enqueue(
          "runPipeline",
          {
            id: pipelineRun.id,
          },
          { tx }
        );
      },
      { timeout: 10000 }
    );
  }
}

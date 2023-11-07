/*
  Warnings:

  - A unique constraint covering the columns `[pipelineOutputRunId]` on the table `EventRecord` will be added. If there are existing duplicate values, this will fail.

*/
-- CreateEnum
CREATE TYPE "PipelineStepType" AS ENUM ('FILTER', 'WEBHOOK');

-- CreateEnum
CREATE TYPE "PipelineRunStatus" AS ENUM ('PENDING', 'STARTED', 'SUCCESS', 'FAILURE');

-- CreateEnum
CREATE TYPE "PipelineRunType" AS ENUM ('DISPATCHER', 'QUEUE');

-- AlterTable
ALTER TABLE "EventRecord" ADD COLUMN     "pipelineOutputRunId" TEXT,
ADD COLUMN     "queueId" TEXT,
ADD COLUMN     "shouldProcessDispatcherPipeline" BOOLEAN NOT NULL DEFAULT true,
ADD COLUMN     "shouldProcessQueuePipeline" BOOLEAN NOT NULL DEFAULT true;

-- AlterTable
ALTER TABLE "JobRun" ADD COLUMN     "payload" JSONB;

-- CreateTable
CREATE TABLE "Queue" (
    "id" TEXT NOT NULL,
    "slug" TEXT NOT NULL,
    "title" TEXT NOT NULL,
    "icon" TEXT NOT NULL,
    "version" TEXT NOT NULL,
    "enabled" BOOLEAN NOT NULL DEFAULT true,
    "internal" BOOLEAN NOT NULL DEFAULT false,
    "organizationId" TEXT NOT NULL,
    "projectId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "Queue_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "QueuePipelineStep" (
    "id" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "type" "PipelineStepType" NOT NULL,
    "config" JSONB,
    "queueId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "QueuePipelineStep_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "EventDispatcherPipelineStep" (
    "id" TEXT NOT NULL,
    "key" TEXT NOT NULL,
    "type" "PipelineStepType" NOT NULL,
    "config" JSONB,
    "dispatcherId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "EventDispatcherPipelineStep_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "PipelineRun" (
    "id" TEXT NOT NULL,
    "type" "PipelineRunType" NOT NULL,
    "steps" TEXT[],
    "nextStepIndex" INTEGER DEFAULT 0,
    "inputEventId" TEXT NOT NULL,
    "metadata" JSONB,
    "output" JSONB,
    "status" "PipelineRunStatus" NOT NULL DEFAULT 'PENDING',
    "error" TEXT,
    "projectId" TEXT NOT NULL,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "queuedAt" TIMESTAMP(3),
    "startedAt" TIMESTAMP(3),
    "completedAt" TIMESTAMP(3),

    CONSTRAINT "PipelineRun_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "Queue_projectId_slug_key" ON "Queue"("projectId", "slug");

-- CreateIndex
CREATE UNIQUE INDEX "QueuePipelineStep_key_queueId_key" ON "QueuePipelineStep"("key", "queueId");

-- CreateIndex
CREATE UNIQUE INDEX "EventDispatcherPipelineStep_key_dispatcherId_key" ON "EventDispatcherPipelineStep"("key", "dispatcherId");

-- CreateIndex
CREATE UNIQUE INDEX "EventRecord_pipelineOutputRunId_key" ON "EventRecord"("pipelineOutputRunId");

-- AddForeignKey
ALTER TABLE "Queue" ADD CONSTRAINT "Queue_organizationId_fkey" FOREIGN KEY ("organizationId") REFERENCES "Organization"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "Queue" ADD CONSTRAINT "Queue_projectId_fkey" FOREIGN KEY ("projectId") REFERENCES "Project"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "QueuePipelineStep" ADD CONSTRAINT "QueuePipelineStep_queueId_fkey" FOREIGN KEY ("queueId") REFERENCES "Queue"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "EventDispatcherPipelineStep" ADD CONSTRAINT "EventDispatcherPipelineStep_dispatcherId_fkey" FOREIGN KEY ("dispatcherId") REFERENCES "EventDispatcher"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "EventRecord" ADD CONSTRAINT "EventRecord_queueId_fkey" FOREIGN KEY ("queueId") REFERENCES "Queue"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "EventRecord" ADD CONSTRAINT "EventRecord_pipelineOutputRunId_fkey" FOREIGN KEY ("pipelineOutputRunId") REFERENCES "PipelineRun"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PipelineRun" ADD CONSTRAINT "PipelineRun_inputEventId_fkey" FOREIGN KEY ("inputEventId") REFERENCES "EventRecord"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "PipelineRun" ADD CONSTRAINT "PipelineRun_projectId_fkey" FOREIGN KEY ("projectId") REFERENCES "Project"("id") ON DELETE CASCADE ON UPDATE CASCADE;

import { NestFactory } from '@nestjs/core';
import { LogsProcessorModule } from './crons/logs.processor.module';

async function start() {
  const transactionProcessorApp = await NestFactory.create(LogsProcessorModule);
  await transactionProcessorApp.listen(4242);
}

start().then();

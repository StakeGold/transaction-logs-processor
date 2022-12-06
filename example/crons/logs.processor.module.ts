import { Module } from '@nestjs/common';
import { ScheduleModule } from '@nestjs/schedule';

import { LogsProcessorService } from './logs.processor.service';

@Module({
  imports: [ScheduleModule.forRoot()],
  providers: [LogsProcessorService],
})
export class LogsProcessorModule {}

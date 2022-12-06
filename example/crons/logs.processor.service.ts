import { Injectable, Logger } from '@nestjs/common';
import { Cron } from '@nestjs/schedule';

import { LogsProcessor, TransactionLog } from '../../src/logs.processor';
import { Locker } from '../utils/locker';

@Injectable()
export class LogsProcessorService {
  private readonly logger: Logger;
  private lastTimestamp: number | undefined;
  private readonly logsProcessor = new LogsProcessor();
  constructor() {
    this.logger = new Logger(LogsProcessorService.name);
  }

  @Cron('*/6 * * * * *')
  async handleNewLog() {
    Locker.lock('newLogs', async () => {
      await this.logsProcessor.start({
        elasticUrl: 'http://devnet-index.elrond.com', // devnet
        maxLookBehindInSeconds: 100,
        getLastProcessedTimestamp: async () => {
          return this.lastTimestamp;
        },
        setLastProcessedTimestamp: async (timestamp: number) => {
          this.lastTimestamp = timestamp;
        },
        onMessageLogged: (topic, message) => {
          this.logger.error(message);
        },
        onLogsReceived: async (logs: TransactionLog[], startTimestamp: number, endTimestamp: number) => {
          console.log(`Received ${logs.length} logs between ${startTimestamp} and ${endTimestamp}`);
        },
      });
    });
  }
}

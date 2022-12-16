import axios from 'axios';

export class LogsProcessor {
  private options: LogsProcessorOptions = new LogsProcessorOptions();

  private isRunning: boolean = false;
  private lastProcessedTimestampInternal = this.getUnixTimestamp();

  async start(options: LogsProcessorOptions) {
    this.options = options;

    await this.startProcessingLogs(options);
  }

  async startProcessingLogs(options: LogsProcessorOptions) {
    if (this.isRunning) {
      this.logMessage(LogTopic.Debug, 'Logs processor is already running');
      return;
    }

    this.isRunning = true;

    try {
      let lastProcessedTimestamp = await this.getLastProcessedTimestampOrCurrent();
      const currentTimestamp = this.getUnixTimestamp();

      if (
        options.maxLookBehindInSeconds &&
        currentTimestamp - lastProcessedTimestamp > options.maxLookBehindInSeconds
      ) {
        lastProcessedTimestamp = currentTimestamp - options.maxLookBehindInSeconds;
      }

      const logs = await this.getLogs(options.elasticUrl, lastProcessedTimestamp, currentTimestamp);
      await this.setLastProcessedTimestamp(currentTimestamp);

      await this.onLogsReceived(logs, lastProcessedTimestamp, currentTimestamp);
    } finally {
      this.isRunning = false;
    }
  }

  private async getLogs(
    elasticUrl: string,
    lastProcessedTimestamp: number,
    currentTimestamp: number,
  ): Promise<TransactionLog[]> {
    try {
      const url = `${elasticUrl}/logs/_search`;

      const elasticQuery = {
        query: {
          bool: {
            filter: {
              range: {
                timestamp: {
                  gte: lastProcessedTimestamp,
                  lte: currentTimestamp,
                },
              },
            },
          },
        },
      };

      const response = await axios.post(url, elasticQuery, {
        headers: {
          'Content-Type': 'application/json',
        },
      });

      return response?.data?.hits?.hits
        ?.filter((hit: any) => hit._source)
        ?.map((hit: any) => hit._source) ?? [];
    } catch (err: any) {
      this.logMessage(LogTopic.Error, err);
      return [];
    }
  }

  private async onLogsReceived(transactionLogs: TransactionLog[], startTimestamp: number, endTimestamp: number) {
    const onLogsReceivedFunc = this.options.onLogsReceived;
    if (onLogsReceivedFunc) {
      await onLogsReceivedFunc(transactionLogs, startTimestamp, endTimestamp);
    }
  }

  private async getLastProcessedTimestampOrCurrent(): Promise<number> {
    let lastProcessedTimestamp = await this.getLastProcessedTimestamp();
    if (lastProcessedTimestamp === null || lastProcessedTimestamp === undefined) {
      lastProcessedTimestamp = this.getUnixTimestamp();
      await this.setLastProcessedTimestamp(lastProcessedTimestamp);
    }

    return lastProcessedTimestamp;
  }

  private async setLastProcessedTimestamp(timestamp: number) {
    const setLastProcessedTimestampFunc = this.options.setLastProcessedTimestamp;
    if (!setLastProcessedTimestampFunc) {
      this.lastProcessedTimestampInternal = timestamp;
      return;
    }

    await setLastProcessedTimestampFunc(timestamp);
  }

  private async getLastProcessedTimestamp(): Promise<number | undefined> {
    const getLastProcessedTimestampFunc = this.options.getLastProcessedTimestamp;
    if (!getLastProcessedTimestampFunc) {
      return this.lastProcessedTimestampInternal;
    }

    return await getLastProcessedTimestampFunc();
  }

  private logMessage(topic: LogTopic, message: string) {
    const onMessageLogged = this.options.onMessageLogged;
    if (onMessageLogged) {
      onMessageLogged(topic, message);
    }
  }

  private getUnixTimestamp() {
    return Math.floor(Date.now() / 1000);
  }
}

export class LogsProcessorOptions {
  elasticUrl: string = '';
  maxLookBehindInSeconds?: number;
  onLogsReceived?: (logs: TransactionLog[], startTimestamp: number, endTimestamp: number) => Promise<void>;
  getLastProcessedTimestamp?: () => Promise<number | undefined>;
  setLastProcessedTimestamp?: (timestamp: number) => Promise<void>;
  onMessageLogged?: (topic: LogTopic, message: string) => void;
}

export enum LogTopic {
  Debug = 'Debug',
  Error = 'Error',
}

export interface TransactionLog {
  address: string;
  events: Event[];
  timestamp: number;
}

export interface Event {
  address: string;
  identifier: string;
  topics: string[];
  data: string;
  order: number;
}

import {invokeMethod, MetadataInspector} from '@loopback/context';
import {
  Consumer,
  ConsumerEvents,
  EachBatchPayload,
  EachMessagePayload,
  RunnerConfig,
  MetadataMap,
  Topic,
  NormalizedTopic,
} from '../types';
import {withPrefix, normalizeTopic} from '../utils';
import {BaseFactory} from './base.factory';

export class ConsumerControllerFactory extends BaseFactory {
  async create(consumer: Consumer, runner?: RunnerConfig) {
    this.controller = await this.context.get<{[method: string]: Function}>(
      `controllers.${this.controllerClass.name}`,
    );

    await this.registerSubscribeMethods(consumer, runner);

    this.registerEventMethods(consumer, ConsumerEvents.HEARTBEAT);
    this.registerEventMethods(consumer, ConsumerEvents.COMMIT_OFFSETS);
    this.registerEventMethods(consumer, ConsumerEvents.GROUP_JOIN);
    this.registerEventMethods(consumer, ConsumerEvents.FETCH);
    this.registerEventMethods(consumer, ConsumerEvents.START_BATCH_PROCESS);
    this.registerEventMethods(consumer, ConsumerEvents.END_BATCH_PROCESS);
    this.registerEventMethods(consumer, ConsumerEvents.STOP);
    this.registerEventMethods(consumer, ConsumerEvents.CONNECT);
    this.registerEventMethods(consumer, ConsumerEvents.DISCONNECT);
    this.registerEventMethods(consumer, ConsumerEvents.REQUEST);
    this.registerEventMethods(consumer, ConsumerEvents.REQUEST_TIMEOUT);
    this.registerEventMethods(consumer, ConsumerEvents.REQUEST_QUEUE_SIZE);

    return this.controller;
  }

  private async registerSubscribeMethods(
    consumer: Consumer,
    runner: RunnerConfig = {},
  ) {
    const subscribeMethods: MetadataMap<Topic[]> =
      MetadataInspector.getAllMethodMetadata<Topic[]>(
        withPrefix(ConsumerEvents.SUBSCRIBE),
        this.controllerClass.prototype,
      ) ?? {};

    const methodNames = Object.keys(subscribeMethods);

    if (!methodNames.length) {
      return;
    }

    const schema = Object.keys(subscribeMethods).reduce(
      (accumulator, method) => {
        return {
          ...accumulator,
          [method]: subscribeMethods[method].map(normalizeTopic),
        };
      },
      {},
    );

    this.detectMethodsTopicDuplicateError(schema);

    const topics: Topic[] = Object.values(schema).flat() as unknown as Topic[];

    for await (const topic of topics) {
      await consumer.subscribe(normalizeTopic(topic));
    }

    const {asBatch = false, ...config} = runner;
    const runnerFn = asBatch ? 'eachBatch' : 'eachMessage';

    await consumer.run({
      ...config,
      [runnerFn]: async (data: EachBatchPayload | EachMessagePayload) => {
        await this.invokeMethods(
          this.findTopicSubscriptionMethods(
            schema,
            asBatch
              ? (data as EachBatchPayload).batch.topic
              : (data as EachMessagePayload).topic,
          ),
          data,
        );
      },
    });
  }

  private detectMethodsTopicDuplicateError(
    schema: Record<string, NormalizedTopic[]>,
  ) {
    for (const [method, topics] of Object.entries(schema)) {
      const duplicates = topics
        .map(({topic}) => topic)
        .filter((topic, index, original) => {
          return index !== original.indexOf(topic);
        });

      if (duplicates.length > 0) {
        throw new Error(
          `${
            this.controllerClass.name
          }:${method} method has duplicates of topics: ${duplicates.join(
            ', ',
          )}`,
        );
      }
    }
  }

  private findTopicSubscriptionMethods(
    schema: Record<string, NormalizedTopic[]>,
    topicName: string,
  ): string[] {
    return Object.keys(schema).filter(method => {
      return schema[method].some(({topic}) =>
        topic instanceof RegExp ? topic.test(topicName) : topic === topicName,
      );
    });
  }

  private async invokeMethods(
    methods: string[],
    data: EachBatchPayload | EachMessagePayload,
  ) {
    await Promise.all(
      methods.map(method =>
        invokeMethod(this.controller, method, this.context, [data]),
      ),
    );
  }
}

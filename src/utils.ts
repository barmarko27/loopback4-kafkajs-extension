import {Topic, NormalizedTopic} from './types';

export function isObject(value?: unknown): value is Object {
  return typeof value === 'object' && value?.constructor === Object;
}

export function isNormalizedTopic(topic: Topic): topic is NormalizedTopic {
  // eslint-disable-next-line @typescript-eslint/ban-ts-ignore
  // @ts-ignore
  return isObject(topic) && topic.topic;
}

export function normalizeTopic(topic: Topic): NormalizedTopic {
  return isNormalizedTopic(topic) ? topic : {topic};
}

export function withPrefix(
  name: string,
  prefix = '',
  kafkaPrefix = 'kafka',
  separator = '.',
): string {
  const prefixes = prefix ? [kafkaPrefix, prefix, name] : [kafkaPrefix, name];
  return prefixes.join(separator);
}

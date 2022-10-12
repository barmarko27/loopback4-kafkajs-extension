import {
  EachBatchPayload,
  EachMessagePayload,
  KafkaMessage,
  ProducerBatch,
  ProducerRecord,
  Serializer,
} from '../types';

export class SerializerSatellite {
  serializer: Serializer;

  constructor(serializer: Serializer) {
    this.serializer = serializer;
  }

  encode(data: unknown): Buffer {
    return this.serializer.encode(data);
  }

  decode(data: Buffer): unknown {
    return this.serializer.decode(data);
  }

  decodeMessage(message: KafkaMessage): {[key: string]: any, key: unknown, value: unknown} {
    return {
      ...message,
      key: message.key ? this.decode(message.key) : null,
      value: this.decode(message.value!),
    };
  }

  encodeProducerRecord(record: ProducerRecord): object {
    return {
      ...record,
      messages: record.messages.map(message => ({
        ...message,
        key: message.key ? this.encode(message.key) : null,
        value: this.encode(message.value),
      })),
    };
  }

  decodeMessagePayload(payload: EachMessagePayload): {[key: string]: any, message: {[key: string]: any, key: unknown, value: unknown}} {
    return {
      ...payload,
      message: this.decodeMessage(payload.message),
    };
  }

  encodeProducerBatch(batch: ProducerBatch): object {
    return {
      ...batch,
      topicMessages:
          batch?.topicMessages?.map(record =>
              this.encodeProducerRecord(record),
          ) ?? [],
    };
  }

  decodeBatchPayload(payload: EachBatchPayload): object {
    return {
      ...payload,
      batch: {
        ...payload.batch,
        messages: payload.batch.messages.map(message =>
            this.decodeMessage(message),
        ),
      },
    };
  }
}

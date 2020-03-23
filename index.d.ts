
export function configure(url: string): void;

export interface PublisherOptions {

}

export class Publisher{
    constructor(queueName: string, options?: PublisherOptions);
    publish(message: string, next?:(err:any | null) => void);
}

export class ExchangePublisher{
    constructor(exchangeName: string, exchangeType: string, options?: PublisherOptions);
    publish(routingKey: string, message: string, next?:(err:any | null) => void);
}

export interface ConsumerOptions {
    prefetch_count?: number;
}

export class Consumer{
    constructor(queueName: string, options?: ConsumerOptions);
    consume(handler:(content: string, done:() => void) => void);
}
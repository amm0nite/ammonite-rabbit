const util = require('util');
const debug = util.debuglog('rabbit');
const amqp = require('amqplib/callback_api');
const StringDecoder = require('string_decoder').StringDecoder;

var state = {
    url:            "amqp://localhost",
    index:          0,
    connection:     null,
    connecting:     false,
    channels:       {},
    defaultOptions: { durable: true }
};

function configure(url) {
    state.url = url;
}

function createName(type, name) {
    state.index++;
    return [type, name, state.index].join('_');
}

function withConnection(next) {
    if (state.connection) {
        return next(null, state.connection);
    }

    if (state.connecting) {
        return setTimeout(() => {
            withConnection(next);
        }, 0);
    }

    state.connecting = true;

    amqp.connect(state.url, (err, conn) => {
        state.connecting = false;
        if (err) return next(err);

        debug('connection');
        state.connection = conn;

        state.connection.on('close', () => {
            debug("connection closed");
            state.connection = null;
        });

        state.connection.on('error', (err) => {
            debug("connection error", err);
            state.connection = null;
        });

        return next(null, state.connection);
    });
}

function withChannel(name, next) {
    withConnection((err, conn) => {
        if (err) return next(err);

        if (state.channels[name]) {
            return next(null, state.channels[name]);
        }

        conn.createChannel((err, chan) => {
            if (err) return next(err);

            debug('channel ' + name);
            state.channels[name] = chan;

            state.channels[name].on('close', () => {
                debug("channel " + name + " closed");
                delete state.channels[name];
            });

            state.channels[name].on('error', (err) => {
                debug("channel " + name + " error", err);
                delete state.channels[name];
            });

            return next(null, state.channels[name]);
        });
    });
}

function close() {
    if (state.connection) {
        state.connection.close();
    }
}

class Publisher {
    constructor(queueName, options) {
        this.queueName = queueName;
        this.channelName = createName('direct_publisher', queueName);
        if (!options) {
            options = {};
        }
        this.options = options;
    }

    publish(message, next) {
        if (!next) {
            next = (err) => {
                if (err) return debug('publish error', err);
            };
        }
        message = message + '';

        withChannel(this.channelName, (err, chan) => {
            if (err) return next(err);

            chan.assertQueue(this.queueName, state.defaultOptions, (err) => {
                if (err) return next(err);

                chan.sendToQueue(this.queueName, new Buffer(message), {persistent: true});
                return next(null);
            });
        });
    }
}

class ExchangePublisher {
    constructor(exchangeName, exchangeType, options) {
        this.exchangeName = exchangeName;
        this.exchangeType = exchangeType;
        this.channelName  = createName('exchange_publisher', exchangeName);
        if (!options) {
            options = {};
        }
        this.options = options;
    }

    publish(routingKey, message, next) {
        if (!next) {
            next = (err) => {
                if (err) return debug('publish error', err);
            };
        }
        message = message + '';

        withChannel(this.channelName, (err, chan) => {
            if (err) return next(err);

            chan.assertExchange(this.exchangeName, this.exchangeType, state.defaultOptions, (err) => {
                if (err) return next(err);

                chan.publish(this.exchangeName, routingKey, new Buffer(message), {persistent: true});
                return next(null);
            });
        });
    }
}

class Consumer {
    constructor(queueName, options) {
        this.queueName = queueName;
        this.channelName = createName('consumer', queueName);
        if (!options) {
            options = {};
        }
        this.options = options;
    }

    consume(handler) {
        if (!handler) {
            handler = () => {};
        }

        var restarting = false;
        var that = this;

        var errorRecovery = (err) => {
            debug(err);
            if (!restarting) {
                restarting = true;
                debug("consumer restarting");
                setTimeout(() => { that.consume(handler); }, 1000);
            }
            else {
                debug("consumer already restarting");
            }
            return;
        };

        withChannel(this.channelName, (err, chan) => {
            if (err) return errorRecovery(err);

            if (this.options.prefetch_count) {
                chan.prefetch(this.options.prefetch_count);
            }

            chan.assertQueue(this.queueName, state.defaultOptions, (err) => {
                if (err) return errorRecovery(err);

                chan.consume(this.queueName, (msg) => {
                    if (!msg) {
                        return errorRecovery('empty');
                    }

                    var decoder = new StringDecoder('utf8');
                    var content = decoder.end(msg.content);

                    var done = () => { chan.ack(msg); };
                    handler(content, done);
                });
            });

            chan.on('close', () => {
                return errorRecovery('channel_closed');
            });
        });
    }
}

module.exports = { configure, Publisher, ExchangePublisher, Consumer };

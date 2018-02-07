var logger = require('winston');
var amqp = require('amqplib/callback_api');
var StringDecoder = require('string_decoder').StringDecoder;

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

function createName(type, queueName) {
    state.index++;
    return [type, queueName, state.index].join('_');
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
        
        logger.info('connection');
        state.connection = conn;
        
        state.connection.on('close', () => {
            logger.warn("connection closed");
            state.connection = null;
        });
        
        state.connection.on('error', (err) => {
            logger.error("connection error", err);
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
            
            logger.info('channel ' + name);
            state.channels[name] = chan;
            
            state.channels[name].on('close', () => {
                logger.warn("channel " + name + " closed");
                delete state.channels[name];
            });

            state.channels[name].on('error', (err) => {
                logger.error("channel " + name + " error", err);
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
        this.channelName = createName('publisher', queueName);
        if (!options) {
            options = {};
        }
        this.options = options;
    }

    publish(message, next) {
        if (!next) {
            next = () => {};
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

class Consumer {
    constructor(queueName, options) {
        this.queueName = queueName;
        this.channelName = createName('consumer', queueName);
        if (!options) {
            options = {};
        }
        this.options = options;
    }

    consume(process) {
        if (!process) {
            process = () => {};
        }

        var restarting = false;
        var that = this;

        var errorRecovery = (err) => {
            logger.error(err);
            if (!restarting) {
                restarting = true;
                logger.info("consumer restarting");
                setTimeout(() => { that.consume(process); }, 1000);
            }
            else {
                logger.debug("consumer already restarting");
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
                    process(content, done);
                });
            });

            chan.on('close', () => {
                return errorRecovery('channel_closed'); 
            });
        });
    }
}

module.exports = { configure, Publisher, Consumer };
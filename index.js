var logger = require('winston');
var amqp = require('amqplib/callback_api');
var StringDecoder = require('string_decoder').StringDecoder;

var state = {
    url : "amqp://localhost",
    index : 0,
    connection : null,
    channels : {}
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
    
    amqp.connect(state.url, function(err, conn) {
        if (err) { return next(err); }
        
        logger.info('connection');
        state.connection = conn;
        
        state.connection.on('close', function () {
            logger.warn("connection closed");
            state.connection = null;
        });
        
        state.connection.on('error', function (err) {
            logger.error("connection error", err);
            state.connection = null;
        });
        
        return next(null, state.connection);
    });
}

function withChannel(name, next) {
    withConnection(function (err, conn) {
        if (err) { return next(err); }
        
        if (state.channels[name]) {
            return next(null, state.channels[name]);
        }
        
        conn.createChannel(function(err, chan) {
            if (err) { return next(err); }
            
            logger.info('channel ' + name);
            state.channels[name] = chan;
            
            state.channels[name].on('close', function () {
                logger.warn("channel " + name + " closed");
                delete state.channels[name];
            });

            state.channels[name].on('error', function (err) {
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
        if (!next) { next = function() {}; }
        message = message + '';

        var queueName = this.queueName;

        withChannel(this.channelName, function (err, chan) {
            if (err) { return next(err); }
            
            chan.assertQueue(queueName, {durable: true});
            chan.sendToQueue(queueName, new Buffer(message), {persistent: true});
            return next(null);
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
        if (!process) { process = function() {}; }

        var queueName = this.queueName;
        var options = this.options;
        var restarting = false;
        var that = this;

        var errorRecovery = function(err) {
            logger.error("consumer failed: " + err);
            if (!restarting) {
                restarting = true;
                logger.info("consumer restarting");
                setTimeout(function() { that.consume(process); }, 1000);
            }
            else {
                logger.debug("consumer already restarting");
            }
            return;
        };

        withChannel(this.channelName, function (err, chan) {
            if (err) { return errorRecovery(err); }
            
            if (options.prefetch_count) {
                chan.prefetch(options.prefetch_count);
            }
            chan.checkQueue(queueName);
            
            chan.consume(queueName, function (msg) {
                if (!msg) {
                    return errorRecovery('empty');
                }

                var decoder = new StringDecoder('utf8');
                var content = decoder.end(msg.content);

                var done = function() { chan.ack(msg); };
                process(content, done);
            });

            chan.on('close', function() {
                return errorRecovery('channel_closed'); 
            });
        });
    }
}

exports.configure = configure;
exports.Publisher = Publisher;
exports.Consumer = Consumer;
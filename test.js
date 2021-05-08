
const rabbit = require('./index.js');

rabbit.configure('amqp://username:password@127.0.0.1');

const queueName = 'target';

const publisher = new rabbit.Publisher(queueName);
const consumer = new rabbit.Consumer(queueName);

setInterval(() => {
    publisher.publish(JSON.stringify({ message: "hello world" }));
}, 1000);

consumer.consume((content, done) => {
    console.log(content);
    done();
});

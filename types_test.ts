
import { configure, Publisher, Consumer } from './index.js';

configure('amqp://localhost');

var pub = new Publisher('test');
pub.publish('hello');
pub.publish('welcome', function(err) {
    console.log(err);
});

var cons = new Consumer('test', { prefetch_count: 1 });
cons.consume(function(content: string, done) {
    done();
});

import * as rabbit from './index.js';

rabbit.configure('amqp://127.0.0.1');
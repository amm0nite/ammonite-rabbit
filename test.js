var helper = require('./index.js');

helper.configure('amqp://main:ammonite@10.0.0.1');

var pub = new helper.Publisher('unicorn-task');
setInterval(function() {
    pub.publish((new Date()).toString());
}, 1000);

var consumer = new helper.Consumer('unicorn-task');
consumer.consume(function(msg, done) {
    console.log(msg);
    done();
});
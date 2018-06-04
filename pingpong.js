const { Client, ConsumerGroupStream, KeyedMessage, ProducerStream } = require('kafka-node');
const { Transform } = require('stream');
const _ = require('lodash');
const chalk = require('chalk') ;

const client = new Client('zoo1', 'oktavheft');

const producer = new ProducerStream(client);

const stdinTransform = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (text, encoding, callback) {
    let num = parseInt(text);
    console.log(`Pushing message ${text} to ExampleTopic`);
    let message = { num: num, method: 'two' }
    callback(null, {
      topic: 'Pingpong',
      messages: JSON.stringify(message)
    });
  }
});

process.stdin.setEncoding('utf8');
process.stdin.pipe(stdinTransform).pipe(producer);

const resultProducer = new ProducerStream(client);
const resultProducer2 = new ProducerStream(client);

const consumerOptions = {
  kafkaHost: 'kafka1:9092',
  groupId: 'OktavheftPingpongGroup',
  sessionTimeout: 25000,
  protocol: ['roundrobin'],
  asyncPush: false,
  id: 'cons',
  fromOffset: 'latest'
};

const consumerGroup = new ConsumerGroupStream(consumerOptions, 'Pingpong');

const messageTransform = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (message, encoding, callback) {
    let mesObj = JSON.parse(message.value);
    let num = parseInt(mesObj.num)+1;
    if( mesObj.method==='one' ) {
      let responseObj =  { method: 'two', num: num };
        setTimeout(() =>  {
          console.log(chalk.greenBright('   #1 \t Received message ' + JSON.stringify(mesObj) + '\n transforming input'));
          consumerGroup.commit(null, true);
          callback(null, {
            topic: 'Pingpong',
            messages: JSON.stringify(responseObj)
          })
        }, 3000);
    } else {
      callback();
    }
  }
});

const messageTransform2 = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (message, encoding, callback) {
    let mesObj  = JSON.parse(message.value);
    let num = parseInt(mesObj.num)+1;
    if( mesObj.method==='two' ) {
      let responseObj =  { method: 'one', num: num };
        setTimeout(() =>  {
          console.log( chalk.redBright('   #2 \t Received message ' + JSON.stringify(mesObj) + '\n transforming input'));
          consumerGroup.commit(null, true);
          callback(null, {
            topic: 'Pingpong',
            messages: JSON.stringify(responseObj)
          })
        }, 4000);
    } else {
      callback();
    }
  }
});

consumerGroup
.pipe(messageTransform)
.pipe(resultProducer);

consumerGroup
.pipe(messageTransform2)
.pipe(resultProducer2);
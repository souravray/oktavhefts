const { Client, ConsumerGroupStream, KeyedMessage, ProducerStream } = require('kafka-node'),
  { Duplex, Writable, Transform} = require('stream'),
  _ = require('lodash'),
  blessed = require('blessed'),
  contrib = require('blessed-contrib'),
  screen = blessed.screen(),
  upstreamTopic = 'back.pressure.up.parts',
  downstreamTopic = 'back.pressure.down.4';

Array.prototype.incrFill = function() {
  let arr = this;
  for(let i=0; i<arr.length; i++) {
   arr[i]=i;
  }
  return arr;
}

var _pressure = { one: { 
                      upstream: 0,
                      midstream: 0,
                      downstream: 0,
                    },
                    two: { 
                      upstream: 0,
                      midstream: 0,
                      downstream: 0,
                    }
                  };

/*        PRODUCER      */
/* ==================== */
const tap = new Duplex({
      objectMode: true,
      readableObjectMode: true,
      write(data, encoding, callback) {
       this.push(data);
       callback();
      },
      read(size) {
      }
    });

const client = new Client('zoo1:2181,zool2:2182,zool3:2183', 'oktavheft');
const producer = new ProducerStream({
  kafkaClient: client,
  producer: {
     partitionerType: 2,
  },
});

client.once('connect', function () {
  client.loadMetadataForTopics([], function (error, results) {
    if (error) {
      return console.error(error);
    }
    console.log('%j', _.get(results, '1.metadata'));
  });
});

const producerTransform = new Transform({
  objectMode: true,
  transform (data, encoding, callback) {
    callback(null, {
      topic: upstreamTopic,
      partition: (data.method==='one') ? 0:1,
      messages: JSON.stringify(data)
    });
  }
});

tap.pipe(producerTransform).pipe(producer);

var  producerCurry = function (method) {
  let producer =  (writeStream, duration, i=0) => {
    writeStream.write({value : i, method: method});
    // incriment value counter
    i++;
    // incriment upstream count for the method
    _pressure[method]['upstream']++;
    setTimeout(producer, duration, writeStream, duration, i);
  }
  return producer;
}


client.refreshMetadata([upstreamTopic], (err, result) => {
  if (err) {
      console.warn('Error refreshing kafka metadata', err);
  }

  console.log(JSON.stringify(result, null, 2));
});

const producerOne = producerCurry('one');
producerOne(tap, 1000);
const producerTwo = producerCurry('two');
producerTwo(tap, 1000);



/*      CONSUMER G. - config     */
/* ==============================*/

const consumerOptions = {
  kafkaHost: 'kafka1:9092',
  groupId: 'OktavheftBackpressurePartGroup',
  sessionTimeout: 25000,
  protocol: ['roundrobin'],
  asyncPush: false,
  fromOffset: 'latest'
};


const consOptions1 = Object.assign({},consumerOptions, {partition: 0, id: 'cns1'});
const consOptions2 = Object.assign({},consumerOptions, {partition: 1, id: 'cns2'});


/*      CONSUMER - MID     */
/* ========================*/
const midstreamConsumerOne = new ConsumerGroupStream(consOptions1, upstreamTopic);
const midstreamConsumerTwo = new ConsumerGroupStream(consOptions2, upstreamTopic);

const workerOne = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (message, encoding, callback) {
    let mesObj  = JSON.parse(message.value);
    if( mesObj.method==='one' ) {
      setTimeout(() =>  {
        _pressure[mesObj.method]['midstream']++;
        midstreamConsumerOne.commit(message, true);
        callback(null, {
          topic: downstreamTopic,
          messages: JSON.stringify(mesObj)
        })
      }, 4000);
    } else {
      callback();
    }
  }
});

const workerTwo = new Transform({
  objectMode: true,
  decodeStrings: true,
  transform (message, encoding, callback) {
    let mesObj  = JSON.parse(message.value);
    if( mesObj.method==='two' ) {
      setTimeout(() =>  {
        _pressure[mesObj.method]['midstream']++;
        midstreamConsumerTwo.commit(message, true);
        callback(null, {
          topic: downstreamTopic,
          messages: JSON.stringify(mesObj)
        })
      }, 1000);
    } else {
      callback();
    }
  }
});

const resultProducerOne = new ProducerStream(client);
const resultProducerTwo = new ProducerStream(client);

midstreamConsumerOne
.pipe(workerOne)
.pipe(resultProducerOne);

midstreamConsumerTwo
.pipe(workerTwo)
.pipe(resultProducerTwo);



/*     CONSUMER - DOWN     */
/* ========================*/
const downConsumer = new ConsumerGroupStream(consumerOptions, downstreamTopic);

const sink = new Writable({
  objectMode: true,
  write(message, encoding, callback) {
    let mesObj  = JSON.parse(message.value);
    downConsumer.commit(message, true, function(err, data) {
      if(err) {
        return process.nextTick(callback);
      }
      _pressure[mesObj.method]['downstream']++;
      process.nextTick(callback);
    });
  } 
});

downConsumer
.pipe(sink);



/*  PLOT  */
/*  ==== */

var lineGraph = contrib.line(
   { width: 100
   , height: 60
   , left: 8
   , top: 0  
   , xPadding: 5
   , label: 'Stream Pressures'
   , showLegend: true
   , legend: {width: 24, height: 32}});

var backpressureData = [
    {
      title: 'Upstream One',
      style: { line: 'red' },
      x: new Array(100).incrFill() ,
      y: new Array(100).fill(0.0) ,
    },
    {
      title: 'Midstream One',
      style: { line: 'magenta' },
      x: new Array(100).incrFill() ,
      y: new Array(100).fill(0.0) ,
    },
    { 
      title: 'Upstream Two',
      style: { line: 'green' },
      x: new Array(100).incrFill() ,
      y: new Array(100).fill(0.0) ,
    },{
      title: 'Midstream Two',
      style: { line: 'cyan' },
      x: new Array(100).incrFill() ,
      y: new Array(100).fill(0.0) ,
    }];

screen.append(lineGraph) 
lineGraph.setData(backpressureData);

const updateData = function(method, upstreamPs,midstreamPs ) {
  if(method === 'one') {
    backpressureData[0].y.shift();
    backpressureData[0].y.push(upstreamPs);
    backpressureData[1].y.shift();
    backpressureData[1].y.push(midstreamPs);
  } else {
    backpressureData[2].y.shift();
    backpressureData[2].y.push(upstreamPs);
    backpressureData[3].y.shift();
    backpressureData[3].y.push(midstreamPs);
  }
  lineGraph.setData(backpressureData);
  screen.render();
}

const samplerCurry = function (method) {
  let smapler = function(sampleDuration) {
    let upstreamPessure = _pressure[method]['upstream'] - _pressure[method]['midstream'],
      midstreamPessure = _pressure[method]['midstream'] - _pressure[method]['downstream'];
    updateData(method,upstreamPessure,midstreamPessure);
    setTimeout(smapler, sampleDuration, sampleDuration);
  }
  return smapler;
}

const samplerOne = samplerCurry('one');
samplerOne(3000);
const samplerTwo = samplerCurry('two');
samplerTwo(3000);


screen.key(['escape', 'q', 'C-c'], function(ch, key) {
  return process.exit(0);
});

screen.render();






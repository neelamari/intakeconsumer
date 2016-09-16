var express = require('express');
var app = express();

var Cfenv = require('cfenv');
var MessageHub = require('message-hub-rest');
var appEnv = Cfenv.getAppEnv();
var instance;
var cleanedUp = false;
var topic = 'projects';
var consumerInstance;
var consumerGroupName = 'node-kafka-consumers';
var consumerInstanceName = 'node-kafka-consumer-' + '1';
var consumeInterval;

app.listen(appEnv.port, '0.0.0.0', function() {
  console.log("server starting on " + appEnv.url);
});

app.get('/', function (req, res) {
  res.send('Hello World!');
});

var start = function(restEndpoint, apiKey, callback) {
  if(!appEnv.services || (appEnv.services && Object.keys(appEnv.services).length === 0)) {
    if(restEndpoint && apiKey) {
      appEnv.services = {
        "messagehub": [
           {
              "label": "intakemessagehub",
              "credentials": {
                 "api_key": apiKey,
                 "kafka_rest_url": restEndpoint,
              }
           }
        ]
      };
    } else {
      console.error('A REST Endpoint and API Key must be provided.');
      process.exit(1);
    }
  } else {
    consumerInstanceName = 'node-kafka-consumer-' + appEnv.app.instance_id;
    console.log('Endpoint and API Key provided have been ignored, as there is a valid VCAP_SERVICES.');
  }

  console.log('Consumer Group Name: ' + consumerGroupName);
  console.log('Consumer Group Instance: ' + consumerInstanceName);

  instance = new MessageHub(appEnv.services);

  consumeInterval = setInterval(function() {
    if(consumerInstance) {
      consumerInstance.get(topic)
        .then(function(data) {
          if(data.length > 0) {
            for(var index in data) {
              data[index] = JSON.parse(data[index]);
              console.log('Consumer Group Instance: ' + consumerInstanceName + ', Message: ' + data[index].message);
            }

          }
        })
        .fail(function(error) {
          throw new Error(error);
        });
    }
  }, 250);

  instance.topics.create(topic)
      .then(function(response) {
        console.log('topic created');
        return instance.consume(consumerGroupName, consumerInstanceName, { 'auto.offset.reset': 'largest' });
      })
      .then(function(response) {
        consumerInstance = response[0];
        console.log('Consumer Instance created.');
        if(callback) {
          callback();
        }
      })
      .fail(function(error) {
        console.log(error);
        stop(1);
      });
};

var registerExitHandler = function(callback) {
  if(callback) {
    var events = ['exit', 'SIGINT', 'uncaughtException'];

    for(var index in events) {
      process.on(events[index], callback);
    }
  } else if(!callback) {
    throw new ReferenceException('Provided callback parameter is undefined.');
  }
};

// Register a callback function to run when
// the process exits.
registerExitHandler(function() {
  stop();
});

var stop = function(exitCode) {
  exitCode = exitCode || 0;

     if(consumerInstance) {
      console.log('Removing consumer instance: ' + consumerGroupName + '/' + consumerInstanceName);
      consumerInstance.remove()
        .fin(function(response) {
          try {
            console.log(JSON.stringify(response));
          } catch(e) {
            console.log(response);
          }

          cleanedUp = true;
          process.exit(exitCode);
        });
    } else {
      cleanedUp = true;
      process.exit(exitCode);
    }
};

// If this module has been loaded by another module, don't start
// the service automatically. If it's being started from the command license
// (i.e. node app.js), start the service automatically.
if(!module.parent) {
  if(process.argv.length >= 4) {
    start(process.argv[process.argv.length - 2], process.argv[process.argv.length - 1]);
  } else {
    start();
  }
}

module.exports = {
  start: start,
  stop: stop,
  appEnv: appEnv
}

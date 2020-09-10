// Abusix Raw Threat Intellience
// Exmaple Node.js script

// npm install stompit
const stompit = require('stompit');
const util = require('util');

// Put your credentials here
const config = {
    "user": "<YOUR USERNAME>",
    "pass": "<YOUR PASSWORD>",
    "topic": "<TOPIC>",
    // If you want a shared subscription make this value
    // the same for each process that should share the data.
    // "channel": ${config.user} + "-" + "myprocess",
}

var start_stomp_client = function () {
    stompit.connect({
        "host": "streams.abusix.net",
        "port": 61612,
        "ssl": true,
        "maxLineLength": 1048576,
        "connectHeaders": {
            "login": config.user,
            "passcode": config.pass,
            "heart-beat": "30000,30000",
        }
    }, (err, client) => {
        var self = this;

        function retry() {
            if (client) client.disconnect();
            console.log('STOMP: attempting to reconnect');
            setTimeout(() => {
                self.start_stomp_client(config);
            }, 5 * 1000);
        }

        if (err) {
            console.log(`STOMP: connect error (${err.message})`);
            return retry();
        }

        client.on('error', (cerr) => {
            console.error(`STOMP: client error (${cerr.message})`);
            return retry();
        });

        console.log(`STOMP: connected`);

        var topics = config.topic;
        if (!Array.isArray(topics)) {
             topics = topics.replace(/\s+/g,'').split(/[;,]/);
        }
        topics.forEach((topic) => {
            console.log(`STOMP: subscribing to topic: ${topic} (channel: ${config.channel})`);
            client.subscribe({
                "channel": config.channel || null,
                "destination": `/topic/${topic}`,
                "ack": "auto"
            }, (serr, message) => {
                if (serr) {
                    console.error(`STOMP: ${topic} subscribe error (${serr.message})`);
                    return;
                }
                message.readString('utf-8', (merr, body) => {
                    if (merr) {
                        console.error(`STOMP: ${topic} message read error (${merr.message})`);
                        return;
                    }
                    
                    // Your code to handle the input messages
                    console.log(util.inspect(body));
                });
            });
        });
    });
}

start_stomp_client();

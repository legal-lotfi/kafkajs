const { kafka } = require('./index')
const Error = require("http-errors");

class Producer {

    constructor() {
        this.producer = kafka.producer();
    }

    connect(config = {}) {
        //initialize connection to zookeeper/kafka
        this.producer.connect(config)
        .then(() => {
            console.log("action=kafka_producer_start status=READY")
        })
        .catch(err => {
            console.log("action=kafka_producer_start status=ERROR error=" + err)
        })
    }

    /*
        topicMessages object should respect the following schema:
        topicMessages = [
            { 
                topic: "i.e: notification", // The topic name to send the messages to
                messages: [
                    message // An array of messages to send to that topic, message must be: { value: "the message object" }
                ],
            },
        ]
    */
    sendMessage(topicMessages) {
        this.producer.sendBatch({ topicMessages })
        .then(data => {
            console.log('action=notification_sent status=OK response=' + JSON.stringify(data));
        })
        .catch(err => {
            throw new Error(err)
        })
    }
}

module.exports = new Producer()
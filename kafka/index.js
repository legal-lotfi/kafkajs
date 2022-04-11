const { Kafka } = require('kafkajs')

// Set Kafka connection
const kafka = new Kafka({
    clientId: 'monster-backend',
    brokers: ['localhost:9092']
})


module.exports = {
    kafka
};
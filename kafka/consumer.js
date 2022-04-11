const { kafka } = require("./index");
const admin = require("./admin.js");

/**
 * 
 * @param {*} consumer 
 * @returns {Array} subscriptionTopics
 */
async function consumerSubscriptions(consumer) {
  let subscriptionTopics = [];
  // Subscribe to a topic
  await consumer.subscribe({
    topic: "text",
    fromBeginning: false,
  });
  subscriptionTopics.push("text");

  return subscriptionTopics;
}

// Create a consumer and attach it to the socket
async function createConsumer(socket, groupId) {
  // consumer 1 : "5ac00000"

  const consumer = kafka.consumer({ groupId: groupId });

  await consumer.connect();

  // The consumer subscribe to multiple topics
  let subscriptionTopics = await consumerSubscriptions(consumer);

  consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = partition + message.value.toString();

      console.log("EMMITING TO CLIENT SOCKET : ", topic, event);
      socket.emit(topic, event);
    },
  });

  // Seek to last offset for all subscribed topics/partitions
  resetTopicOffsets(consumer, groupId, subscriptionTopics, -1);
  
  return consumer;
}

async function resetTopicOffsets(consumer, groupId, topics, offset){
  topics.forEach(async (topic) => {
    let patitionsOffset = await admin.admin.fetchOffsets({ groupId: groupId, topic });
    for (const i in patitionsOffset) {
        let { partition } = patitionsOffset[i];
        console.log({ topic, partition, offset: offset })
        consumer.seek({ topic, partition, offset: offset });
      }
  });
}

// Main function
const consumerIO = (server) => {
  const io = server.of("mq");

  // Init socket IO connection
  io.on("connection", async (socket) => {
    // Init consumer client
    const consumer = await createConsumer(
      socket,
      socket.handshake.query.groupId
    );
    socket.on("disconnect", async () => {
      await consumer.disconnect();
    });
  });
};

module.exports = consumerIO;
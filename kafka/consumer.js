const { kafka } = require("./index");
const admin = require("./admin.js");

async function consumerSubscriptions(consumer) {
  // Subscribe to a topic
  await consumer.subscribe({
    topic: "text",
    fromBeginning: false,
  });

}

// Create a consumer and attach it to the socket
async function createConsumer(socket, groupId) {
  // consumer 1 : "5ac00000"

  const consumer = kafka.consumer({ groupId: groupId });

  try {
    await admin.admin.resetOffsets({ groupId, topic: "text" });
  } catch (err) {
    console.log(err);
  }

  await consumer.connect();

  // The consumer subscribe to multiple topics
  await consumerSubscriptions(consumer);

  let fetchedOffsets = await admin.admin.fetchTopicOffsets("text");
  let { partition, offset } = fetchedOffsets[0];
  console.log(partition, offset);
  let off = await admin.admin.fetchOffsets({ groupId, topic: "text" });
  console.log(off);
  // consumer.seek({ topic: 'text', partition, offset })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = partition + message.value.toString();

      console.log("EMMITING TO CLIENT SOCKET : ", topic, event);
      socket.emit(topic, event);
    },
  });

  return consumer;
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

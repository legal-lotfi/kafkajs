const express = require("express");
const { Server } = require("socket.io");

const producer = require("./kafka/producer"); // Create Kafka producer
producer.connect();

const app = new express();

// Set basic express settings
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

app.get("/", (req, res) => {
  res.send("/Users/amina/Downloads/Computer-Networks-5th-Edition.pdf");
});

app.get("/producer", (req, res) => {
  let topicMessages = [
    {
      topic: req.query.topic, // The topic name to send the messages to
      messages: [
        { value: req.query.msg }, // An array of messages to send to that topic, message must be: { value: "the message object" }
      ],
    },
  ];

  producer.sendMessage(topicMessages);

  res.send("sending producer message ....");
});

app.get("/socket", (req, res) => {
  res.sendFile(__dirname + "/index.html");
});

// Start the server
const port = Number(process.env.PORT || 3000);
const httpServer = app.listen(port, () => {
  console.log("Express server started on port: http://localhost:" + port);
});

// Setting up socket server
const ioServer = new Server(httpServer);

// Attach a consumer to each socket connection
require("./kafka/consumer")(ioServer);

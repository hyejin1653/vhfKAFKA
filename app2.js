const express = require("express");
const app = express();
const iconv = require("iconv-lite");

const kafka = require("kafka-node");
//const { kafka } = require("kafkajs");
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({
  kafkaHost: "155.155.4.228:9092",
});

client.on("ready", () => {
  console.log("Kafka Connected");
});
//console.log("client", client);
const admin = new kafka.Admin(client);

let topicL = [];
//console.log("admin", admin);
admin.listTopics((err, res) => {
  let data = res[1].metadata;
  //console.log(data);
  for (let topic in data) {
    if (topic.includes("VHF_Packet_5")) {
      //topicL.push(data[topic][0]);
      topicL.push({ topic: topic });
    }
  }
});
// admin.listGroups((err, res) => {
//   //console.log("consumerGroups", res);
//   for (let consu in res) {
//     //console.log(consu);
//     admin.describeGroups([consu], (err, res) => {
//       console.log(JSON.stringify(res, null, 1));
//     });
//   }
// });

app.get("/topicList", (req, res) => {
  console.log(topicL);
  let consumer = new Consumer(client, topicL, {
    autoCommit: false,
  });
  consumer.on("message", (message) => {
    //var data = iconv.decode(message.value, "utf-8").toString();
    console.log("Message : " + JSON.parse(message.value));

    // if (message.value.length > 28) {
    //   var msg = JSON.parse(message.value);
    //   console.log(msg);
    // }
  });

  //res.send("data");
});

app.listen(5000, () => {
  console.log("http://localhost:5000 Server start!");
});

const SSE = require("sse");
const express = require("express");
const app = express();

const kafka = require("kafka-node");
const client = new kafka.KafkaClient({
  kafkaHost: "155.155.4.227:9092",
});

const admin = new kafka.Admin(client);

admin.listGroups((err, res) => {
  //console.log("consumerGroups", res);
  for (let consu in res) {
    //console.log(consu);
    admin.describeGroups([consu], (err, res) => {
      //   console.log(
      //     consu,
      //     res[consu].groupId,
      //     res[consu].state,
      //     res[consu].members
      //   );
      let obj = {};
      obj["topicNm"] = consu;
      obj["groupId"] = res[consu].groupId;
      obj["state"] = res[consu].state;
      //console.log(obj);

      if (res[consu].members.length > 0) {
        console.log(res[consu]);
      }
    });
  }
});

const server = app.listen(7777, () => {
  console.log("7777 Port Open!");
});

const sse = new SSE(server);

sse.on("connection", (client) => {
  setInterval(() => {
    //console.log("dddd");
    //client.send(JSON.stringify(res));
  }, 1000);
});

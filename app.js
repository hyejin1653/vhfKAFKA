const express = require("express");
const app = express();
const iconv = require("iconv-lite");

//const kafka = require("kafka-node");
const SnappyJS = require("snappy");
const protobuf = require("protobufjs");
const { Kafka } = require("kafkajs");
var WebSocket = require("ws");
//const Consumer = kafka.Consumer;

const kafka = new Kafka({
  clientId: "vhf-kafka",
  brokers: ["155.155.4.227:9092"],
});

const consumer = kafka.consumer({
  groupId: "kafkaStatus",
});

// let run = async () => {
//   //admin.listTopics();
//   await admin.connect();
//   const existingTopics = await admin.listTopics();
//   console.log(existingTopics);
// };

/*let topicL = [];
admin.connect().then(async () => {
  const existingTopics = await admin.listTopics();

  for (let topic of existingTopics) {
    //console.log(topic);
    // || topic.includes("vhf")
    if (topic.includes("VHF_Packet_1")) {
      //if (!topic.includes("DCS"))
      topicL.push({ center: "인천", topic: topic });
    } else if (topic.includes("VHF_Packet_4")) {
      topicL.push({ center: "울산", topic: topic });
    } else if (topic.includes("VHF_Packet_5")) {
      topicL.push({ center: "부산", topic: topic });
    }
  }
});
*/

let topicL = [];
// //console.log("admin", admin);
// admin.listTopics((err, res) => {
//   let data = res[1].metadata;
//   console.log(data);
//   for (let topic in data) {
//     if (topic.includes("VHF") || topic.includes("vhf")) {
//       if (!topic.includes("DCS")) topicL.push(data[topic][0]);
//     }
//   }
// });

app.get("/topicList", async (req, res) => {
  //console.log(topicL);
  //consumer.disconnect();
  //consumer.stop();

  topicL.push({ center: "부산", topic: "tp_VHF_Packet_511" });

  res.send(topicL);
  //   let consumer = new Consumer(client, topicL, {
  //     autoCommit: true,
  //     fetchMaxWaitMs: 1000,
  //     fetchMaxBytes: 1024 * 1024,
  //     fromOffset: true,
  //   });
  //   consumer.on("message", (message) => {
  //     //var data = iconv.decode(message.value, "utf-8").toString();
  //     console.log(message.value.length);

  //   });
});

async function decodeTestMessage(buffer) {
  const root = await protobuf.load("./proto/vhf.proto");
  const testMessage = root.lookupType("vhf.VHF");
  const err = testMessage.verify(buffer);
  if (err) {
    throw err;
  }
  const message = testMessage.decode(buffer);
  return testMessage.toObject(message);
}

function dateConvert(dt) {
  let tDate = new Date(Number(dt));
  //console.log(dt, tDate);
  let date = ("0" + tDate.getDate()).slice(-2);
  let month = ("0" + (tDate.getMonth() + 1)).slice(-2);
  let year = tDate.getFullYear();
  let hours =
    tDate.getHours().toString().length == 1
      ? "0" + tDate.getHours().toString()
      : tDate.getHours();
  let minutes =
    tDate.getMinutes().toString().length == 1
      ? "0" + tDate.getMinutes().toString()
      : tDate.getMinutes();
  let seconds =
    tDate.getSeconds().toString().length == 1
      ? "0" + tDate.getSeconds().toString()
      : tDate.getSeconds();

  return `${year}-${month}-${date} ${hours}:${minutes}:${seconds}`;
}

const initKafka = async (topicNm) => {
  console.log("start subscribe");
  await consumer.connect();
  await consumer.subscribe({
    topic: topicNm,
    fromBeginning: false,
    fromOffset: "latest",
  });
  //wss.on("connection", async (ws, request) => {
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      //if (err) throw err;
      //console.log(message.timestamp, message.offset, partition);
      let convertDt = dateConvert(message.timestamp);
      let obj = await decodeTestMessage(message.value);
      console.log(obj);

      let payload;
      //voice
      if (obj.datatype == 1) {
        payload = obj.voice.buffer;
      } else {
        payload = obj.ctrl.payload;
        payload = Buffer.from(payload, "utf8");
      }

      if (ws.readyState === ws.OPEN) {
        // let newObj = {
        //   value: payload,
        //   partition: partition,
        //   offset: message.offset,
        //   timestamp: convertDt,
        // };
        //console.log(JSON.stringify(newObj));
        //let result = JSON.stringify(newObj);

        //let buffer = new Uint8Array(payload).buffer;

        let parti = Buffer.from(partition.toString(), "utf8");
        let offset = Buffer.from(message.offset.toString(), "utf8");
        let date = Buffer.from(convertDt, "utf8");

        //console.log(parti.length, offset.length, date.length);
        //[partition : 1, offset : 8, date : 19, compressed]
        let compressed = SnappyJS.compressSync(payload);
        let resultBuffer = Buffer.concat([parti, offset, date, compressed]);
        //console.log(resultBuffer);
        ws.send(resultBuffer);
        //sleep(1000);
      }
    },
    //});
  });
};

const ws = new WebSocket("ws://155.155.4.228:7511");
ws.onopen = () => {
  console.log("ws opened on browser");
};

app.get("/topicStart/:topic", (req, res) => {
  //console.log(req.params.topic);
  let topicNm = req.params.topic;

  //console.log(consumer);
  initKafka(topicNm);
  res.send(topicNm);
});

app.listen(5000, () => {
  console.log("http://localhost:5000 Server start!");
  //run();
});

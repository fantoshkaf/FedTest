import http from "k6/http";
import {
    Writer,
    SchemaRegistry,
    SCHEMA_TYPE_STRING ,
} from "k6/x/kafka";
const brokers = ["localhost:29092"];

const topic = "computer";

const writer = new Writer({
  brokers: brokers,
  topic: topic,
  autoCreateTopic: true,
});


const schemaRegistry = new SchemaRegistry();




export const options = {
  scenarios: {
    two_steps_load: {
      executor: 'ramping-arrival-rate',
      timeUnit: '1s',
      preAllocatedVUs: 4,
      maxVUs: 10,
      stages: [
        { duration: '5s', target: 2 }, // Первая ступень: 2 rps в течение 2 минут
        { duration: '2m', target: 2 }, 

        { duration: '5s', target: 4 }, // Вторая ступень: 4 rps в течение 2 минут
        { duration: '2m', target: 4 },

      ],
    },
  },
};
export default function () {
    let page = Math.floor(Math.random() * 574) + 1;
    
    // Открытие страницы с этим номером
    let res = http.get('http://computer-database.gatling.io/computers/' + page);
    
    // Извлечение названия компьютера
    let computerName = res.body.match(/<input[^>]*id="name"[^>]*value="([^"]+)"/)


    writer.produce({
      messages: [
          {

          value: schemaRegistry.serialize({
              data: computerName[1],
              schemaType: SCHEMA_TYPE_STRING,
          }),
          },
      ],
      });
  
}

export function teardown(data) {
    writer.close();
}
import * as amqp from "amqplib";

interface OrderEvent {
  id: number;
  customer: string;
  event: string;
}

async function sendOrderEvents() {
  const connection = await amqp.connect("amqp://user:password@localhost:5672");
  console.log("Connected to RabbitMQ successfully!");
  const channel = await connection.createChannel();

  const exchange = "virtuozo.direct";
  const queue = "nfe-queue";

  channel.assertExchange(exchange, "direct");
  channel.assertQueue(queue);
  channel.bindQueue(queue, exchange, "order.created");

  console.log("Waiting for NF-e events...");

  channel.consume(
    queue,
    (msg) => {
      if (msg) {
        const orderEvent: OrderEvent = JSON.parse(msg.content.toString());
        console.log("Received NF-e event:", orderEvent);
        channel.ack(msg);
      }
    },
    { noAck: false },
  );
}

sendOrderEvents().catch((error) => {
  console.error("Unexpected error in producer:", error);
});

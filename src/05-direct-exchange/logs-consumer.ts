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
  const queue = "logs-queue";
  const routingKeys = ["order.created", "order.shipped", "order.updated"];

  channel.assertExchange(exchange, "direct");
  channel.assertQueue(queue);
  await Promise.all(
    routingKeys.map((routingKey) =>
      channel.bindQueue(queue, exchange, routingKey),
    ),
  );

  console.log("Waiting for Logs events...");

  channel.consume(
    queue,
    (msg) => {
      if (msg) {
        const orderEvent: OrderEvent = JSON.parse(msg.content.toString());
        console.log("Received Logs event:", orderEvent);
        channel.ack(msg);
      }
    },
    { noAck: false },
  );
}

sendOrderEvents().catch((error) => {
  console.error("Unexpected error in producer:", error);
});

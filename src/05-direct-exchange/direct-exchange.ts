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

  const exchange = "amq.direct";

  const orderEvents: OrderEvent[] = [
    { id: 1, customer: "Alice", event: "order.created" },
    { id: 2, customer: "Bob", event: "order.created" },
    { id: 3, customer: "Charlie", event: "order.shipped" },
    { id: 4, customer: "Diana", event: "order.updated" },
  ];

  await channel.assertExchange(exchange, "direct", { durable: true });

  for (const orderEvent of orderEvents) {
    const routingKey = orderEvent.event;
    const message = Buffer.from(JSON.stringify(orderEvent));
    channel.publish(exchange, routingKey, message);
  }

  setTimeout(() => {
    connection.close(); //close the channels automatically
    process.exit(0);
  }, 500);
}

sendOrderEvents().catch((error) => {
  console.error("Unexpected error in producer:", error);
});

import amqp from "amqplib";

const EXCHANGE_NAME = "amq.direct";

type Order = {
  id: number;
  customerName: string;
  items: Array<{ productId: number; quantity: number }>;
  totalAmount: number;
  createdAt: Date;
};

export async function publishOrder() {
  const connection = await amqp.connect("amqp://admin:password@localhost:5672");
  const channel = await connection.createChannel();

  await channel.assertExchange(EXCHANGE_NAME, "direct", { durable: true });

  const order: Order = {
    id: Math.floor(Math.random() * 1000),
    customerName: "John Doe",
    items: [
      { productId: 1, quantity: 2 },
      { productId: 2, quantity: 1 },
    ],
    totalAmount: 5999,
    createdAt: new Date(),
  };

  const isPublished = channel.publish(
    EXCHANGE_NAME,
    "order.created",
    Buffer.from(JSON.stringify(order)),
    {
      persistent: true,
    },
  );

  //isPublished will be true if the message was sent to the broker, but it doesn't guarantee that it was received by the broker. To ensure that the message was received, we can use the confirm channel.

  console.log(`Order published: ${JSON.stringify(order)}`);

  setTimeout(() => {
    channel.close();
    connection.close();
  }, 500);
}

publishOrder()
  .then(() => {
    console.log("Order published successfully.");
  })
  .catch((error) => {
    console.error("Error publishing order:", error);
  });

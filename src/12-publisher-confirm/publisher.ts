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
  const connection = await amqp.connect("amqp://user:password@localhost:5672");
  const channel = await connection.createConfirmChannel();

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

  channel.on("return", (msg) => {
    /**
     * fields: {
      replyCode: 312,
      replyText: 'NO_ROUTE',
      exchange: 'amq.direct',
      routingKey: 'order.created'
    },
    **/
    // The 'return' event is emitted when a message is published with the 'mandatory' flag set to true, but the message cannot be routed to any queue. This can happen if there are no queues bound to the exchange with the specified routing key.
    console.error("Message was returned:", msg);
  });

  //isPublished will be true if the message was sent to the broker, but it doesn't guarantee that it was received by the broker. To ensure that the message was received, we can use the confirm channel.
  const isPublished = channel.publish(
    EXCHANGE_NAME,
    "order.created",
    Buffer.from(JSON.stringify(order)),
    {
      persistent: true,
      mandatory: true,
    },
    (err, ok) => {
      if (err) {
        console.error("Message was not published:", err);
      } else {
        console.log("Message was published successfully.", ok);
      }
    },
  );

  // Wait for the confirmation that the message was received by the broker
  await channel.waitForConfirms();

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

import * as amqp from "amqplib";

async function producer() {
  const connection = await amqp.connect("amqp://user:password@localhost:5672");
  console.log("Connected to RabbitMQ successfully!");
  const channel = await connection.createChannel();

  const queue = "task_queue";
  await channel.assertQueue(queue); // Create the queue if it doesn't exist

  const messages = new Array(20).fill(null).map((_, index) => ({
    id: index,
    content: `Message number ${index}`,
    number: Math.floor(Math.random() * 100),
  }));

  await Promise.all(
    messages.map((message) =>
      channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)), {
        contentType: "application/json",
      }),
    ),
  );

  setTimeout(() => {
    connection.close(); //close the channels automatically
    process.exit(0);
  }, 500);
}

producer().catch((error) => {
  console.error("Unexpected error in producer:", error);
});

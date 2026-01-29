import * as amqp from "amqplib";

async function producer() {
  const connection = await amqp.connect("amqp://user:password@localhost:5672");
  console.log("Connected to RabbitMQ successfully!");
  const channel = await connection.createChannel();

  const queue = "task_queue";
  const message = { id: 1, content: "Hello, World!" };

  await channel.assertQueue(queue); // Create the queue if it doesn't exist
  channel.sendToQueue(queue, Buffer.from(JSON.stringify(message)));

  console.log(`Sent message: ${JSON.stringify(message)}`, {
    contentType: "application/json",
  });

  setTimeout(() => {
    connection.close(); //close the channels automatically
    process.exit(0);
  }, 500);
}

producer().catch((error) => {
  console.error("Unexpected error in producer:", error);
});

import * as amqp from "amqplib";

async function producer() {
  const connection = await amqp.connect("amqp://user:password@localhost:5672");
  console.log("Connected to RabbitMQ successfully!");
  const channel = await connection.createChannel();

  const queue = "task_queue";
  const message = "Hello World!";

  await channel.assertQueue(queue); // Create the queue if it doesn't exist
  channel.sendToQueue(queue, Buffer.from(message));

  console.log(`Sent message: ${message}`);

  setTimeout(() => {
    connection.close(); //close the channels automatically
    process.exit(0);
  }, 500);
}

producer().catch((error) => {
  console.error("Unexpected error in producer:", error);
});

import * as amqp from "amqplib";

async function consumer() {
  const connection = await amqp.connect("amqp://user:password@localhost:5672");
  console.log("Connected to RabbitMQ successfully!");

  const channel = await connection.createChannel();

  const queue = "task_queue";

  channel.assertQueue(queue); // Create the queue if it doesn't exist
  console.log("Waiting for messages in %s. To exit press CTRL+C", queue);

  channel.consume(queue, (msg) => {
    if (msg !== null) {
      const messageContent = msg.content.toString();
      const obj = JSON.parse(messageContent);
      const contentType = msg.properties.contentType;
      console.log("Received message:", obj);
      console.log("Content Type:", contentType);
      channel.ack(msg); // Acknowledge the message
    }
  });
}

consumer().catch((error) => {
  console.error("Unexpected error in consumer:", error);
});

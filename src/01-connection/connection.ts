import * as amqp from "amqplib";

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function connect() {
  try {
    const connection = await amqp.connect(
      "amqp://user:password@localhost:5672",
    );
    console.log("Connected to RabbitMQ successfully!");

    //pelo canal é feita a manipulação do RabbitMQ
    const channel = await connection.createChannel();
    console.log("Channel created successfully!");

    await sleep(10000);

    //Importante fechar a conexão e o canal após o uso
    await channel.close();
    await connection.close();
    console.log("Connection closed successfully!!");
  } catch (error) {
    console.error("Connection failed, retrying in 5 seconds...", error);
  }
}

connect();

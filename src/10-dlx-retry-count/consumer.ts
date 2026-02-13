import amqp from "amqplib";

async function deadLetterExchange() {
  const connection = await amqp.connect("amqp://user:password@localhost:5672");
  const channel = await connection.createChannel();

  const queue = "nfe.queue";

  await channel.assertExchange("amq.direct", "direct");
  await channel.assertQueue(queue, {
    deadLetterExchange: "dlx.exchange",
  });
  await channel.bindQueue(queue, "amq.direct", "order");

  await channel.assertQueue("fail.queue");

  await channel.assertExchange("dlx.exchange", "direct");
  await channel.assertQueue("retry.queue", {
    messageTtl: 5000,
    deadLetterExchange: "amq.direct",
  });
  await channel.bindQueue("retry.queue", "dlx.exchange", "order");

  console.log(`[*] Waiting for messages in ${queue}. To exit press CTRL+C`);

  channel.consume(
    queue,
    (msg) => {
      // Simular processamento, apenas para fins didático
      //setTimeout(() => {
      const content = msg?.content.toString();
      if (!msg || !content) {
        console.log("[!] Received empty message, ignoring...");
        if (msg) {
          channel.sendToQueue(
            "fail.queue",
            Buffer.from(
              JSON.stringify({
                error: "Received empty message",
                payload: msg.content.toString(),
              }),
            ),
          );
          channel.ack(msg); //se fizer nack ou reject, a mensagem vai para o DLX, mas nesse caso queremos enviar para uma fila de falha específica.
        }
        return;
      }

      console.log(`[x] Received '${content}'`);

      try {
        // Simular sucesso ou falha
        if (parseInt(content) > 5) {
          throw new Error("Processing failed");
        }

        console.log("[x] Done processing");
        channel.ack(msg, true);
      } catch (error) {
        //se aconteceu um erro não reprocessável, publicar na fila de falha.

        const maxRetries = 3;
        const xDeath = msg.properties.headers?.["x-death"] || [];
        const retryCount = xDeath[0]?.count || 0;
        if (retryCount < maxRetries) {
          channel.nack(msg, false, false); //channel.reject(msg, true);
          console.error(
            `[!] Processing error, retrying (${retryCount + 1}/${maxRetries})...`,
          );
          return;
        }

        //@ts-expect-error
        const newMsg = { error: error.message, payload: content };
        channel.sendToQueue("fail.queue", Buffer.from(JSON.stringify(newMsg)));
        console.log(
          "Sending message to fail.queue after max retries reached:",
          newMsg,
        );

        channel.ack(msg); //ack para remover da fila de retry, já que enviamos para a fila de falha.

        //@ts-expect-error
        console.error("[!] Processing error:", error.message);
      }
      //}, 10000);
    },
    { noAck: false },
  );
}

deadLetterExchange().catch(console.error);

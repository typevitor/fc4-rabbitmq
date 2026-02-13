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
  //DLX
  await channel.assertExchange("dlx.exchange", "direct");
  await channel.assertQueue("dlx.queue");
  await channel.bindQueue("dlx.queue", "dlx.exchange", "order");

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
        //@ts-expect-error
        console.error("[!] Processing error:", error.message);

        channel.nack(msg, false, true); //channel.reject(msg, true);
      }
      //}, 10000);
    },
    { noAck: false },
  );
}

deadLetterExchange().catch(console.error);

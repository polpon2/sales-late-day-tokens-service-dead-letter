import asyncio, aio_pika, json

async def rb_order(connection: aio_pika.Connection, body: bytes) -> None:
    channel = await connection.channel()
    await channel.default_exchange.publish(
        aio_pika.Message(body=body),
        routing_key="rb.order",
    )
    return


async def rb_payment(connection: aio_pika.Connection, body: bytes) -> None:
    channel = await connection.channel()
    await channel.default_exchange.publish(
        aio_pika.Message(body=body),
        routing_key="rb.payment",
    )
    return


async def rb_inventory(connection: aio_pika.Connection, body: bytes) -> None:
    channel = await connection.channel()
    await channel.default_exchange.publish(
        aio_pika.Message(body=body),
        routing_key="rb.inventory",
    )
    return


async def rb_deliver(connection: aio_pika.Connection, body: bytes) -> None:
    channel = await connection.channel()
    await channel.default_exchange.publish(
        aio_pika.Message(body=body),
        routing_key="rb.deliver",
    )
    return


async def process_rollback(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        body: dict = json.loads(message.body)

        stage: int = body['stage']

        print(f" [x] Received {body}")

        match stage:
            case 1:
                await rb_order(connection=connection, body=message.body)
            case 2:
                await rb_payment(connection=connection, body=message.body)
            case 3:
                await rb_inventory(connection=connection, body=message.body)
            case 4:
                await rb_deliver(connection=connection, body=message.body)
            case _:
                return


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://rabbit-mq",
    )

    exchange_name = "dlx"

    # Creating channel
    channel = await connection.channel()

    # Maximum message count which will be processing at the same time.
    await channel.set_qos(prefetch_count=10)

    # Declaring exchange dl
    exchange = await channel.declare_exchange(exchange_name, type='direct')

    # Declaring queue
    queue = await channel.declare_queue(name="dl", arguments={
                                                    'x-dead-letter-exchange': 'amq.direct',
                                                    })
    await channel.declare_queue("rb.payment")
    await channel.declare_queue("rb.inventory")
    await channel.declare_queue("rb.deliver")

    print(' [*] Waiting for messages. To exit press CTRL+C')

    # Binding dl exchange with queue
    await queue.bind(exchange=exchange)

    await queue.consume(lambda message: process_rollback(message, connection))

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
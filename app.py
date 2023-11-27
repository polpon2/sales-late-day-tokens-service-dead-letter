import pika, sys, os, json
from dotenv import load_dotenv
import requests

load_dotenv()

bucket_name = "toktik-s3"

def request_dead(file: str):
    # url = os.getenv('COMPLETED_ENDPOINT')

    # try:
    #     requests.post(url, json = json.loads(file))
    # except: # Set default
    #     requests.post("http://localhost:8000/api/process-failed", json = json.loads(file))
    return True


def callback(ch, method, properties, body):
    body: str = body.decode('utf-8')
    # body: dict = json.loads(body)

    print(f" [x] DEAD - {body}")

    request_dead(body)
    return


def main():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbit-mq', port=5672))
    except:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    channel.exchange_declare(exchange='dlx', exchange_type='direct')

    channel.queue_declare(queue='dl',
                        arguments={
                                'x-message-ttl': 5000,
                                'x-dead-letter-exchange': 'amq.direct',
                        })

    channel.queue_bind(exchange='dlx', queue='dl')

    channel.basic_consume(queue='dl', on_message_callback=callback, auto_ack=True)

    print(' [*] Waiting for dead messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
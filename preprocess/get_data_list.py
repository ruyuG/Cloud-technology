import json
import infofile
import pika
from pika.exceptions import AMQPConnectionError, AMQPChannelError, AMQPError
import time
import logging
import socket

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
def wait_for_rabbitmq(host='rabbitmq', retries=10, initial_wait_time=10):
    wait_time = initial_wait_time
    for i in range(retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            connection.close()
            print("RabbitMQ is ready!")
            return True
        except (AMQPConnectionError,AMQPChannelError,AMQPError, socket.gaierror) as e:
            print(f"RabbitMQ not ready, attempt {i + 1}/{retries},unable to connect to RabbitMQ: {e}")
            time.sleep(wait_time)
            wait_time *= 2
    logging.error(f"RabbitMQ is not ready after {retries} retries. Exiting.")
    return False

def send_tasks_to_queue(files):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    #channel.queue_declare(queue='end_signal', durable=True)
    channel.queue_declare(queue='total_tasks', durable=True)
    total_tasks = len(files)
    logging.info(f"Total tasks is {total_tasks} ")
    for file in files:
        message = json.dumps(file)
        channel.basic_publish(exchange='',
                              routing_key='task_queue',
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
    """
    # Send end of preprocess message for preocess start
    end_message = json.dumps({"type": "end_of_preprocess"})
    channel.basic_publish(exchange='',
                          routing_key='end_signal',
                          body=end_message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))
    logging.info("All tasks sent to queue")
    """

    # Send total tasks message for plot
    task_count_message = json.dumps({"type": "total_tasks", "count": total_tasks})
    channel.basic_publish(exchange='',
                          routing_key='total_tasks',
                          body=task_count_message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,
                          ))
    connection.close()


def get_data_list(samples, tuple_path):
    file_data = []
    for s in samples:
        print('Processing '+s+' samples')
        for val in samples[s]['list']:
            if s == 'data':
                prefix = "Data/"
            else:
                prefix = "MC/mc_" + str(infofile.infos[val]["DSID"]) + "."
            fileString = tuple_path + prefix + val + ".4lep.root"
            file_data.append({"fileString": fileString, "val": val, "category": s})

    return file_data
def main():
    if not wait_for_rabbitmq():
        exit(1)

    # file path
    tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
    # Loading samples
    with open('samples.json', 'r') as f:
        samples = json.load(f)

    # Get the data
    file_data = get_data_list(samples, tuple_path)
    """
    # Print or save the data
    print(json.dumps(file_data, indent=4))
    with open('file_data.json', 'w') as f:
        json.dump(file_data, f, indent=4)
    """
    send_tasks_to_queue(file_data)
if __name__ == "__main__":
    main()

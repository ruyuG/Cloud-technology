# base on latest python image
FROM python:3.9-slim
# add our python program
WORKDIR /app
COPY . /app
# required packages
RUN pip install --no-cache-dir pika
# Define environment variable to specify the host of RabbitMQ
ENV RABBITMQ_HOST=rabbitmq
# the command to run our program
CMD [ "python", "get_data_list.py"]


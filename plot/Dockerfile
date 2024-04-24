# base on latest python image
FROM python:3.9-slim
# add our python program
WORKDIR /app
COPY . /app
# install dependent libraries
RUN pip install numpy matplotlib awkward pyarrow pandas pika
# the command to run our program
CMD [ "python", "plot.py"]


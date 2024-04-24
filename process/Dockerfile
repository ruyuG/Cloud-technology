# base on latest python image
FROM python:3.9-slim
# add our python program
WORKDIR /app
COPY . /app
#COPY ../config/infofile.py /app
# install dependent libraries
RUN pip install numpy matplotlib uproot awkward vector pika requests aiohttp pyarrow
# Make port 80 available to the world outside this container
EXPOSE 80

# Define environment variable
ENV NAME World

# the command to run our program
CMD [ "python", "process.py"]


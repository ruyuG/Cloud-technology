import uproot # for reading .root files
import awkward as ak # to represent nested data in columnar format
import vector # for 4-momentum calculations
import time # to measure time to analyse
import math # for mathematical functions such as square root
import numpy as np # for numerical calculations such as histogramming
import matplotlib.pyplot as plt # for plotting
from matplotlib.ticker import AutoMinorLocator # for minor ticks
import sys
import infofile
import pika
import json
from pika.exceptions import AMQPConnectionError,AMQPChannelError
import logging
import pickle
import socket
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


# define C value
lumi = 10 # fb-1 # data_A,data_B,data_C,data_D
fraction = 1.0 # reduce this is if you want the code to run quicker
#tuple_path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
MeV = 0.001
GeV = 1.0

def wait_for_rabbitmq(host='rabbitmq', retries=10, initial_wait_time=10):
    wait_time = initial_wait_time
    for i in range(retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            connection.close()
            print("RabbitMQ is ready!")
            return True
        except (AMQPConnectionError,AMQPChannelError, socket.gaierror) as e:
            print(f"RabbitMQ not ready, attempt {i + 1}/{retries},unable to connect to RabbitMQ: {e}")
            time.sleep(wait_time)
            wait_time *= 2
    logging.error(f"RabbitMQ is not ready after {retries} retries. Exiting.")
    return False

def setup_rabbitmq():
    if not wait_for_rabbitmq(host='rabbitmq'):
        print(" Failed to connect to Ribbitmq")
    # RabbitMQ setup
    parameters = pika.ConnectionParameters(host='rabbitmq', heartbeat=600)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)
    #channel.queue_declare(queue='end_signal', durable=True)
    return connection, channel
# define function to calculate weight of MC event
def calc_weight(xsec_weight, events):
    return (
        xsec_weight
        * events.mcWeight
        * events.scaleFactor_PILEUP
        * events.scaleFactor_ELE
        * events.scaleFactor_MUON
        * events.scaleFactor_LepTRIGGER
    )

# define function to get cross-section weight
def get_xsec_weight(sample):
    info = infofile.infos[sample] # open infofile
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    return xsec_weight # return cross-section weight

# define function to calculate 4-lepton invariant mass.
def calc_mllll(lep_pt, lep_eta, lep_phi, lep_E):
    # construct awkward 4-vector array
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    # calculate invariant mass of first 4 leptons
    # [:, i] selects the i-th lepton in each event
    # .M calculates the invariant mass
    return (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV

# cut on lepton charge
# paper: "selecting two pairs of isolated leptons, each of which is comprised of two leptons with the same flavour and opposite charge"
def cut_lep_charge(lep_charge):
# throw away when sum of lepton charges is not equal to 0
# first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    return lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0

# cut on lepton type
# paper: "selecting two pairs of isolated leptons, each of which is comprised of two leptons with the same flavour and opposite charge"
def cut_lep_type(lep_type):
# for an electron lep_type is 11
# for a muon lep_type is 13
# throw away when none of eeee, mumumumu, eemumu
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    return (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)

def read_file(path,sample,category):
    start = time.time() # start the clock
    print("\tProcessing: "+sample + "  "+ category) # print which sample is being processed
    data_all = [] # define empty list to hold all data for this sample

    # open the tree called mini using a context manager (will automatically close files/resources)
    with uproot.open(path + ":mini") as tree:
        numevents = tree.num_entries # number of events
        if 'data' not in sample: xsec_weight = get_xsec_weight(sample) # get cross-section weight
        for data in tree.iterate(['lep_pt','lep_eta','lep_phi',
                                  'lep_E','lep_charge','lep_type',
                                  # add more variables here if you make cuts on them
                                  'mcWeight','scaleFactor_PILEUP',
                                  'scaleFactor_ELE','scaleFactor_MUON',
                                  'scaleFactor_LepTRIGGER'], # variables to calculate Monte Carlo weight
                                 library="ak", # choose output type as awkward array
                                 entry_stop=numevents*fraction): # process up to numevents*fraction

            nIn = len(data) # number of events in this batch

            if 'data' not in sample: # only do this for Monte Carlo simulation files
                # multiply all Monte Carlo weights and scale factors together to give total weight
                data['totalWeight'] = calc_weight(xsec_weight, data)

            # cut on lepton charge using the function cut_lep_charge defined above
            data = data[~cut_lep_charge(data.lep_charge)]

            # cut on lepton type using the function cut_lep_type defined above
            data = data[~cut_lep_type(data.lep_type)]

            # calculation of 4-lepton invariant mass using the function calc_mllll defined above
            data['mllll'] = calc_mllll(data.lep_pt, data.lep_eta, data.lep_phi, data.lep_E)

            # array contents can be printed at any stage like this
            #print(data)

            # array column can be printed at any stage like this
            #print(data['lep_pt'])

            # multiple array columns can be printed at any stage like this
            #print(data[['lep_pt','lep_eta']])

            nOut = len(data) # number of events passing cuts in this batch
            data_all.append(data) # append array from this batch
            elapsed = time.time() - start # time taken to process
            print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)+"\t in "+str(round(elapsed,1))+"s") # events before and after
    return ak.concatenate(data_all) # return array containing events passing all cuts

"""
# Check if the correct number of arguments was given
if len(sys.argv) != 4:
    print("Usage: python script.py <path> <sample> <category>")
    sys.exit(1)

# Assign arguments to variables
path = sys.argv[1]
sample = sys.argv[2]
category = sys.argv[3]
"""
def wait_for_file_list(channel):
    logging.info(' [*] Waiting for preprocess to complete.')
    for method_frame, properties, body in channel.consume('end_signal', auto_ack=True, inactivity_timeout=2):
        if body:
            task = json.loads(body)
            if task.get("type") == "end_of_preprocess":
                logging.info("Preprocess completed, starting process tasks.")
                break
        else:
            # Timeout reached, no message, keep waiting
            logging.info("No end signal yet, continuing to wait...")
            continue


def handle_message(ch, method, properties, body):
    logging.info("Start handle message")
    try:
        task = json.loads(body)
        path = task['fileString']
        sample = task['val']
        category = task['category']

        # Call read_file or any other processing function
        start = time.time() # time at start of whole processing
        data = read_file(path, sample,category)
        elapsed = time.time() - start # time after whole processing
        print("Time taken: "+str(round(elapsed,1))+"s") # print total time taken to process every file

        # Save processed data to a file 
        #ak.to_parquet(data, f'/data/{category}-{sample}.parquet')
        #logging.info(f"Processed {sample} saved to /data/{category}-{sample}.parquet")

        send_data_to_queue(data,category, sample)

        # Acknowledge the message as processed
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error("Failed to process: {}".format(e), exc_info=True)
        print(f"Failed to process message: {str(e)}")
        ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

def send_data_to_queue_bak(data, queue_name='plot_queue'):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)

    message = pika.dumps(data)
    channel.basic_publish(
        exchange='',
        routing_key=queue_name,
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
            content_type='application/octet-stream'
        )
    )
    connection.close()
def send_data_to_queue(data, category, sample):
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()
    channel.queue_declare(queue='plot_queue', durable=True)
    
    # Serialize data with pickle to maintain structure
    serialized_data = pickle.dumps({'category': category, 'sample': sample, 'data': data})
    
    # Send message
    channel.basic_publish(
        exchange='',
        routing_key='plot_queue',
        body=serialized_data,
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
    )
    logging.info(f"Data for {category}-{sample} sent to plot queue")
    connection.close()

def main():
    connection, channel = setup_rabbitmq()
    #wait_for_file_list(channel)

    # Set up consumption of messages
    channel.basic_consume(queue='task_queue', on_message_callback=handle_message, auto_ack=False)
    print(' Waiting for messages.')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    """
    # Send end signal
    end_message = json.dumps({"type": "end_of_process"})
    channel.basic_publish(exchange='',
                          routing_key='end_signal',
                          body=end_message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))

    print("Sent end meaasage to queue")
    """
if __name__ == "__main__":
    main()

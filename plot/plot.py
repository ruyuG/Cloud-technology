import awkward as ak
import glob
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import AutoMinorLocator
import os
import json
import time
import pandas
import pika
from pika.exceptions import AMQPConnectionError,AMQPChannelError
import pickle
import logging
import socket

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

MeV = 0.001
GeV = 1.0
lumi = 10  # fb-1
fraction = 1.0  # For simplicity, process all data


data_store = {}
total_tasks_expected = 0
tasks_received = 0

# Loading the samples
with open('samples.json', 'r') as f:
    samples = json.load(f)

"""
MeV = float(os.getenv('MeV', '0.001'))  
GeV = float(os.getenv('GeV', '1.0'))
lumi = float(os.getenv('LUMI', '10'))
fraction = float(os.getenv('Fraction','1.0'))
"""

def merge_data(file_pattern):
    """Combine all data and reconstruct the structure based on category and identifier from file names."""
    print('Merge start')
    data = {}  # Dictionary to hold combined arrays by category

    # Load all parquet files and group by category
    files = glob.glob(file_pattern)
    for file in files:
        filename = os.path.basename(file)
        # Assuming filename format is 'Category-Val.parquet'
        parts = filename.split('-')
        if len(parts) == 2:
            category = parts[0]
            val = parts[1].replace('.parquet', '')  # Directly remove the .parquet extension

            if category not in data:
                data[category] = []

            array = ak.from_parquet(file)
            data[category].append(array)

    # Concatenate arrays for each category
    for category in data:
        if data[category]:  # Ensure there are arrays to concatenate
            data[category] = ak.concatenate(data[category], axis=0)
        else:
            data[category] = ak.Array([])  # Handle empty lists appropriately

    return data



def plot_data(data):
    print('plot start')
    xmin = 80 * GeV
    xmax = 250 * GeV
    step_size = 5 * GeV

    bin_edges = np.arange(start=xmin, # The interval includes this value
                     stop=xmax+step_size, # The interval doesn't include this value
                     step=step_size ) # Spacing between values
    bin_centres = np.arange(start=xmin+step_size/2, # The interval includes this value
                            stop=xmax+step_size/2, # The interval doesn't include this value
                            step=step_size ) # Spacing between values

    data_x,_ = np.histogram(ak.to_numpy(data['data']['mllll']), 
                            bins=bin_edges ) # histogram the data
    data_x_errors = np.sqrt( data_x ) # statistical error on the data

    signal_x = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)']['mllll']) # histogram the signal
    signal_weights = ak.to_numpy(data[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

    mc_x = [] # define list to hold the Monte Carlo histogram entries
    mc_weights = [] # define list to hold the Monte Carlo weights
    mc_colors = [] # define list to hold the colors of the Monte Carlo bars
    mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

    for s in samples: # loop over samples
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
            mc_x.append( ak.to_numpy(data[s]['mllll']) ) # append to the list of Monte Carlo histogram entries
            mc_weights.append( ak.to_numpy(data[s].totalWeight) ) # append to the list of Monte Carlo weights
            mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
            mc_labels.append( s ) # append to the list of Monte Carlo legend labels
    


    # *************
    # Main plot 
    # *************
    main_axes = plt.gca() # get current axes
    
    # plot the data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                       fmt='ko', # 'k' means black and 'o' is for circles 
                       label='Data') 
    
    # plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges, 
                                weights=mc_weights, stacked=True, 
                                color=mc_colors, label=mc_labels )
    
    mc_x_tot = mc_heights[0][-1] # stacked background MC y-axis value
    
    # calculate MC statistical uncertainty: sqrt(sum w^2)
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])
    
    # plot the signal bar
    main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot, 
                   weights=signal_weights, color=signal_color,
                   label=r'Signal ($m_H$ = 125 GeV)')
    
    # plot the statistical uncertainty
    main_axes.bar(bin_centres, # x
                  2*mc_x_err, # heights
                  alpha=0.5, # half transparency
                  bottom=mc_x_tot-mc_x_err, color='none', 
                  hatch="////", width=step_size, label='Stat. Unc.' )

    # set the x-limit of the main axes
    main_axes.set_xlim( left=xmin, right=xmax ) 
    
    # separation of x axis minor ticks
    main_axes.xaxis.set_minor_locator( AutoMinorLocator() ) 
    
    # set the axis tick parameters for the main axes
    main_axes.tick_params(which='both', # ticks on both x and y axes
                          direction='in', # Put ticks inside and outside the axes
                          top=True, # draw ticks on the top axis
                          right=True ) # draw ticks on right axis
    
    # x-axis label
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                        fontsize=13, x=1, horizontalalignment='right' )
    
    # write y-axis label for main axes
    main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                         y=1, horizontalalignment='right') 
    
    # set y-axis limits for main axes
    main_axes.set_ylim( bottom=0, top=np.amax(data_x)*1.6 )
    
    # add minor ticks on y-axis for main axes
    main_axes.yaxis.set_minor_locator( AutoMinorLocator() ) 

    # Add text 'ATLAS Open Data' on plot
    plt.text(0.05, # x
             0.93, # y
             'ATLAS Open Data', # text
             transform=main_axes.transAxes, # coordinate system used is that of main_axes
             fontsize=13 ) 
    
    # Add text 'for education' on plot
    plt.text(0.05, # x
             0.88, # y
             'for education', # text
             transform=main_axes.transAxes, # coordinate system used is that of main_axes
             style='italic',
             fontsize=8 ) 
    
    # Add energy and luminosity
    lumi_used = str(lumi*fraction) # luminosity to write on the plot
    plt.text(0.05, # x
             0.82, # y
             '$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes
    
    # Add a label for the analysis carried out
    plt.text(0.05, # x
             0.76, # y
             r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text 
             transform=main_axes.transAxes ) # coordinate system used is that of main_axes

    # draw the legend
    main_axes.legend( frameon=False ) # no box around the legend
    plt.savefig('/data/output_plot.png')
    logging.info("Saved /data/output_plot.png")
    return

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

def setup_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='plot_queue', durable=True)
    channel.queue_declare(queue='total_tasks', durable=True)
    channel.basic_consume(queue='plot_queue', on_message_callback=on_message_received, auto_ack=False)
    channel.basic_consume(queue='total_tasks', on_message_callback=on_message_received, auto_ack=False)
    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()


def merge_all_data():
    for category in data_store:
        if data_store[category]:
            data_store[category] = ak.concatenate(data_store[category], axis=0)
        else:
            data_store[category] = ak.Array([])  # Handle empty lists appropriately
    logging.info("Data merged successfully.")

def on_message_received(ch, method, properties, body):
    global tasks_received, total_tasks_expected
    

    if method.routing_key == 'total_tasks':
        message = json.loads(body)
        total_tasks_expected = message["count"]
        logging.info(f"Total tasks to receive: {total_tasks_expected}")
    else:
        message = pickle.loads(body)
        category = message['category']
        data = message['data']

        if category not in data_store:
            data_store[category] = []

        data_store[category].append(data)
        tasks_received += 1
        logging.info(f"Received and processed data for {category}")
    ch.basic_ack(delivery_tag=method.delivery_tag)
        # Check if all tasks are received
    if tasks_received == total_tasks_expected:
        logging.info("All tasks received, beginning merge and plot.")
        merge_all_data()
        plot_data(data_store)

def main():
    wait_for_rabbitmq()
    logging.info("Consumer setup starting.")
    setup_consumer()
    logging.info("Consumer setup completed.")

if __name__ == '__main__':
    main()

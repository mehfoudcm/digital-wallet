import csv
import os
import shutil
import time
import numpy as np
import pandas as pd
import networkx as nx

#start clock and pull in the historical transactions
t = time.time()

#initialize the system 
paymentlist1 = [("time", "id1", "id2","amount")]

#####CHANGES#####
shutil.copy2('paymo_input/batch_payment.txt','paymo_input/batch_payment.csv')
shutil.copy2('paymo_input/stream_payment.txt','paymo_input/stream_payment.csv')
#os.rename('paymo_input/batch_payment.txt','paymo_input/batch_payment.csv')
#os.rename('paymo_input/stream_payment.txt','paymo_input/stream_payment.csv')


#call the historical transactions
f = open('paymo_input/batch_payment.csv','rU')


#clean the historical transactions
for line in f:
    col = line.split(",")
    try:
        rowcheck = time.mktime(time.strptime(col[0], '%Y-%m-%d %H:%M:%S'))
    except ValueError:
        rowcheck = "skip"        
    if rowcheck != "skip":
        paymentlist1.append((col[0],col[1],col[2],col[3]))
  
          
#writes clean list in case other analytics are needed (without messages)
f.close()
with open('paymentlist1.csv', 'w') as paylist2:
    paylist3 = csv.writer(paylist2)
    for row in paymentlist1:
        paylist3.writerow(row)
paymentlist = pd.read_csv('paymentlist1.csv',header=0,skipinitialspace=True,usecols=["time", "id1", "id2","amount"])


#builds a network between users with the historical transaction data
networkarr = paymentlist[['id1','id2']].values


#uses both nodes representing no emphasis on direction
networknodes1 = networkarr[:,0]
networknodes2 = networkarr[:,1]
networknodes = np.hstack((networknodes1,networknodes2))
networknodesarray = np.unique(networknodes)
g = nx.Graph()


#using networkx draws a graph with equal weights on all edges
g.add_nodes_from(networknodesarray,weight=1)
g.add_edges_from(networkarr)
for (a, b) in g.edges():
    g.add_edge(a,b,weight=1)


#initializes and writes a clean stream payments table
newpayment1 = [("time", "id1", "id2","amount")]



f = open('paymo_input/stream_payment.csv', 'rU')
for line in f:
    col = line.split(",")
    try:
        rowcheck = time.mktime(time.strptime(col[0], '%Y-%m-%d %H:%M:%S'))
    except ValueError:
        rowcheck = "skip"
        
    if rowcheck != "skip":
        newpayment1.append((col[0],col[1],col[2],col[3]))       
f.close()
with open('newpayment1.csv', 'w') as paylist2:
    paylist3 = csv.writer(paylist2)
    for row in newpayment1:
        paylist3.writerow(row)


#writes clean streamed payment list in case other analytics are needed (without messages)
newpayment = pd.read_csv('newpayment1.csv',header=0,skipinitialspace=True,usecols=["time", "id1", "id2","amount"])

######CHANGES######
#counts the number of transactions in the stream
count = newpayment['id1'].count()


#Establish a price cap based on sample of history of data for additional feature 1
if count > 100:
    sample1 = paymentlist.loc[np.random.choice(paymentlist.index, 100, replace=False)]
    avg = sample1.mean(axis=0)
    avgp = avg[2] #mean
    std = sample1.std(axis=0)
    stdp = std[2] #standard deviation
    pricecap = avgp+2*stdp #zscore
#data built on normal distribution, so we can just pull avg + 2*std from, 24.89+2*10.05
#use z score cap of 44.98, anything over within top 2.5% should be verified
#pricecap = 44.98
else:
    pricecap = 44.98

highprice = []
for row in newpayment['amount']:
	if row > pricecap:
		highprice.append(1)
	else:
		highprice.append(0)

if count > 1:
#finds the total time of the stream
    firsttime = newpayment["time"].iloc[0]
    firsttime = time.mktime(time.strptime(firsttime, '%Y-%m-%d %H:%M:%S'))
    lasttime = newpayment["time"].iloc[-1]
    lasttime = time.mktime(time.strptime(lasttime, '%Y-%m-%d %H:%M:%S'))
    totaltime = lasttime-firsttime+1
else:
    totaltime = 1

#sets up count of transactions over time for additional feature 2
if count > 1:
    newpayment1=newpayment
    timing=newpayment1.id2.value_counts().reset_index().rename(columns={'index': 'id2', 0: 'countpersec'})
    timing['countpersec'] = timing['countpersec']/(totaltime)
    newpayment = newpayment.reset_index().merge(timing, on='id2', how="left").set_index('index')
    newpayment.sort_index(inplace=True)   
else:
    newpayment['countpersec'] = 1

#Runs through the graph using dijkstra's algorithm to find the pathlength
nodeset1 = newpayment[['id1','id2']].values
pathlength = []
for row in nodeset1:
    if g.has_node(row[0]) and g.has_node(row[1]):
        try:
            d = len(nx.shortest_path(g,row[0],row[1]))-1
            pathlength.append(d)
        except nx.NetworkXNoPath:
            pathlength.append(0)
    else:
        pathlength.append(0)


#writes the price and pathlength onto the graph already including count of transactions
newpayment['degreeoftransaction'] = pathlength
newpayment['priceovercap'] = highprice

		
#writes the outputs
output1 = []
for row in newpayment['degreeoftransaction']:
    if row == 1:
        output1.append('trusted')
    else:
        output1.append('unverified')
output1f = open('paymo_output/output1.txt', 'w')
for item in output1:
    print>>output1f, item
output1f.close()
output2 = []
for row in newpayment['degreeoftransaction']:
    if row <= 2 and row > 0:
        output2.append('trusted')
    else:
        output2.append('unverified')
output2f = open('paymo_output/output2.txt', 'w')
for item in output2:
    print>>output2f, item
output2f.close()
output3 = []
for row in newpayment['degreeoftransaction']:
    if row <= 4 and row > 0:
        output3.append('trusted')
    else:
        output3.append('unverified')
output3f = open('paymo_output/output3.txt', 'w')
for item in output3:
    print>>output3f, item
output3f.close()


#writes the total output with all three features and the two additional features
totaloutput = []
fraudulentset = newpayment[['countpersec','degreeoftransaction','priceovercap']].values
for row in fraudulentset:
    if row[0] <= 1 and row[1] > 0 and row[1] <= 4 and row[2] == 0:
        totaloutput.append('trusted')
    else:
        totaloutput.append('unverified')
output4f = open('paymo_output/output4.txt', 'w')
for item in totaloutput:
    print>>output4f, item
output4f.close()


#write the total output to a file
newpayment.to_csv(r'output.txt')

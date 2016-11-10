#PLEASE SEE DOCUMENTATION -- CHRISTOPHER MEHFOUD
#BASE SET UP FOR ALL FEATURES WITH INPUT REQUIRED AND OUTPUT POSSIBLE
#TIME OF STEPS GIVEN AS OUTPUT AS WELL
#BUILT FOR GREATER THAN 1 TRANSACTION AT A TIME

import csv
import time
import numpy as np
import pandas as pd
import networkx as nx

#start clock and pull in the historical transactions
t = time.time()
print "Please type the filename of transaction history to be used (default is batch_payment.csv):"
batchfilename = raw_input()
print "Setting up the system"


#initialize the system 
paymentlist1 = [("time", "id1", "id2","amount")]


#call the historical transactions
try:
    f = open(batchfilename, 'rU')
except IOError:
    f = open('batch_payment.csv','rU')


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
elapsed1 = time.time() - t
e1 = elapsed1
print("Time to build a clean historical transaction table", e1)


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


#writes network to a file in case extraction or study required
nx.write_edgelist(g,'test.edgelist',data=['weight'])
elapsed2 = time.time()-t
e2 = elapsed2-elapsed1
print("Time to build network of batch payments",e2)


#Establish a price cap based on sample of history of data for additional feature 1
sample1 = paymentlist.loc[np.random.choice(paymentlist.index, 100, replace=False)]
avg = sample1.mean(axis=0)
avgp = avg[2] #mean
std = sample1.std(axis=0)
stdp = std[2] #standard deviation
pricecap = avgp+2*stdp #zscore


#Writes pricecap so that it can be pulled by other files
with open("pricecap.txt", 'w') as f:
    f.write("%d" % pricecap)
elapsed3 = time.time()-t
e3 = elapsed3-elapsed2
print("Time to write pricecap",e3)

elapsed4 = time.time()-t
e4 = elapsed4-elapsed3
print("Time to set up base table",e4)
    

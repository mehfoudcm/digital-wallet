#PLEASE SEE DOCUMENTATION -- CHRISTOPHER MEHFOUD
#ALL FEATURES WITH INPUT REQUIRED AND OUTPUT POSSIBLE
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



#####FILE NAME CHANGES IF DESIRED#####
#shutil.copy2('paymo_input/batch_payment.txt','paymo_input/batch_payment.csv')
#shutil.copy2('paymo_input/stream_payment.txt','paymo_input/stream_payment.csv')
#os.rename('paymo_input/batch_payment.txt','paymo_input/batch_payment.csv')
#os.rename('paymo_input/stream_payment.txt','paymo_input/stream_payment.csv')



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
#nx.write_edgelist(g,'test.edgelist',data=['weight'])
elapsed2 = time.time()-t
e2 = elapsed2-elapsed1
print("Time to build network of batch payments",e2)


#establishes control on output desired
print "System ready for transactions"
print "Would you like to print output? y or n"
yon = raw_input()
if yon == 'y':
    print "Would you like to print feature1, feature2, feature3, or allfeatures?"
    featprint = raw_input()
else:
    featprint = 'none'
print "Please insert file name of transactions to be streamed (default is stream_payment.csv):"
filename = raw_input()
#removes typing time
elapsed3 = time.time() - t
e3 = elapsed3-elapsed2


#initializes and writes a clean stream payments table
newpayment1 = [("time", "id1", "id2","amount")]
try:
    f = open(filename, 'rU')
except IOError:
    f = open('stream_payment.csv', 'rU')   
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
elapsed4 = time.time() - t
e4 = elapsed4-elapsed3
print("Time to build a clean table of streamed transactions", e4)


#Establish a price cap based on sample of history of data for additional feature 1
sample1 = paymentlist.loc[np.random.choice(paymentlist.index, 100, replace=False)]
avg = sample1.mean(axis=0)
avgp = avg[2] #mean
std = sample1.std(axis=0)
stdp = std[2] #standard deviation
pricecap = avgp+2*stdp #zscore


#Writes pricecap so that it can be pulled by other files
#with open("pricecap.txt", 'w') as f:
#    f.write("%d" % pricecap)
#with open("pricecap.txt") as f:
#    pricecap=float(f.readlines()[0])
elapsed5 = time.time() - t
e5 = elapsed5-elapsed4
print("Time to build or pull the pricecap", e5)


#data built on normal distribution, so we can just pull avg + 2*std from, 24.89+2*10.05
#use z score cap of 44.98, anything over within top 2.5% should be verified
#pricecap = 44.98
highprice = []
for row in newpayment['amount']:
	if row > pricecap:
		highprice.append(1)
	else:
		highprice.append(0)
elapsed6 = time.time() - t
e6 = elapsed6-elapsed5
print("Time to build the amount column", e6)



#finds the total time of the stream
firsttime = newpayment["time"].iloc[0]
firsttime = time.mktime(time.strptime(firsttime, '%Y-%m-%d %H:%M:%S'))
lasttime = newpayment["time"].iloc[-1]
lasttime = time.mktime(time.strptime(lasttime, '%Y-%m-%d %H:%M:%S'))
totaltime = lasttime-firsttime+1


#counts the number of transactions in the stream
count = newpayment['id1'].count()


#sets up count of transactions over time for additional feature 2
newpayment1=newpayment
timing=newpayment1.id2.value_counts().reset_index().rename(columns={'index': 'id2', 0: 'countpersec'})
timing['countpersec'] = timing['countpersec']/(totaltime)
newpayment = newpayment.reset_index().merge(timing, on='id2', how="left").set_index('index')
newpayment.sort_index(inplace=True)   
elapsed7 = time.time() - t
e7 = elapsed7-elapsed6
print("Time to build time the column", e7)


#reads in the graph if it is not already there
#k = nx.read_edgelist('test.edgelist', nodetype=int, data=(('weight',int),))


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
elapsed8 = time.time() - t
e8 = elapsed8-elapsed7
print("Time for each transaction to run through the network", e8)


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
output1f = open('output1.txt', 'w')
for item in output1:
    print>>output1f, item
output1f.close()
output2 = []
for row in newpayment['degreeoftransaction']:
    if row <= 2 & row > 0:
        output2.append('trusted')
    else:
        output2.append('unverified')
output2f = open('output2.txt', 'w')
for item in output2:
    print>>output2f, item
output2f.close()
output3 = []
for row in newpayment['degreeoftransaction']:
    if row <= 4 & row > 0:
        output3.append('trusted')
    else:
        output3.append('unverified')
output3f = open('output3.txt', 'w')
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
output4f = open('output4.txt', 'w')
for item in totaloutput:
    print>>output4f, item
output4f.close()


#write the total output to a file
newpayment.to_csv(r'output.txt')


#write the output desired on screen
if featprint == 'feature1':
    print output1
elif featprint == 'feature2':
    print output2
elif featprint == 'feature3':
    print output3
elif featprint == 'allfeatures':
    print totaloutput
else:
    print "Each output is in a text file in this directory with the correct listing, where output4 contains all features.\n Output.txt is the total output that lists each element by transaction." 


#calculating the significant times and printing
elapsedimportant = e4+e5+e6+e7+e8
runtimeforstream = elapsedimportant
timefortimeframe = elapsedimportant/totaltime
avgtimepertrans = elapsedimportant/count
transpersecond = count/totaltime
elapsed9 = time.time() - t
e9 = elapsed9-elapsed8
print("Time writing of files", e9)
print("Time of the entire process",elapsed9)
print("Run time for processessing transactions",runtimeforstream)
print("Average time per transaction",avgtimepertrans)
print("Seconds in Stream",totaltime)
print("Transactions per second in stream",transpersecond)

import socket
import sys
import time
import threading
import Queue
import SocketServer
import simplejson as json
import os

padding = "blankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblanknkblankblankblankblankblankblankblankblankblankblankblankblanknkblankblankblankblankblankblankblank"

bQueue = Queue.Queue()
mQueue = Queue.Queue()

bSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


SEGMENTS = 20
PACKAGES = 175

count = 0

file = open('DATA/'+time.strftime("%H")+'/BFR.csv','w')
		
# Thread that handles broadcasting of messages
# Runs of off the bQueue
class ThreadBroadcast(threading.Thread):
	def __init__(self, queue):
		threading.Thread.__init__(self)
		self.queue = queue

	def run(self):
		while True:
			msg = self.queue.get()
			bSocket.sendto(msg.toString(0), (msg.dest, 9999))
			self.queue.task_done()

class ThreadTimer(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.started = False
		self.start_time = -1
		
	def run(self):
		self.start_time = time.time()
		while True:
			if not self.started:
				if time.time() - self.start_time >= 2:
					self.started = True
					self.start_time = time.time()
			else:
				if time.time() - self.start_time > 1:
					client.processBuffer()
					self.start_time = time.time()
	
# UDP Server that receives packets
# Received packets and then places it on the ThreadMessageHandle thread
# Uses the mQueue		
class MyUDPHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		global count
		count += 1
		data = self.request[0].strip()
		mQueue.put(data)
	
# Processes incoming messages
# Uses the mQueue			
class ThreadMessageHandle(threading.Thread):
	def __init__(self, queue):
		threading.Thread.__init__(self)
		self.queue = queue

	def run(self):
		while True:
			data = self.queue.get()
			client.handleMessage(data)
			self.queue.task_done()		

# Class that describes the information being sent between the client and server
class Message(object):
	def __init__(self):
		self.type = 0 #Type of message (0 - normal data packet, 1 - message to server to change quality)
		self.time = time.time() # Time packet was created
		self.dest = "" # Hostname of the destination of the packet
		self.segment = -1 # What segment the packet is for
		self.quality = -1 # The quality of the given packet
		self.src = socket.gethostname() # The origin of the packet (ie. the client's hostname)
		self.message = "" # a message that can be sent
		self.number = -1 # a unique identifier for each packet
		
	# Creates Json object from Message object
	def toString(self,amount):
		fill = ""
		for i in xrange(amount):
			fill += padding
		return_string = '{"type":"'+str(self.type)+'","segment":"'+str(self.segment)+'","quality":"'+str(self.quality)+'","message":"'+str(self.message)+'","src":"'+self.src+'","number":"'+ str(self.number)+'","fill":"'+fill+'"}'			
		return return_string
	
	# Creates Message Object from JSON Object
	@classmethod
	def fromString(self,data):
		msg = Message()
		jsonObject = json.loads(data)
		msg.segment = int(jsonObject['segment'])
		msg.quality = int(jsonObject['quality'])
		msg.type = int(jsonObject['type'])
		msg.message = jsonObject['message']
		msg.src = jsonObject['src']
		msg.number = jsonObject['number']
		return msg

# Client Object		
class Client(object):
	def __init__(self):
		self.hostname = socket.gethostname() #hostname of client
		self.quality = [] #quality for each segment
		self.queue = [] #queue holding segments that are received
		self.server = "" #hostname of the server
		self.last_dump_segment = 0 #video segment that was last dumped from the queue
		#self.last_dump_time = -1 #time when the last queue dump occurred
		self.has_received = False # if the program has begun receiving any packets
		self.empty_queues = 0 # number of times the queue was empty. if received 5, assume transmission is over and quit program
		
	# Takes incoming packet and processes it by adding it to the queue
	def handleMessage(self,data):
		if not self.has_received:
				t = ThreadTimer()
				t.setDaemon(True)
				t.start()  
				self.has_received = True
		msg = Message.fromString(data)
		self.server = msg.src
		self.quality.insert(msg.segment,msg.quality)
		self.queue.append(msg.segment)
# 		if self.last_dump_time == -1:
# 			self.last_dump_time = time.time()
# 		elif (time.time() - self.last_dump_time > 1):
# 			self.last_dump_time = time.time()
# 			
# 			self.last_dump_time = time.time()
		
	def processBuffer(self):
		fill_rate = self.getFillRate(self.last_dump_segment)
		next_quality = self.checkNextQuality(fill_rate)
		print str(fill_rate) + "  " + str(self.quality[self.last_dump_segment]) + "  " + str(self.last_dump_segment)
		if fill_rate == 0.0:
			self.empty_queues += 1
			if self.empty_queues == 5:
				print "Length " + str(len(self.queue))
				print "Count " + str(count)
				os.abort()
		else:
			self.empty_queues = 0
		if next_quality != -1:
			oMsg = Message()
			oMsg.type = 1
			oMsg.message = next_quality
			oMsg.dest = self.server
			bQueue.put(oMsg)
		file.write("%f,%d,%d,%f\n" % (fill_rate ,self.last_dump_segment,self.quality[self.last_dump_segment],time.time()))
		self.dumpQueue(self.last_dump_segment)
		self.last_dump_segment += 1
			
	# Gets the fill rate of self.queue for the given segment
	# Counts the number of segments in the queue			
	def getFillRate(self,segment):
		count = 0
		for i in xrange(len(self.queue)):
			if self.queue[i] == segment:
				count += 1
		rate = float(count)/float(PACKAGES)
		return rate
		
	# Determines what the next quality should be given a fill rate
	# Decrease quality if less than 70%
	# Increase quality if greater than 90%
	def checkNextQuality(self,fill_rate):
		if (fill_rate < .70): 
			if self.quality[self.last_dump_segment] == 1:
				return -1
			else:
				return self.quality[self.last_dump_segment] - 1
		elif (fill_rate > .90):
			if self.quality[self.last_dump_segment] == 4:
				return -1
			else:
				return self.quality[self.last_dump_segment] + 1
		else:
			return -1
	
	# Dumps the queue of any instances of a given segment			
	def dumpQueue(self,segment):
		while (self.queue.count(segment) != 0):
			self.queue.remove(segment)	
				
# Creates 50 threads to process incoming messages	
for i in range(5):
	t = ThreadMessageHandle(mQueue)
	t.setDaemon(True)
	t.start()

# Creates 2 threads to broadcast messages back to the server
for i in range(5):
	t = ThreadBroadcast(bQueue)
	t.setDaemon(True)
	t.start()        	       		
	
start = time.time()
client = Client()
server = SocketServer.UDPServer((socket.gethostname(), 5006), MyUDPHandler)
server.serve_forever()
print time.time() - start


	
bQueue.join()

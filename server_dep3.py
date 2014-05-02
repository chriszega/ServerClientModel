import socket
import sys
import time
import threading
import Queue
import SocketServer
import simplejson as json

padding = "blankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblankblanknkblankblankblankblankblankblankblankblankblankblankblankblanknkblankblankblankblankblankblankblank"

bQueue = Queue.Queue()
mQueue = Queue.Queue()

bSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		
		
SEGMENTS = 20
PACKAGES = 150

# Thread that handles broadcasting of messages
# Runs of off the bQueue
class ThreadBroadcast(threading.Thread):
	def __init__(self, queue):
		threading.Thread.__init__(self)
		self.queue = queue

	def run(self):
		while True:
			msg = self.queue.get()
			bSocket.sendto(msg.toString(1*msg.quality), (msg.dest, 5006))
			self.queue.task_done()

# Thread that handles listening of messages
# Creates a thread that runs the UDPSocket Server
# Socket server handles the actual incoming messages
class ThreadListen(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)

	def run(self):
		server = SocketServer.UDPServer((socket.gethostname(), 9999), MyUDPHandler)
		server.serve_forever()
		
# UDP Server that listens for incoming messages
# Once a message is received, it is offloaded to another thread for processing	
class MyUDPHandler(SocketServer.BaseRequestHandler):
	def handle(self):
		data = self.request[0].strip()
		mQueue.put(data)
		
# Thread the processes incoming messages
# Runs off of mQueue
class ThreadMessageHandle(threading.Thread):
	def __init__(self, queue):
		threading.Thread.__init__(self)
		self.queue = queue

	def run(self):
		while True:
			data = self.queue.get()
			msg = Message.fromString(data)
			server.checkMessage(msg)
			self.queue.task_done()		

# see client_dep2.py for info
class Message(object):
	def __init__(self):
		self.type = 0
		self.time = time.time()
		self.dest = ""
		self.segment = -1
		self.quality = -1
		self.src = socket.gethostname()
		self.message = ""
		self.number = -1
		
	def toString(self,amount):
		fill = ""
		for i in xrange(amount):
			fill += padding
		return_string = '{"type":"'+str(self.type)+'","segment":"'+str(self.segment)+'","quality":"'+str(self.quality)+'","message":"'+self.message+'","src":"'+self.src+'","number":"'+ str(self.number)+'","fill":"'+fill+'"}'			
		return return_string
	
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

# Simple client object that is used by the server to keep track of who it is broadcasting to	
class Client(object):
	def __init__(self,hostname):
		self.hostname = hostname # hostname of the client
		self.quality = 1 #current quality of the client
		
# Server object
class Server(object):
	def __init__(self):
		self.clients = [] #stores the clients of the server
		self.current_segment = 0 
	
	def run(self):
		for h in xrange(len(self.clients)): #loops through all client
			client = self.clients[h]
			dest = client.hostname
			for i in xrange(SEGMENTS): # loops through each segment
				quality = client.quality # set quality to a separate variable instead of using object variable to prevent the value changing mid segment
				for j in xrange(PACKAGES): # loops through each packet
					start_time = time.time()
				
					# create message to send
					msg = Message()
					msg.dest = dest
					msg.segment = i
					msg.quality = quality
					msg.number = j
					
					bQueue.put(msg)
					while (time.time() - start_time < float(1)/float(PACKAGES)): # wait for a specific time so 1 segment takes 1 second
						pass
			time.sleep(10)
						
	# Checks the incoming message from a client
	def checkMessage(self,msg):
		print "Received " + str(msg.type) + "  "  + str(msg.src) + "  " + str(msg.message)
		if int(msg.type) == 1:	# if type == 1, it is a message to change quality
			for x in range(len(self.clients)): # loop through all the clients looking for a client that matches the message's origin
				if self.clients[x].hostname == msg.src:
					if (int(msg.message) > 0 and int(msg.message) < 5): #if the quality is valid, set the client's quality to the quality set within the message
						self.clients[x].quality = int(msg.message)
			
# Create 20 threads to handle incoming messages				
for i in range(20): 
	t = ThreadMessageHandle(mQueue)
	t.setDaemon(True)
	t.start()

# Create 50 threads to handle broadcasting messages
for i in range(50):
	t = ThreadBroadcast(bQueue)
	t.setDaemon(True)
	t.start()     
	       		
# Create one thread to run the UDP server off the main thread
for i in range(1):
	t = ThreadListen()
	t.setDaemon(True)
	t.start()



server = Server()

# loop through the arguments,where each argument is a client
for x in range(1,len(sys.argv)):
	client = Client(sys.argv[x])
	server.clients.append(client)
	
start = time.time()

server.run()

print time.time() - start

# wait to make sure no packets are left to be sent
time.sleep(10)

	


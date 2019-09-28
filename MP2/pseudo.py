class Node:
	def __init__(self):
		self.monitors = []
		self.members_map = 
		self.heartbeat_status = 
		self.introducer =  
		self.finger_table = 

	def send_heartbeat():
		for i in self.monitors:
			monitor_conn.send("HEARTBEAT #count")

	def receive_heartbeat():
		# listen to heartbeat conn port and update heartbeat status

	def receive():
		message = conn.receive()
		message_type, virtual_position, ip, event_count = # extract message type
		# if you have already processed, no further action
		disseminate(message)
		switch(message_type):
			type 'JOIN':

			type 'LEAVE':
				mark entry as dead
			type 'CRASH':
				mark entry as dead

	def disseminate(message):
		for i in self.finger_table:
			iconn.send(message)

	def track_targets():
		# runs a check periodically
		hb_period = 2
		for i in self.heartbeat_status:
			if an entry is stale (2 hb periods it wasn't updated)

			check_suspicion()

	def check_suspicion(vid_node):
		get_neighbors from the members_map
		and send them a query 'SUSPECT VIRTUAL_POSITION IP'
		# if no positive response to SUSPECT, update member_map
		message = 'CRASH, vid, ip, count'
		disseminate(message)

message = TYPE, VIRTUAL_POSITION, IP, COUNT
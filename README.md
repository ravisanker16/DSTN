# Data Storage Technologies and Networks
Building a Distributed Data Storage System.

## Kafka Topics


 1. A topic for each storage node whose name is decided by the storage node. The head node gets this topic name when the storage first exchanges a packet with the head node.
 2.  ***initial*** <String, byte[]>: Group-1 sends data on this topic. Only the head node consumes from it. Key is the name of the image and value is the byte array of the image.
 3. ***meta*** <no-key, byte[]>: Head node produces to it and storage node number 0 consumes from it, which acts as a backup when head node goes down. Value is the byte array serialised version of the metadata json file.
 4. ***request-topic*** <no-key, String>: Group-1 sends the request for image on this topic. Group-1 produces to this and the head node consumes from it. Value is the name of the requested image by Group-1.
 5. ***image-topic*** <String, byte[]>: Group-1 consumes from it and Head node produces to it. This is the image that Group-1 requested for. Key is the name of the requested image and value ithe byte array of the requested image.
 6. ***img-req-for-storage-node*** <String, String>: Head node produces to this and all the storage nodes consume from this. Key is the name of requested image. Value is the topic name to which the head node had produced this image to.
 7. ***img-from-storage-node*** <String, byte[]>: Head node consumes from this and all the storage nodes produce to it.  Key is name of the requested image and the value is the byte array of the image.

## Flow

 1. Group-1 produces the image to ***initial***.
 2. Head Node consumes from ***initial*** and with its distribution algorithm produces to the appropriate storage node topic. This process is repeated for the backup.
 3. Group-1 requests for an image on ***request-topic***.
 4. Head Node consumes from ***request-topic***, finds out which storage node has the image and produces to ***img-req-for-storage-node*** where the key will be name of requested image and value will be the name of the topic assigned this storage node.
 5. The storage node consumes from ***img-req-for-storage-node***, reads the value and notices its meant for itself. It reads the image and produces it to ***img-from-storage-node***.
 6. Head Node consumes from ***img-from-storage-node***, updates some internal data structure, and produces it to ***image-topic*** from which Group-1 can consume.
 7. In case there is no response, the Head Node with the help of its internal data structures, sends the request to either the storage node again or the backup storage node for the requested image.

## Fault Tolerance

### Handling of storage node failure

#### Storing of Data
- Each storage node sends a heartbeat every specified period of time to the head node. The heart beat also contains the data of which images the storage node has received since the last heart beat.
- When the head node consumes from *initial* topic, it finds an appropriate topic to send the image and its backup to. The metadata is not updated yet. The metadata is only updated with an acknowledgement through the heartbeat.
- If the head node does not receive a heartbeat within some predefined period of time for a storage node, that storage node is invalidated. This is done by maintaining a HashMap for each storage node.

#### Addressing Requests
- When a request comes to the head node, the metadata.json file is read to check which storage node the requested image is stored at. The validity of the storage node is checked. If it's not valid, request is sent to the backup node.
- **What happens if the head node has made a request but no response?**
	- For every request, the head node maintains a time stamp. Let's call this the requestTimeStampMap. This maps the  requested image name to the time stamp at which the request was received by the head node.
	- This map is periodically gone through, if the request was made 60s (as an example) ago, validity is checked and request is sent again. The map is updated with the latest time stamp.
	- If an image has been received by the head node, the head node removes its entry from map.
- **What happens if a storage node goes down and it's started again?**
		 - We have a boolean attribute in ProfilePacket called firstTime. This can be set to false.

### Handling of head node failure
- The first storage node to join is assumed to be the buddy node. This is hard-coded. Metadata from the Head Node is being periodically being produced to the ***meta*** topic which the buddy node consumes from.
- The storage node expects an acknowledgement from the head node for each heart beat it sends. If there is no ack for two consecutive heartbeats, the buddy assumes the head node is dead and takes over.
- If there is no ack for three consecutive heartbeats, the other storage nodes assumes the head node is dead and connects to the hard-coded buddy's IP address.
- Group-1 is unaware of this as they are only connected to the Kafka Cluster. We are assuming the kafka cluster is fault-tolerant.

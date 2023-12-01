# DSTN
Building a Distributed Data Storage System

## Kafka Topics


 1. A topic for each storage node whose name is decided by the storage node. The head node gets this topic name when the storage first exchanges a packet with the head node.
 2.  ***initial*** <String, byte[]>: Group-1 sends data on this topic. Only the head node consumes from it. Key is the name of the image and value is the byte array of the image.
 3. ***meta*** <no-key, byte[]>: Head node produces to it and storage node number 0 consumes from it, which acts as a backup when head node goes down. Value is the byte array serialised version of the metadata json file.
 4. ***request-topic*** <no-key, String>: Group-1 sends the request for image on this topic. Group-1 produces to this and the head node consumes from it. Value is the name of the requested image by Group-1.
 5. ***image-topic*** <String, byte[]>: Group-1 consumes from it and Head node produces to it. This is the image that Group-1 requested for. Key is the name of the requested image and value ithe byte array of the requested image.
 6. ***img-req-for-storage-node*** <String, String>: Head node produces to this and all the storage nodes consume from this. Key is the name of requested image. Value is the topic name to which the head node had produced this image to.
 7. ***img-from-storage-node*** <String, byte[]>: Head node consumes from this and all the storage nodes produce to it.  Key is name of the requested image and the value is the byte array of the image.

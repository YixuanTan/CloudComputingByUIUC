/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	ring = curMemList;

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

MessageBase * MP2Node::createMessageBase(MessageType type, string key, string value){
	//Message *message = (Message *) malloc(sizeof(Message));
	MessageBase *messagebase = new MessageBase();
	messagebase->id = g_transID++;
	messagebase->total = 0;
	messagebase->success = 0;
	messagebase->currtime = par->getcurrtime();
	messagebase->type = type;
	messagebase->key = key;
	messagebase->value = value;
	msg_list[messagebase->id] = messagebase;
	return messagebase;
}

void MP2Node::dispatchMsg(MessageBase *messagebase) {
	Message *msg;
	vector<Node> replicas = findNodes(messagebase->key);
	if (replicas.size() == 3) {
		// send to first replica
		msg = new Message(messagebase->id, memberNode->addr, messagebase->type, messagebase->key, messagebase->value, PRIMARY);
		emulNet->ENsend(&memberNode->addr, &replicas.at(0).nodeAddress, msg->toString());
		delete msg;

		// send to second replica
		msg = new Message(messagebase->id, memberNode->addr, messagebase->type, messagebase->key, messagebase->value, SECONDARY);
		emulNet->ENsend(&memberNode->addr, &replicas.at(1).nodeAddress, msg->toString());
		delete msg;

		// send to third replica
		msg = new Message(messagebase->id, memberNode->addr, messagebase->type, messagebase->key, messagebase->value, TERTIARY);
		emulNet->ENsend(&memberNode->addr, &replicas.at(2).nodeAddress, msg->toString());
		delete msg;
	}
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	 dispatchMsg(createMessageBase(CREATE, key, value));
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	 dispatchMsg(createMessageBase(READ, key, ""));
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	 dispatchMsg(createMessageBase(UPDATE, key, value));
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	 dispatchMsg(createMessageBase(DELETE, key, ""));
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(int id, string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	if (ht->create(key, value)) {
		log->logCreateSuccess(&memberNode->addr, false, id, key, value); // log success
		return true;
	} else {
		log->logCreateFail(&memberNode->addr, false, id, key, value); // log failure
		return false;
	}
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(int id, string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string value = ht->read(key);
	if (value != "") {
		log->logReadSuccess(&memberNode->addr, false, id, key, value); // log success
	} else {
		log->logReadFail(&memberNode->addr, false, id, key); // log failure
	}
	return value;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(int id, string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if (ht->update(key, value)) {
		log->logUpdateSuccess(&memberNode->addr, false, id, key, value); // log success
		return true;
	} else {
		log->logUpdateFail(&memberNode->addr, false, id, key, value); // log failure
		return false;
	}
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deleteKey(int id, string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	if (ht->deleteKey(key)) {
		log->logDeleteSuccess(&memberNode->addr, false, id, key); // log success
		return true;
	} else {
		log->logDeleteFail(&memberNode->addr, false, id, key); // log failure
		return false;
	}
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		Message *msgRcvd = new Message(string(data, data + size));

		/*
		 * Handle the message types here
		 */

		Message *msgSend;
    		switch(msgRcvd->type) {
			case CREATE:
				msgSend = new Message(msgRcvd->transID, memberNode->addr, REPLY, false);
				msgSend->success = createKeyValue(msgRcvd->transID, msgRcvd->key, msgRcvd->value, msgRcvd->replica);
				// send reply back
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case READ:
			msgSend = new Message(msgRcvd->transID, memberNode->addr, READREPLY, "");
				msgSend->value = readKey(msgRcvd->transID, msgRcvd->key);
				// send reply back
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case UPDATE:
				msgSend = new Message(msgRcvd->transID, memberNode->addr, REPLY, false);
				msgSend->success = updateKeyValue(msgRcvd->transID, msgRcvd->key, msgRcvd->value, msgRcvd->replica);
				// send reply back
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case DELETE:
				msgSend = new Message(msgRcvd->transID, memberNode->addr, REPLY, false);
				msgSend->success = deleteKey(msgRcvd->transID, msgRcvd->key);
				// send reply back
				emulNet->ENsend(&memberNode->addr, &msgRcvd->fromAddr, msgSend->toString());
				delete msgSend;
				break;
			case REPLY:
				processReply(msgRcvd->transID, msgRcvd->success, "");
				break;
			case READREPLY:
				processReply(msgRcvd->transID, false, msgRcvd->value);
				break;
		}
		delete msgRcvd;
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	 for(auto const& it : msg_list) {
		 checkTimeout(msg_list[it.first], par->getcurrtime());
	 }

}

void MP2Node::processReply(int id, bool success, string value){
	MessageBase *messagebase;

	if (msg_list.find(id) != msg_list.end()) {
		messagebase=msg_list[id];
	} else {
		// id not found ?!
		return;
	}

	messagebase->total++;
	if (success) { // CREATE, UPDATE or DELETE
		messagebase->success++;
	}
	if (value != "") { // READ
		messagebase->success++;
		messagebase->value = value;
	}
	checkQuorum(messagebase);
}

void MP2Node::checkTimeout(MessageBase *messagebase, int cur_time){
	if ((cur_time - messagebase->currtime) > 3) {
		messagebase->total++;
		checkQuorum(messagebase);
	}
}

void MP2Node::checkQuorum(MessageBase *messagebase){
	if (messagebase->success == 2) {
		switch(messagebase->type) {
			case CREATE:
				log->logCreateSuccess(&memberNode->addr, true, messagebase->id, messagebase->key, messagebase->value);
				break;
			case READ:
				log->logReadSuccess(&memberNode->addr, true, messagebase->id, messagebase->key, messagebase->value);
				break;
			case UPDATE:
				log->logUpdateSuccess(&memberNode->addr, true, messagebase->id, messagebase->key, messagebase->value);
				break;
			case DELETE:
				log->logDeleteSuccess(&memberNode->addr, true, messagebase->id, messagebase->key);
				break;
			default:
				break;
		}
		msg_list.erase(messagebase->id);
		delete(messagebase);
	} else if (messagebase->total == 3 && messagebase->success < 2) {
		switch(messagebase->type) {
			case CREATE:
				log->logCreateFail(&memberNode->addr, true, messagebase->id, messagebase->key, messagebase->value);
				break;
			case READ:
				log->logReadFail(&memberNode->addr, true, messagebase->id, messagebase->key);
				break;
			case UPDATE:
				log->logUpdateFail(&memberNode->addr, true, messagebase->id, messagebase->key, messagebase->value);
				break;
			case DELETE:
				log->logDeleteFail(&memberNode->addr, true, messagebase->id, messagebase->key);
				break;
			default:
				break;
		}
		msg_list.erase(messagebase->id);
		delete(messagebase);
	}
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */

	int i;
	for(i = 0; i < ring.size(); i++){
		if(ring.at(i).nodeAddress == memberNode->addr){
			break;
		}
	}

	vector<Node> r = {ring[(i+1) % ring.size()], ring[(i+2) % ring.size()]}; // first replica and second replica

	if (hasMyReplicas.size() < 2 ||
		  !(r[0].nodeAddress == hasMyReplicas[0].nodeAddress &&
			  r[1].nodeAddress == hasMyReplicas[1].nodeAddress)) {
		for(auto& it : ht->hashTable) {
			dispatchMsg(createMessageBase(CREATE, it.first, it.second));
		}
		hasMyReplicas = r;
	}
}

/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 *        Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <sstream>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 *        This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 *        All initializations routines for a member.
 *        Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int*)(&memberNode->addr.addr);
    int port = *(short*)(&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode, id, port);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == strcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + sizeof(memberNode->addr.addr) + 1, &memberNode->heartbeat, sizeof(long));
        //stringstream log_msg;
        //log_msg << "Writing heartbeat " << memberNode->heartbeat;
        //log->LOG(&memberNode->addr, log_msg.str().c_str());

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
     * Your code goes here
     */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 *        Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    /*
     * Your code goes here
     */

    assert(size >= sizeof(MessageHdr));

    MessageHdr* msg = (MessageHdr*) data;
    Address *src_addr = (Address*)(msg+1);

    size -= sizeof(MessageHdr) + sizeof(Address) + 1;
    data += sizeof(MessageHdr) + sizeof(Address) + 1;

    switch (msg->msgType) {
        case JOINREQ:
            onJoin(src_addr, data, size);
            onHeartbeat(src_addr, data, size);
            break;
        case PING: 
            onHeartbeat(src_addr, data, size);
            break;
        case JOINREP:
            {
            memberNode->inGroup = 1;
            stringstream msg;
            msg << "JOINREP from " <<  src_addr->getAddress();
            msg << " data " << *(long*)(data );
            log->LOG(&memberNode->addr, msg.str().c_str());
            onHeartbeat(src_addr, data, size);
            break;
            }
        default:
            log->LOG(&memberNode->addr, "Received other msg");
            break;
    }
}

Address AddressFromMLE(MemberListEntry* mle) {
    Address a;
    memcpy(a.addr, &mle->id, sizeof(int));
    memcpy(&a.addr[4], &mle->port, sizeof(short));
    return a;
}

void MP1Node::onJoin(Address* addr, void* data, size_t size) {
    MessageHdr* msg;
    size_t msgsize = sizeof(MessageHdr) + sizeof(memberNode->addr) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    msg->msgType = JOINREP;

    memcpy((char *)(msg+1), &memberNode->addr, sizeof(memberNode->addr));
    memcpy((char *)(msg+1) + sizeof(memberNode->addr) + 1, &memberNode->heartbeat, sizeof(long));
    
  stringstream ss;
  ss<< "Sending JOINREP to " << addr->getAddress() <<" heartbeat "<<memberNode->heartbeat;
  log->LOG(&memberNode->addr, ss.str().c_str());
  emulNet->ENsend(&memberNode->addr, addr, (char *)msg, msgsize);
  free(msg);
}

void MP1Node::onHeartbeat(Address* addr, void* data, size_t size) {
    std::stringstream msg;
    assert(size >= sizeof(long));
    long *heartbeat = (long*)data;

    //msg << "Heartbeat from " << addr->getAddress() << ": ";
    //msg << *heartbeat;
    //log->LOG(&memberNode->addr, msg.str().c_str());
    //msg.str("");

    bool newData = UpdateMemberList(addr, *heartbeat);
    if (newData) {
        LogMemberList();
        SendHBSomewhere(addr, *heartbeat);
    } else {
        //log->LOG(&memberNode->addr, "Heartbeat up-to-date.");
    }
}

bool MP1Node::UpdateMemberList(Address *addr, long heartbeat)  {
    vector<MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
        if ((AddressFromMLE(&(*it)) == *addr) == 0) {
            if (heartbeat > it->getheartbeat()) {
                it->setheartbeat(heartbeat);
                it->settimestamp(par->getcurrtime());
                return true;
            } else {
                return false;
            }
        }
    }
    MemberListEntry mle(*((int*)addr->addr),
              *((short*)&(addr->addr[4])),
              heartbeat,
              par->getcurrtime());
    memberNode->memberList.push_back(mle);
    log->logNodeAdd(&memberNode->addr, addr);
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 *        the nodes
 *        Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    int timeout = 5;

    stringstream ss;
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
      //ss << "Current: " << par->getcurrtime() << " ts: " << it->timestamp << " id: "<< it->id;
        //log->LOG(&memberNode->addr, ss.str().c_str());
      //ss.str("");
      if (par->getcurrtime() - it->timestamp > timeout) { // if time out happens
        Address addr = AddressFromMLE(&(*it)); // get the 
        ss << "Timing out " << addr.getAddress();
        log->LOG(&memberNode->addr, ss.str().c_str());
        ss.str("");
        
        vector<MemberListEntry>::iterator next_it = it;
        vector<MemberListEntry>::iterator next_next_it = it+1;
        for (next_it = it; next_next_it != memberNode->memberList.end(); next_it++, next_next_it++) {
          *next_it = *next_next_it;
        }
        memberNode->memberList.resize(memberNode->memberList.size()-1);
        it -= 1;
        LogMemberList();
        log->logNodeRemove(&memberNode->addr, &addr);
      }
  }
    /*
     * Your code goes here
     */
    //log->LOG(&memberNode->addr, "Looping.");

    UpdateMemberList(&memberNode->addr, ++memberNode->heartbeat);

    SendHBSomewhere(&memberNode->addr, 
            memberNode->heartbeat);
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

//MemberListEntry MLEFromAddr(Address* addr) {
//  MemberListEntry mle(0, 0);
//  memcpy(&mle.id, addr->addr, sizeof(int));
//  memcpy(&mle.port, addr->addr + 4, sizeof(short));
//  return mle;
//}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode, int id, short port) {
    memberNode->memberList.clear();
    MemberListEntry mle = MemberListEntry(id, port);
    mle.settimestamp(par->getcurrtime());
    mle.setheartbeat(memberNode->heartbeat);
    memberNode->memberList.push_back(mle);
}

void MP1Node::LogMemberList() {
  stringstream msg;
  msg << "[";
  for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
    msg << it->getid() << ": " << it->getheartbeat() << "(" << it->gettimestamp() << "), ";
  }
  msg << "]";
  //log->LOG(&memberNode->addr, msg.str().c_str());
}

void MP1Node::SendHBSomewhere(Address *src_addr, long heartbeat) {
    int k = 30;
  double prob = k / (double)memberNode->memberList.size();

  MessageHdr *msg;
  
    size_t msgsize = sizeof(MessageHdr) + sizeof(src_addr->addr) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));
    
    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = PING;
    memcpy((char *)(msg+1), src_addr->addr, sizeof(src_addr->addr));
    memcpy((char *)(msg+1) + sizeof(src_addr->addr) + 1, &heartbeat, sizeof(long));
    
  for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
      Address dst_addr = AddressFromMLE(&(*it));
      if ((dst_addr == memberNode->addr) == 0 ||
        ((dst_addr == *src_addr) == 0)) {
          continue;
      }
      if ((((double)(rand() % 100))/100) < prob) { // randomly send out gossip msg

        //stringstream ss;
        //ss<< "Relaying hb about " << src_addr->getAddress() << " to " << dst_addr.getAddress();
        //log->LOG(&memberNode->addr, ss.str().c_str());
        emulNet->ENsend(&memberNode->addr, &dst_addr, (char *)msg, msgsize);
      } else {
        //log->LOG(&memberNode->addr, "Not relaying hb");
      }
  }
    free(msg);
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
            addr->addr[3], *(short*)&addr->addr[4]) ;    
}
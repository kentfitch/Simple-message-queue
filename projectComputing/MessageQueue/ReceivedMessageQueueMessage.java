package projectComputing.MessageQueue ; 

/** 
  ReceivedMessageQueueMessage - a message returned by a MessageQueueReader from a MessageQueue
  2012 John Evershed, Kent Fitch, www.projectcomputing.com
**/

import java.math.*;

public class ReceivedMessageQueueMessage {

	final byte id[] ;
	final byte contents[] ;
	final boolean possiblyReplayed ;

	ReceivedMessageQueueMessage(byte id[], byte contents[], boolean possiblyReplayed) {

		this.id = id ;
		this.contents = contents ;
		this.possiblyReplayed = possiblyReplayed ;
	}

	String idAsString() {

		return new BigInteger(1, id).toString(16) ;
	}

	String contentsAsString() {

		return new String(contents) ;
	}
}

package projectComputing.MessageQueue ; 

/** 
  ReceivedMessageQueueMessage - a message returned by a MessageQueueReader from a MessageQueue
  2012 John Evershed, Kent Fitch, www.projectcomputing.com
**/

import java.math.*;

public class ReceivedMessageQueueMessage {

	public final byte id[] ;
	public final byte contents[] ;
	public final boolean possiblyReplayed ;

	ReceivedMessageQueueMessage(byte id[], byte contents[], boolean possiblyReplayed) {

		this.id = id ;
		this.contents = contents ;
		this.possiblyReplayed = possiblyReplayed ;
	}

	public String idAsString() {

		return new BigInteger(1, id).toString(16) ;
	}

	public String contentsAsString() {

		return new String(contents) ;
	}
}

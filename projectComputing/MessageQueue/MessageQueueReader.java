package projectComputing.MessageQueue ; 

/** 
  MessageQueueReader - used by sinks of messages from a MessageQueue
  2012 John Evershed, Kent Fitch, www.projectcomputing.com

  use like this:

	MessageQueueReader mqReader = new MessageQueueReader(serverAddr, serverPort) ;
	...	

	while (true) {
		...

		ReceivedMessageQueueMessage message = mqReader.read() ;

		System.out.println("Received message id: " + message.idAsString() + ", contents: " + message.contentsAsString() + 
			", possibly replayed:" + message.possiblyReplayed) ;

		// to get id as 16-byte array, use message.messageId; to get contents as byte array, use message.contents
		..
	}
	mqReader.close() ;
**/

import java.io.*;
import java.net.*;
import java.nio.* ;

public class MessageQueueReader {

	Socket server ;
	final BufferedInputStream bis ;
	final BufferedOutputStream bos ;
	final ByteBuffer lenBuffer ;
	int count = 0 ;

	public MessageQueueReader(String serverAddr, int serverSinkPort) throws Exception {

		this.server = new Socket(serverAddr, serverSinkPort) ;
		this.bis = new BufferedInputStream(server.getInputStream()) ;
		this.bos = new BufferedOutputStream(server.getOutputStream()) ;

		this.lenBuffer = ByteBuffer.allocate(4) ;
	}

	public ReceivedMessageQueueMessage read() throws Exception {

			byte b = (byte) bis.read() ;
			if (b < 0) throw new Exception("Unexpected eof received from message queue") ;

			boolean possiblyReplayed ;
			if (b == 'M') possiblyReplayed = false ;
			else if (b == 'R') possiblyReplayed = true ;
			else throw new Exception("Unexpected message type from message queue: " + b) ;

			byte[] id = readBytes(16) ;
			int len = readLen() ;
			byte[] contents = readBytes(len) ;
			
			bos.write('Y') ;
			bos.flush() ;
			count++ ;
			return new ReceivedMessageQueueMessage(id, contents, possiblyReplayed) ;
	}

	public void close() throws Exception { 

		bis.close() ;	
		bos.close() ;
		server.close() ;
		server = null ;
	}

	public String toString() {

		return ((server != null) ? ("Connected to message queue " + server.getRemoteSocketAddress().toString()) : "Not connected") + ", received count:" + count ;
	}

	int readLen() throws Exception {

		for (int i=0;i<4;i++) {
			int j = bis.read() ;
			if (j < 0) throw new Exception("Eof reading message length") ;
			lenBuffer.put(i, (byte) j) ;
		}
		return lenBuffer.getInt(0) ;
	}

	byte[] readBytes(int sz) throws Exception {

		final byte buf[] = new byte[sz] ;
		int start = 0 ;
		int len = sz ;
		while (len > 0) {
			int i = bis.read(buf, start, len) ;
			if (i < 0) throw new Exception("Eof reading message") ;
			start += i ;
			len -= i ;
		}
		return buf ;
	}
}	

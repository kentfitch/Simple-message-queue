package projectComputing.MessageQueue ; 

/** 
  MessageQueueWriter - used by sources of messages for MessageQueue
  2012 John Evershed, Kent Fitch, www.projectcomputing.com

  use like this:
	MessageQueueWriter mqWriter = new MessageQueueWriter(serverAddr, serverPort) ;

	byte[] contents ;
	byte[ id = new byte[16] ;	// only if generating message ids

	...	
	while (moreToSend) {

		...
		// if generating and sending your own 16 byte message ids

		mqWriter.write(id, contents) ;

		// else 
		mqWriter.write(contents) ;
		..
	}
	mqWriter.close() ;
**/

		
import java.io.*;
import java.net.*;
import java.nio.* ;

public class MessageQueueWriter {

	Socket server ;
	final BufferedInputStream bis ;
	final BufferedOutputStream bos ;
	final ByteBuffer lenBuffer ;
	int count = 0 ;

	public MessageQueueWriter(String serverAddr, int serverPort) throws Exception {

		this.server = new Socket(serverAddr, serverPort) ;
		this.bis = new BufferedInputStream(server.getInputStream()) ;
		this.bos = new BufferedOutputStream(server.getOutputStream()) ;

		this.lenBuffer = ByteBuffer.allocate(4) ;
	}

	public void write(byte[] message) throws Exception {

		write(null, message) ;
	}

	public void write(byte id[], byte[] message) throws Exception {

		if ((message == null) || (message.length == 0)) throw new Exception("no message supplied") ;

		if (id == null)	bos.write('M') ;
		else {
			if (id.length != 16) throw new Exception("id must be null or 16 bytes") ;
			bos.write('I') ;
			bos.write(id) ;
		}
		
		byte[] lenBytes = lenBuffer.putInt(0, message.length).array() ;
		bos.write(lenBytes) ;

		bos.write(message) ;
		bos.flush() ;

		if (bis.read() != 'Y') throw new Exception("Message not acked") ;
		count++ ;
	}

	public void close() throws Exception { 

		bos.write('E') ;
		bos.flush() ;
		bis.close() ;	
		bos.close() ;
		server.close() ;
		server = null ;
	}

	public String toString() {

		return ((server != null) ? ("Connected to " + server.getRemoteSocketAddress().toString()) : "Not connected") + ", sent count:" + count ;
	}
}		

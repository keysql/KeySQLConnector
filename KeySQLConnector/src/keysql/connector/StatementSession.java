package keysql.connector;

import java.util.concurrent.CompletableFuture;

final class StatementSession {
	private static final String SELECT_INDENT = "  ";
	private final String request;
	private final CompletableFuture<ReplyMessage> promise;
	private MessageHeader header;
	private StringBuffer replyBuffer;
	private long readSize;
	private long readBytes;
	public StatementSession(int userId, long id, String request, boolean jsonReply, CompletableFuture<ReplyMessage> promise) {
		this.request = request;
		this.promise = promise;
		this.header = new MessageHeader(request.length(),userId, id);
		this.header.setMessageFormat(jsonReply ? "json" : "keysql");
		this.replyBuffer = new StringBuffer((int)header.getMessageLength());
		this.readSize = 0;
		this.readBytes = 0;
	}
	public void setPromise(boolean formatedReply) {
		if (!promise.isCancelled()) {
			promise.complete(new ReplyMessage(header, formatedReply ? getFormatedReply() : replyBuffer.toString()));
		}
	}
	public void setPromise() {
		setPromise(false);
	}
	public boolean promiseCancelled() {
		return promise.isCancelled();
	}
	public String getRequest() {
		return request;
	}
	public void setMessageHeader(MessageHeader header) {
		this.header = header;
	}
	public MessageHeader getMessageHeader() {
		return header;
	}
	public StringBuffer getReplyBuffer() {
		return replyBuffer;
	}
	public void appendReply(final String chunk, long chunkSize) {
		replyBuffer.append(chunk);
		this.readBytes += chunkSize; 
		this.readSize += chunk.length();
	}
	public boolean complete() {
		if (header.getMessageLength() <= this.readBytes) {
			setPromise(true); // parameter true (formated reply) for keyark/query demo only!
			                  // must set as query parameter in general
			return true;
		}
		return false;
	}
	private String getFormatedReply() {
		if (ExitStatus.EXIT_SUCCESS.valueOf() == header.getExitCode() && 
				header.getStatementType() == StatementType.SELECT)
		{
			int ownLength = replyBuffer.length() - (int)this.readSize;
			return replyBuffer.substring(0, ownLength) + 
					ConnectorHelper.formatSelectReply(replyBuffer.substring(ownLength),SELECT_INDENT, header.jsonReply())
					 + header.getResultLine();
		}
		return replyBuffer.append(header.getResultLine()).append('\n').toString();
	}
}

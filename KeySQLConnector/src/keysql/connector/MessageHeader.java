package keysql.connector;

import java.math.RoundingMode;
import java.text.DecimalFormat;

final class MessageHeader {
	long messageLength;
	int userId;
	long clientMessageId;
	TransactionCommand command = TransactionCommand.EXECUTE;
	short exitCode;
	StatementType type = StatementType.UNKNOWN;
	long instanceCount = 0;
	double elapsedTime = -1.0;
	int page = 0;
	String messageFormat = null; // keysql/ json
	String encoding = null;
	
	MessageHeader(long len, int userId, long id) {
		this.messageLength = len;
		this.userId = userId;
		this.clientMessageId = id;
	}
	MessageHeader(long len, int userId, long id, TransactionCommand command) {
		this.messageLength = len;
		this.userId = userId;
		this.clientMessageId = id;
		this.command = command;
	}
	MessageHeader(short error) {
		this.exitCode = error;
		this.messageLength = -1;
		this.clientMessageId = -1;
	}
	MessageHeader(String message) {
		String[] parts = message.substring(0, message.indexOf('\n')).split(" ");
		if (parts.length > 0)
			messageLength = Long.valueOf(parts[0]);
		if (parts.length > 1)
			userId = Integer.valueOf(parts[1]);
		if (parts.length > 2)
			clientMessageId = Long.valueOf(parts[2]);
		if (parts.length > 3)
			command = setValue(Integer.valueOf(parts[3]).intValue());
		if (parts.length > 4)
			exitCode = Short.valueOf(parts[4]);
		for (int i = 5; i < parts.length; i += 2) {
			if (i == parts.length - 1)
				break;
			String prompt = parts[i];
			String value = parts[i + 1];
			
			switch(prompt) {			
			case "type":
				type = StatementType.fromInteger(Integer.valueOf(value));
				break;
			case "count":
				instanceCount = Long.valueOf(value);
				break;
			case "page":
				page = Integer.valueOf(value);
				break;
			case "format":
				messageFormat = value;
				break;
			case "encoding":
				encoding = value;
				break;
			case "time":
				elapsedTime = Double.valueOf(value);
				break;
			}
		}
	}
	short getExitCode() {
		return exitCode;
	}
	long getMessageLength() {
		return messageLength;
	}
	int getUserId() {
		return userId;
	}
	long getClientMessageId() {
		return clientMessageId;
	}
	StatementType getStatementType() {
		return type;
	}
	long getInstanceCount() {
		return instanceCount;
	}
	int getPage() {
		return page;
	}
	String getMessageFormat() {
		return messageFormat;
	}
	void setMessageFormat(String messageFormat) {
		this.messageFormat = messageFormat;
	}
	String getEncoding() {
		return encoding;
	}
	void setEncoding(String encoding) {
		this.encoding = encoding;
	}
	boolean jsonReply() {
		return messageFormat != null && messageFormat.equals("json");
	}
	double getElapsedTime() {
		return elapsedTime;
	}
	private TransactionCommand setValue( int value) {
      switch(value){
        case 0: return TransactionCommand.BEGIN;
        case 2: return TransactionCommand.ABORT;
        default:
             return TransactionCommand.EXECUTE;
      }
   }
	@Override
	public String toString() {
		StringBuilder r = new StringBuilder(messageLength + " " + userId + " " + clientMessageId + " " +  
				command.valueOf() + " " + exitCode);
		if (type != StatementType.UNKNOWN) {
			r.append(" type " + type.getValue());
		}
		if (instanceCount >=  0) {
			r.append(" count " + instanceCount);
		}
		if (page > 0) {
			r.append(" page " + page);
		}
		if (messageFormat != null && !messageFormat.isEmpty()) {
			r.append(" format " + messageFormat);
		}
		if (encoding != null && !encoding.isEmpty()) {
			r.append(" encoding " + encoding);
		}
		return r.append('\n').toString();
	}
	public String getResultLine() {
		if (ExitStatus.EXIT_SUCCESS.valueOf() != exitCode)
			return "";
		DecimalFormat df = new DecimalFormat("#.##");
		df.setRoundingMode(RoundingMode.HALF_DOWN);
		String elapsed = "(" +df.format(elapsedTime) + " sec)";
		switch (type) {
		case SELECT:
			return instanceCount 
					+ " instance" 
					+ (instanceCount == 1 ? "" : "s") 
					+  " in result store " 
					+ elapsed;

		case INSERT:
		case UPDATE:
		case DELETE:
			return "KeySQL query OK, "
					+ instanceCount 
					+ " instance" 
					+ (instanceCount == 1 ? "" : "s") 
					+  " affected " 
					+ elapsed;

		case CREATE:
		case DROP:
			return "KeySQL query OK. " 
					+ elapsed;
			
		default:
			return "";

		}
	}
}

enum ExitStatus {
	UNKNOWN(0), 
	EXIT_SUCCESS(200),
	EXIT_WARNING(300),
	EXIT_ERROR(400);
	
	private final int val;
	
	private ExitStatus(int val) { 
		this.val = val;
	};
	
	short valueOf() { 
		return (short)val;
	}
};

enum TransactionCommand {
	BEGIN(0),
	EXECUTE(1),
	ABORT(2),
	COMMIT(3);
	
	private final int val;
	
	TransactionCommand(int val) { this.val = val;};
	int valueOf() {
		return val;
	}
};
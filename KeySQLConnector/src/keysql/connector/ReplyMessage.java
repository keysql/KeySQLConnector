package keysql.connector;

public class ReplyMessage {
	public final static short EXIT_CODE_SUCCESS = 200; 
	public final static short EXIT_CODE_WARNING = 300; //incorrect request
	public final static short EXIT_CODE_SERVER_ERROR = 400;
	public final static short EXIT_CODE_COMUNICATION_ERROR = 500;
	private String reply;
	private int userId;
	private long count;
	private short exitCode;
	private StatementType type;
	double elapsedTime;
	boolean jsonReply;
	
	ReplyMessage(MessageHeader header,String reply) {
		this.userId = header.getUserId();
		this.count = header.getInstanceCount();
		this.exitCode = header.getExitCode();
		this.type = header.getStatementType();
		this.elapsedTime = header.getElapsedTime();
		this.jsonReply = header.jsonReply();
		this.reply = reply;
	}
	public String getReply() {
		return reply;
	}
	public long getCount() {
		return count;
	}
	public short getExitCode() {
		return exitCode;
	}
	public int getUserId() {
		return userId;
	}
	public StatementType getStatementType() {
		return type;
	}
	public double getElapsedTime() {
		return elapsedTime;
	}
	public boolean isJsonReply() {
		return jsonReply;
	}
}

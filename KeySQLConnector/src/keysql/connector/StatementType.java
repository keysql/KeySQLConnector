package keysql.connector;

public enum StatementType {
	UNKNOWN(0), 
	CREATE(1), 
	DROP(2), 
	SHOW(3), 
	INSERT(4), 
	DELETE(5), 
	UPDATE(6), 
	SELECT(7);
		
	private int value;
	private StatementType(int value) {
		this.value = value;
	}
	public int getValue() {
		return value;
	}
	public static StatementType fromInteger(int value) {
		switch (value) {
		case 1:
			return StatementType.CREATE;
		case 2:
			return StatementType.DROP;
		case 3:
			return StatementType.SHOW;
		case 4:
			return StatementType.INSERT;
		case 5:
			return StatementType.DELETE;
		case 6:
			return StatementType.UPDATE;
		case 7:
			return StatementType.SELECT;
		}
		return StatementType.UNKNOWN;
	}
}

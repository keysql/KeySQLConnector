package keysql.connector;

import java.util.List;

public interface IKeySQLJson {
	public static String createCatalogStatement(final String catalogName) {
		return "CREATE CATALOG " + catalogName;
	}
	public static String createStoreStatement(final String storeName, final String catalogName) {
		return "CREATE STORE " + storeName + " FOR CATALOG " + catalogName;
	}
	//generate create keyobject statements
	//return list of create statements string
	public List<String> createStatements(final String catalogName);
	//generate insert statement from json string "{...},{...},..."
	//return insert statement or null, if all json instances do not match schema
	public String insertInstancesStatement(final String jsonInstances, final String storeName);
}

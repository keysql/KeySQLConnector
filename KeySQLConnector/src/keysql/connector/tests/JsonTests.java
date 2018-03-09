package keysql.connector.tests;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.fasterxml.jackson.core.JsonParseException;

import keysql.connector.IKeySQLJson;
import keysql.connector.KeySQLJson;

public class JsonTests {

	@Test
	public void createMoviesShort() {
		BufferedWriter writer = null;
		try {
			Map<String,String> arrayObjectNames = new HashMap<>();
			arrayObjectNames.put("CAST", "ACTOR");
			final String schema = new String(Files.readAllBytes(Paths.get("movies_short_schema.json")));
			final IKeySQLJson converter = new KeySQLJson(schema, "movie_short", arrayObjectNames);
			final List<String> statements = converter.createStatements("movies_catalog");
			final String json = new String(Files.readAllBytes(Paths.get("movies_short.json")));
			final String insertStatement = converter.insertInstancesStatement(json, "movies_short_store");
			if (insertStatement != null) {
				statements.add(IKeySQLJson.createStoreStatement("movies_short_store", "movies_catalog"));
				statements.add(insertStatement);
			}
			final File sqlFile = new File("movies_short.sql");
			writer = new BufferedWriter(new FileWriter(sqlFile));
			writer.write(IKeySQLJson.createCatalogStatement("movies_catalog") + ";\n");
			for (final String st : statements) {
				writer.write(st + ";\n");
			}
		} catch (final JsonParseException e) {
			final int pos = (int) e.getLocation().getCharOffset();
			System.out.println("JsonParseException: " + e.getOriginalMessage().substring(pos, pos + 20));
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (final Exception e) {
			}
		}
	}
	@Test
	public void createPatientAnswer() {
		BufferedWriter writer = null;
		try {
			Map<String,String> arrayObjectNames = new HashMap<>();
			arrayObjectNames.put("__QUESTIONS__", "__QUESTION");
			arrayObjectNames.put("VALUES", "VALUE");
			final String schema = new String(Files.readAllBytes(Paths.get("patient_answer_shema.json")));
			final IKeySQLJson converter = new KeySQLJson(schema, "patient_answer", arrayObjectNames);
			final List<String> statements = converter.createStatements("patient_answer_catalog");
			final String json = new String(Files.readAllBytes(Paths.get("patient_answer_transformed.json")));
			final String insertStatement = converter.insertInstancesStatement(json, "patient_answer_store");
			if (insertStatement != null) {
				statements.add(IKeySQLJson.createStoreStatement("patient_answer_store", "patient_answer_catalog"));
				statements.add(insertStatement);
			}
			final File sqlFile = new File("patient_answer.sql");
			writer = new BufferedWriter(new FileWriter(sqlFile));
			writer.write(IKeySQLJson.createCatalogStatement("patient_answer_catalog") + ";\n");
			for (final String st : statements) {
				writer.write(st + ";\n");
			}
		} catch (final JsonParseException e) {
			final int pos = (int) e.getLocation().getCharOffset();
			System.out.println("JsonParseException: " + e.getOriginalMessage().substring(pos, pos + 20));
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (final Exception e) {
			}
		}
	}
	
	@Test
	public void createObjectsTest() {
		try {
			final String schema = new String(Files.readAllBytes(Paths.get("JsonSchemaWorldBank.json")));
			final IKeySQLJson converter = new KeySQLJson(schema, "world_bank");
			final List<String> statements = converter.createStatements("JsonTest");
			for (final String s : statements) {
				System.out.println(s + ';');
			}
		} catch (final JsonParseException e) {
			final int pos = (int) e.getLocation().getCharOffset();
			System.out.println("JsonParseException: " + e.getOriginalMessage().substring(pos, pos + 20));
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void insertTest() {
		try {
			final String schema = new String(Files.readAllBytes(Paths.get("JsonSchemaWorldBank.json")));
			final IKeySQLJson converter = new KeySQLJson(schema, "world_bank");
			final String json = new String(Files.readAllBytes(Paths.get("world_bank.json")));
			final String insertStatement = converter.insertInstancesStatement(json, "world_bank");
			System.out.println(insertStatement);
		} catch (final JsonParseException e) {
			final int pos = (int) e.getLocation().getCharOffset();
			System.out.println("JsonParseException: " + e.getOriginalMessage().substring(pos, pos + 20));
		} catch (final Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void wordBankScript() {
		BufferedWriter writer = null;
		try {
			final String schema = new String(Files.readAllBytes(Paths.get("JsonSchemaWorldBank.json")));
			final IKeySQLJson converter = new KeySQLJson(schema, "world_bank");
			final List<String> statements = converter.createStatements("world_bank_catalog");
			final String json = new String(Files.readAllBytes(Paths.get("world_bank.json")));
			final String insertStatement = converter.insertInstancesStatement(json, "world_bank_store");
			if (insertStatement != null) {
				statements.add(IKeySQLJson.createStoreStatement("world_bank_store", "world_bank_catalog"));
				statements.add(insertStatement);
			}
			final File sqlFile = new File("world_bank.sql");
			writer = new BufferedWriter(new FileWriter(sqlFile));
			writer.write(IKeySQLJson.createCatalogStatement("world_bank_catalog") + ";\n");
			for (final String st : statements) {
				writer.write(st + ";\n");
			}
		} catch (final JsonParseException e) {
			final int pos = (int) e.getLocation().getCharOffset();
			System.out.println("JsonParseException: " + e.getOriginalMessage().substring(pos, pos + 20));
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
			} catch (final Exception e) {
			}
		}
	}
}

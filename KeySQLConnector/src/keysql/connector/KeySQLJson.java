package keysql.connector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;

public class KeySQLJson implements IKeySQLJson {
	private final Map<String,String> arrayObjectNames;
	private final KeySQLObject root;
	private final Map<String,KeySQLObject> names;
	private final ObjectMapper mapper;
	private final JsonFactory factory;
	
	//constructor
	//first parameter - json schema
	//second parameter - name of root keyobject
	public KeySQLJson(final String jsonSchema, final String rootName, Map<String,String> arrayObjectNames) 
			throws JsonParseException, IOException, IllegalArgumentException 
	{
		this.arrayObjectNames = arrayObjectNames;
		names = new HashMap<>();
		mapper = new ObjectMapper();
		factory = mapper.getFactory();
		JsonParser jp = factory.createParser(jsonSchema);
		JsonNode node = mapper.readTree(jp);
		root = parseSchemaNode(node, rootName);
		if (root == null)
			throw new IllegalArgumentException("Bad schema");
	}
	
	public KeySQLJson(final String jsonSchema, final String rootName) 
			throws JsonParseException, IOException, IllegalArgumentException
	{
		this(jsonSchema, rootName, new HashMap<>());
	}
	
	private KeySQLObject parseSchemaNode(JsonNode node, String objName) {
		String type = node.get("type").asText();
		KeySQLObject r = null;
		switch (type) {
		case "string":
			r = new PrimitiveObject(objName == null ? "__CHAR_VALUE" : objName, PrimitiveObjectType.CharType);
			break;
		case "integer":
		case "double":
		case "number":
			r =  new PrimitiveObject(objName == null ? "__NUMBER_VALUE" : objName, PrimitiveObjectType.NumberType);
			break;
		case "date":
			r = new PrimitiveObject(objName == null ? "__DATE_VALUE" : objName, PrimitiveObjectType.DateType);
			break;
		case "array":
			KeySQLObject ref = parseSchemaNode(node.get("items"), arrayObjectNames.get(objName));
			r = new MultipleObject(objName == null ? "__" + ref.getName() + "_MULTIPLE" : objName, ref);
			break;
		case "object":
			node = node.get("properties");
			Map<String,KeySQLObject> childs = new TreeMap<>();
			Iterator<Map.Entry<String, JsonNode>> it = node.fields();
			while (it.hasNext()) {
				Map.Entry<String, JsonNode> field = it.next();
				KeySQLObject child = parseSchemaNode(field.getValue(), field.getKey().toUpperCase());
				if (child == null)
					return null;
				childs.put(field.getKey().toUpperCase(),child);
			}
			if (childs.isEmpty())
				return null;
			if (objName == null) {
				if (childs.size() == 1) {
					KeySQLObject child_ref = childs.values().iterator().next();
					if (child_ref.getType() == ObjectType.Composed)
						return child_ref;
				}
				StringBuilder sb = new StringBuilder("_");
				for (String name : childs.keySet())
					sb.append('_').append(name);
				objName = sb.toString();
			}
			r = new ComposedObject(objName, childs);
			break;
		}
		KeySQLObject k = names.get(objName);
		if (k != null && !r.equals(k))
			return null;
		names.put(objName, r);
		return r;
	}
	//generate create keyobject statements
	//return list of create statements string
	@Override
	public List<String> createStatements(final String catalogName) {
		if (root == null)
			return null;
		final List<String> statements = new ArrayList<>();
		final Set<String> processed = new HashSet<>();
		root.createStatements(processed, statements);
		statements.replaceAll(x -> "CREATE KEYOBJECT " + x + " IN CATALOG " + catalogName);
		return statements;
	}
	//generate insert statement from json string "{...},{...},..."
	//return insert statement or null, if all json instances do not match schema
	//skip instance, if it doesn't match schema
	//ignore node if it is not found in schema
	@Override
	public String insertInstancesStatement(final String jsonInstances, final String storeName) {
		final StringBuilder sb = new StringBuilder();
		int brackets = 0;
		int start = -1;
		for (int i = 0; i < jsonInstances.length(); ++i) {
			char c = jsonInstances.charAt(i);
			if (c == '{') {
				++brackets;
				if (start < 0)
					start = i;
			}
			else if (c == '}' && --brackets == 0) {
				String s = getInstance(jsonInstances.substring(start, i + 1));
				if (s != null) {
					if (sb.length() > 0)
						sb.append(",\n");
					sb.append(s);
				}
				start = -1; 
			}
		}
		if (sb.length() == 0)
			return null;
		return "INSERT INTO " + storeName + " INSTANCES\n" + sb.toString();
	}

	private String getInstance(final String jsonInstance) {
		try {
			JsonParser jp = factory.createParser(jsonInstance);
			JsonNode node = mapper.readTree(jp);
			String s = root.getInstance(node);
			if (s == null)
				return null;
			return "{" + s + "}";
		} catch (JsonParseException e) {
			//int pos = (int)e.getLocation().getCharOffset();
			//Logger.write("JsonParseException: " + jsonInstance.substring(pos, pos + 20));
			return null;
		} catch (IOException e) {
			return null;
		}
	}
}

enum PrimitiveObjectType {
	CharType, NumberType, DateType;
	static public String getType(PrimitiveObjectType type) {
		switch (type) {
		case CharType:
			return "CHAR";
		case NumberType:
			return "NUMBER";
		case DateType:
			return "DATE";
		}
		return "";
	}
};

enum ObjectType {
	Unknown, CharType, NumberType, DateType, Multiple, Composed
};

class KeySQLObject {
	protected final String name;
	public KeySQLObject(String name) {
		this.name = name;
	}
	public String getName() {
		return name;
	}
	public ObjectType getType() {
		return ObjectType.Unknown;
	}
	public void createStatements(Set<String> processed, List<String> statements) {
	}
	public String getInstance(JsonNode node) {
		return null;
	}
}

class PrimitiveObject extends KeySQLObject {
	private final PrimitiveObjectType type;
	public PrimitiveObject(String name, PrimitiveObjectType type) {
		super(name);
		this.type = type;
	}
	public ObjectType getType() {
		switch (this.type) {
		case CharType:
			return ObjectType.CharType;
		case NumberType:
			return ObjectType.NumberType;
		case DateType:
			return ObjectType.DateType;
		}
		return ObjectType.Unknown;
	}
	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;
		if (other == null)
			return false;
		if (!(other instanceof PrimitiveObject))
			return false;
		PrimitiveObject o = (PrimitiveObject)other;
		return type == o.type && name.equals(o.name);
	}
	public void createStatements(Set<String> processed, List<String> statements) {
		if (processed.contains(name))
			return;
		processed.add(name);
		statements.add(name + " " + PrimitiveObjectType.getType(type));
	}
	public String getInstance(JsonNode node) {
		JsonNodeType jsonType = node.getNodeType();
		switch (jsonType) {
		case NULL:
			return name+":NULL";
		case NUMBER:
			if (type == PrimitiveObjectType.NumberType)
				return name + ":" + node.asText();
			break;
		case STRING:
			if (type != PrimitiveObjectType.NumberType)
				return name + ":'" + node.asText() + "'";
			return name + ":" + node.asText();
		default:
			break;
		}
		return null;
	}
}

class MultipleObject extends KeySQLObject {
	private final KeySQLObject ref;
	public MultipleObject(String name, KeySQLObject ref) {
		super(name);
		this.ref = ref;
	}
	public ObjectType getType() {
		return ObjectType.Multiple;
	}
	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;
		if (other == null)
			return false;
		if (!(other instanceof MultipleObject))
			return false;
		MultipleObject o = (MultipleObject)other;
		return name.equals(o.name) && ref.equals(o);
	}
	public void createStatements(Set<String> processed, List<String> statements) {
		if (processed.contains(name))
			return;
		processed.add(name);
		ref.createStatements(processed, statements);
		statements.add(name + " {" + ref.getName() + " MULTIPLE}");
	}
	public String getInstance(JsonNode node) {
		JsonNodeType jsonType = node.getNodeType();
		if (jsonType == JsonNodeType.NULL)
			return name + ":NULL";
		if (jsonType != JsonNodeType.ARRAY)
			return null;
		StringBuilder sb = new StringBuilder(name + ":{");
		int initialLength = sb.length();
		Iterator<JsonNode> it = node.iterator();
		boolean not_first = false;
		while (it.hasNext()) {
			JsonNode field = it.next();
			if (not_first)
				sb.append(',');
			not_first = true;
			String s = ref.getInstance(field);
			if (s == null)
				return null;
			sb.append(s);
		}
		if (sb.length() == initialLength) {
			return name + ":NULL";
		}
		sb.append('}');
		return sb.toString();
	}
}

class ComposedObject extends KeySQLObject {
	private final Map<String,KeySQLObject> childs;
	public ComposedObject(String name, Map<String,KeySQLObject> childs) {
		super(name);
		this.childs = childs;
	}
	public final ObjectType getType() {
		return ObjectType.Composed;
	}
	@Override
	public boolean equals(Object other) {
		if (this == other)
			return true;
		if (other == null)
			return false;
		if (!(other instanceof ComposedObject))
			return false;
		ComposedObject o = (ComposedObject)other;
		if (!name.equals(o.name) || childs.size() != o.childs.size())
			return false;
		for (Map.Entry<String, KeySQLObject> entry : childs.entrySet()) {
			KeySQLObject ko = o.childs.get(entry.getKey());
			if (ko == null || !entry.getValue().equals(ko))
				return false;
		}
		return true;
	}
	public void createStatements(Set<String> processed, List<String> statements) {
		if (processed.contains(name))
			return;
		processed.add(name);
		StringBuilder st = new StringBuilder();
		for (KeySQLObject obj : childs.values()) {
			obj.createStatements(processed, statements);
			if (st.length() > 0)
				st.append(',');
			st.append(obj.getName());
		}
		statements.add(name + " {" + st.toString() + "}");
	}
	public String getInstance(JsonNode node) {
		JsonNodeType jsonType = node.getNodeType();
		if (jsonType == JsonNodeType.NULL)
			return "{" + name + ":NULL}";
		if (jsonType != JsonNodeType.OBJECT)
			return null;
		StringBuilder sb = new StringBuilder(name + ":{");
		int initialLength = sb.length();
		Iterator<Map.Entry<String, JsonNode>> it = node.fields();
		boolean not_first = false;
		while (it.hasNext()) {
			Map.Entry<String, JsonNode> field = it.next();
			KeySQLObject ref = childs.get(field.getKey().toUpperCase());
			if (ref == null)
				continue;
			if (not_first) 
				sb.append(',');
			not_first = true;
			String s = ref.getInstance(field.getValue());
			if (s == null)
				return null;
			sb.append(s);
		}
		if (sb.length() == initialLength) {
			return name + ":NULL";
		}
		sb.append("}");
		return sb.toString();
	}
}

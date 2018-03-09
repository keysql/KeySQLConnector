package keysql.connector;

import java.util.ArrayList;
import java.util.List;

public class ConnectorHelper {
	public static String[] createScript(String in) {
		final List<String> out = new ArrayList<>();
		final StringBuilder buffer = new StringBuilder();
		Boolean strValue = false, comment = false;
		char prev = 0;
		for (int i = 0; i < in.length(); ++i) {
			char c = in.charAt(i);
			if (c == ';' && !comment && !strValue) {
				String st = buffer.toString().trim();
				if (!st.isEmpty()) {
					out.add(st);
				}
				buffer.setLength(0);
				prev = 0;
				continue;
			}
			if (c == '\n' && !strValue) {
				comment = false;
				c = ' ';
			}
			else if (!strValue && !comment && c == '-' && prev == '-') {
				comment = true;
				buffer.deleteCharAt(buffer.length() - 1);
			}
			else if (!comment && c == '\'') 
				strValue = !strValue;
			if (!comment)
				buffer.append(c);
			prev = c;
		}
		if (buffer.length() > 0) {
			String st = buffer.toString().trim();
			if (!st.isEmpty()) {
				out.add(st);
			}
		}
		return out.toArray(new String[out.size()]);
	}
	public static String formatSelectReply(String reply, String indent, boolean jsonReply) {
		final StringBuilder indents = new StringBuilder();
		final StringBuilder r =  new StringBuilder();
		boolean val = false;
		if (jsonReply) {
			indents.append(indent);
			r.append('[').append('\n').append(indents);
		}
		int count = 0;
		for (int i = 0; i < reply.length(); ++i) {
			char c = reply.charAt(i);
			if (val) {
				r.append(c);
				if (c == '\'')
					val = false;
				continue;
			}
			switch (c) {
			case ':':
				r.append(' ').append(c).append(' ');
				break;
			case '{':
			case '[':
				++count;
				indents.append(indent);
				r.append(c).append('\n').append(indents);
				break;
			case ',':
				r.append(c);
				if (count > 0)
					r.append('\n').append(indents);
				break;
			case '}':
			case ']':
				--count;
				indents.setLength(indents.length() - indent.length());
				r.append('\n').append(indents).append(c);
				break;
			case '\'':
				r.append(c);
				val = true;
				break;
			default:
				r.append(c);
				break;
			}
		}
		if (jsonReply) {
			r.append("\n]");
		}
		return r.toString();
	}
}

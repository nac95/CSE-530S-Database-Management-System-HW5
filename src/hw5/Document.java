package hw5;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;

public class Document {
	
	/**
	 * Parses the given json string and returns a JsonObject
	 * This method should be used to convert text data from
	 * a file into an object that can be manipulated.
	 */
	public static JsonObject parse(String json) {
		if (json == null) {
			return null;
		}
		JsonParser parser = new JsonParser();
		JsonElement element = parser.parse(json);
		JsonObject result = element.getAsJsonObject();
		return result;
	}
	
	/**
	 * Takes the given object and converts it into a
	 * properly formatted json string. This method should
	 * be used to convert JsonObjects to strings
	 * when writing data to disk.
	 */
	public static String toJsonString(JsonObject json) {
		Set<String> keys = json.keySet();
		StringBuilder re = new StringBuilder();
		for (String key : keys) {
			JsonElement element = json.get(key);
			re.append(key);
			if (element.isJsonObject()) {
				re.append(element.getAsJsonObject().getAsString());
			} 
			if (element.isJsonArray()) {
				re.append(element.getAsJsonArray().getAsString());
			}
			if (element.isJsonPrimitive()) {
				re.append(element.getAsJsonPrimitive().getAsString());
			}
		}
		String result = re.toString();
		return result;
	}
}

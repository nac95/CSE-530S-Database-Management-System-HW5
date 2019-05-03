package hw5;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DBCursor implements Iterator<JsonObject>{
	private DBCollection collection;
	private JsonObject query;
	private JsonObject fields;
	private Map<JsonObject, Integer> result = new HashMap<>();
	private int index = -1;

	public DBCursor(DBCollection collection, JsonObject query, JsonObject fields) {
		this.collection = collection;
		this.query = query;
		this.fields = fields;
		//this.hasNextPointer = -1;
		//process query in this part
		//case 1: no parameter
		if (query == null && fields == null) {
			findAll(collection);
		} 
		//case 2: only have query parameter
		if (query != null && fields == null) {
			Set<String> queryKeys = query.keySet();
			find(collection, queryKeys, query);
		}
		//case 3: have both query and projection parameter
		if (query != null && fields != null) {
			Set<String> queryKeys = query.keySet();
			Set<String> fieldsKeys = fields.keySet();
			Iterator<String> iter2 = fieldsKeys.iterator();
			String fieldKey = iter2.next();
			JsonElement fieldValue = fields.get(fieldKey);
			boolean contain = fieldValue.getAsBoolean();
			find(collection, queryKeys, query, fieldKey, contain);
		}
		print(result);
	}
	
	private void findAll(DBCollection collection) {
		long count = collection.count();
		if (count <= 0) {
			System.out.println(" ");
		} else {
			for (long i = 0; i < count; i++) {
				JsonObject doc = collection.getDocument((int)i);
				System.out.println(doc);
			}
		}
	}
	
	private void find(DBCollection collection, Set<String> queryKeys, JsonObject query) {
		long count = collection.count();
		if (count <= 0) {
			System.out.println(" ");
		} else {
			Iterator<String> iter = queryKeys.iterator();
			//only one key-value pair
			String queryKey = iter.next();
			JsonElement queryValue = query.get(queryKey);
			if (queryValue.isJsonPrimitive()) {
				primitiveValue(count, collection, queryKey, queryValue);
			} else if (queryValue.isJsonObject()) {
				objectValue(count, collection, queryKey, queryValue);
			} else if (queryValue.isJsonArray()) {
				arrayValue(count, collection, queryKey, queryValue);
			}
			if (queryKeys.size() > 1){
				//more than one key-value pair
				while (iter.hasNext()) {
					queryKey = iter.next();
					queryValue = query.get(queryKey);
					if (queryValue.isJsonPrimitive()) {
						primitiveValue(count, collection, queryKey, queryValue);
					} else if (queryValue.isJsonObject()) {
						objectValue(count, collection, queryKey, queryValue);
					}else if (queryValue.isJsonArray()) {
						arrayValue(count, collection, queryKey, queryValue);
					}
				} 
			}
			if (result.size() == 0) {
				System.out.println(" ");
			}
			for (JsonObject object : result.keySet()) {
				if (result.get(object) == queryKeys.size()) {
					print(result);
				}
			}
		}
	}
	
	private void primitiveValue(long count, DBCollection collection, String queryKey, JsonElement queryValue) {
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			}
			if (doc.getAsJsonPrimitive(queryKey).getAsString().equals(queryValue.getAsString())) {
				notHaveField(doc);
			}				
		}
	}
	
	private void objectValue(long count, DBCollection collection, String queryKey, JsonElement queryValue) {
		Set<String> queryValueKeys = ((JsonObject)queryValue).keySet();
		Iterator<String> iter = queryValueKeys.iterator();
		String queryValueKey = iter.next();
		JsonElement queryValueValue = ((JsonObject)queryValue).get(queryValueKey);
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			} else if (doc.get(queryKey).getAsString().equals(queryValue.getAsString())) {
				notHaveField(doc);
			} else if (queryValueValue.isJsonPrimitive()){
				int value = queryValueValue.getAsInt();
				switch(queryValueKey) {
				case "$eq":
					if (doc.getAsJsonObject(queryKey).getAsInt() == value) {
						notHaveField(doc);
					}
					break;
					
				case "$gt":
					if (doc.getAsJsonObject(queryKey).getAsInt() > value) {
						notHaveField(doc);
					}
					break;
					
				case "$gte":
					if (doc.getAsJsonObject(queryKey).getAsInt() >= value) {
						notHaveField(doc);
					}
					break;
					
				case "$lt":
					if (doc.getAsJsonObject(queryKey).getAsInt() < value) {
						notHaveField(doc);
					}
					break;
					
				case "$lte":
					if (doc.getAsJsonObject(queryKey).getAsInt() <= value) {
						notHaveField(doc);
					}
					break;
					
				case "$ne":
					if (doc.getAsJsonObject(queryKey).getAsInt() != value) {
						notHaveField(doc);
					}
					break;
				}
					
			} else if (queryValueValue.isJsonArray()) {
				Set<Integer> set = new HashSet<>();
				Iterator<JsonElement> iter2 = queryValueValue.getAsJsonArray().iterator();
				set.add(iter2.next().getAsInt());
				while (iter2.hasNext()) {
					set.add(iter2.next().getAsInt());
				}
				switch(queryValueKey) {
				case "$in":
					if (set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
						notHaveField(doc);
					}
					break;

				case "$nin":
					if (!set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
						notHaveField(doc);
					}
					break;
				}
			}
		}
	}
	
	private void arrayValue(long count, DBCollection collection, String queryKey, JsonElement queryValue) {
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			}
			if (doc.getAsJsonArray(queryKey).getAsString().equals(queryValue.getAsString())){
				notHaveField(doc);
			}				
		}
	}
	
	private void notHaveField(JsonObject doc) {
		Integer num = result.get(doc);
		if (num == null) {
			result.put(doc, 1);
		} else {
			result.put(doc, num++);
		}
	}
	
	private void find(DBCollection collection, Set<String> queryKeys, JsonObject query, String fieldKey, boolean contain) {
		long count = collection.count();
		if (count <= 0) {
			System.out.println(" ");
		} else {
			Iterator<String> iter = queryKeys.iterator();
			//only one key-value pair
			String queryKey = iter.next();
			JsonElement queryValue = query.get(queryKey);
			if (queryValue.isJsonPrimitive()) {
				primitiveValue(count, collection, queryKey, queryValue, fieldKey, contain);
			} else if (queryValue.isJsonObject()) {
				objectValue(count, collection, queryKey, queryValue, fieldKey, contain);
			} else if (queryValue.isJsonArray()) {
				arrayValue(count, collection, queryKey, queryValue, fieldKey, contain);
			}
			if (queryKeys.size() > 1){
				//more than one key-value pair
				while (iter.hasNext()) {
					queryKey = iter.next();
					queryValue = query.get(queryKey);
					if (queryValue.isJsonPrimitive()) {
						primitiveValue(count, collection, queryKey, queryValue, fieldKey, contain);
					} else if (queryValue.isJsonObject()) {
						objectValue(count, collection, queryKey, queryValue, fieldKey, contain);
					}else if (queryValue.isJsonArray()) {
						arrayValue(count, collection, queryKey, queryValue, fieldKey, contain);
					}
				} 
			}
			if (result.size() == 0) {
				System.out.println(" ");
			}
			for (JsonObject object : result.keySet()) {
				if (result.get(object) == queryKeys.size()) {
					print(result);
				}
			}
		}
	}
	
	private void primitiveValue(long count, DBCollection collection, String queryKey, JsonElement queryValue, String fieldKey, boolean contain) {
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			}
			if (doc.getAsJsonPrimitive(queryKey).getAsString().equals(queryValue.getAsString())) {
				haveField(contain, doc, docKeys, fieldKey);
			}				
		}
	}
	
	private void haveField(boolean contain, JsonObject doc, Set<String> docKeys, String fieldKey) {
		JsonObject object = new JsonObject();
		if (contain) {
			object.add(fieldKey, doc.get(fieldKey));
		} else {
			for (String s : docKeys) {
				if (!s.equals(fieldKey)) {
					object.add(s, doc.get(s));
				}
			}
		}
		Integer num = result.get(object);
		if (num == null) {
			result.put(object, 1);
		} else {
			result.put(object, num++);
		}
	}
	
	private void objectValue(long count, DBCollection collection, String queryKey, JsonElement queryValue, String fieldKey, boolean contain) {
		Set<String> queryValueKeys = ((JsonObject)queryValue).keySet();
		Iterator<String> iter = queryValueKeys.iterator();
		String queryValueKey = iter.next();
		JsonElement queryValueValue = ((JsonObject)queryValue).get(queryValueKey);
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			} else if (doc.get(queryKey).getAsString().equals(queryValue.getAsString())) {
				haveField(contain, doc, docKeys, fieldKey);
			} else if (queryValueValue.isJsonPrimitive()){
				int value = queryValueValue.getAsInt();
				switch(queryValueKey) {
				case "$eq":
					if (doc.getAsJsonObject(queryKey).getAsInt() == value) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;
					
				case "$gt":
					if (doc.getAsJsonObject(queryKey).getAsInt() > value) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;
					
				case "$gte":
					if (doc.getAsJsonObject(queryKey).getAsInt() >= value) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;
					
				case "$lt":
					if (doc.getAsJsonObject(queryKey).getAsInt() < value) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;
					
				case "$lte":
					if (doc.getAsJsonObject(queryKey).getAsInt() <= value) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;
					
				case "$ne":
					if (doc.getAsJsonObject(queryKey).getAsInt() != value) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;
				}
					
			} else if (queryValueValue.isJsonArray()) {
				Set<Integer> set = new HashSet<>();
				Iterator<JsonElement> iter2 = queryValueValue.getAsJsonArray().iterator();
				set.add(iter2.next().getAsInt());
				while (iter2.hasNext()) {
					set.add(iter2.next().getAsInt());
				}
				switch(queryValueKey) {
				case "$in":
					if (set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;

				case "$nin":
					if (!set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
						haveField(contain, doc, docKeys, fieldKey);
					}
					break;
				}
			}
		}
	}
	
	private void arrayValue(long count, DBCollection collection, String queryKey, JsonElement queryValue, String fieldKey, boolean contain) {
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			}
			if (doc.getAsJsonArray(queryKey).getAsString().equals(queryValue.getAsString())) {
				haveField(contain, doc, docKeys, fieldKey);
			}				
		}
	}
	
	private void print(Map<JsonObject, Integer> result) {
		for (JsonObject object : result.keySet()) {
			System.out.println(object);
		}
	}
	
	/**
	 * Returns true if there are more documents to be seen
	 */
	public boolean hasNext() {
		if (index < result.size() - 1) {
			return true;
		}
		return false;
	}

	/**
	 * Returns the next document
	 */
	public JsonObject next() {
		List<JsonObject> list = toList(result);
		return list.get(++index);
	}
	
	private List<JsonObject> toList(Map<JsonObject, Integer> input) {
		List<JsonObject> result = new LinkedList<JsonObject>();
		for (JsonObject object : input.keySet()) {
			result.add(object);
		}
		return result;
	}
	
	/**
	 * Returns the total number of documents
	 */
	public long count() {
		return result.size();
	}

}

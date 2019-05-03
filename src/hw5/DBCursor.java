package hw5;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class DBCursor implements Iterator<JsonObject>{
	private DBCollection collection;
	private JsonObject query;
	private JsonObject fields;
	private LinkedList<JsonObject> targetDocuments;
	private int hasNextPointer = -1;
	private int nextPointer;

	public DBCursor(DBCollection collection, JsonObject query, JsonObject fields) {
		this.collection = collection;
		this.query = query;
		this.fields = fields;
		this.targetDocuments = new LinkedList<>();
		//this.hasNextPointer = -1;
		//process query in this part
		//case 1: no parameter
		if (query == null && fields == null) {
			findAll(collection);
		} 
		//case 2: only have query parameter
		Set<String> queryKeys = query.keySet();
		Iterator<String> iter = queryKeys.iterator();
		String queryKey = iter.next();
		JsonElement queryValue = query.get(queryKey);
		if (query != null && fields == null) {
			find(collection, queryKey, queryValue);
		}
		//case 3: have both query and projection parameter
		Set<String> fieldsKeys = fields.keySet();
		Iterator<String> iter2 = fieldsKeys.iterator();
		String fieldKey = iter.next();
		JsonElement fieldValue = fields.get(fieldKey);
		boolean contain = fieldValue.getAsBoolean();
		if (query != null && fields != null) {
			find(collection, queryKey, queryValue, fieldKey, contain);
		}
	}
	
	private void findAll(DBCollection collection) {
		long count = collection.count();
		if (count <= 0) {
			System.out.println(" ");
		} else {
			for (long i = 0; i < count; i++) {
				JsonObject doc = collection.getDocument((int)i);
				targetDocuments.add(doc);
				System.out.println(doc);
			}
		}
	}
	
	private void find(DBCollection collection, String queryKey, JsonElement queryValue) {
		long count = collection.count();
		if (count <= 0) {
			System.out.println(" ");
		} else {
			if (queryValue.isJsonPrimitive()) {
				for (long i = 0; i < count; i++) {
					JsonObject doc = collection.getDocument((int)i);
					Set<String> docKeys = doc.keySet();
					if (!docKeys.contains(queryKey)) {
						break;
					}
					if (doc.getAsJsonPrimitive(queryKey).getAsString().equals(queryValue.getAsString())) {
						targetDocuments.add(doc);
						System.out.println(doc);
					}				
				}
			} else if (queryValue.isJsonObject()) {
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
						targetDocuments.add(doc);
						System.out.println(doc);
					} else if (queryValueValue.isJsonPrimitive()){
						int value = queryValueValue.getAsInt();
						switch(queryValueKey) {
						case "eq":
							if (doc.getAsJsonObject(queryKey).getAsInt() == value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "gt":
							if (doc.getAsJsonObject(queryKey).getAsInt() > value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "gte":
							if (doc.getAsJsonObject(queryKey).getAsInt() >= value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "lt":
							if (doc.getAsJsonObject(queryKey).getAsInt() < value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "lte":
							if (doc.getAsJsonObject(queryKey).getAsInt() <= value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "ne":
							if (doc.getAsJsonObject(queryKey).getAsInt() != value) {
								targetDocuments.add(doc);
								System.out.println(doc);
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
						case "in":
							if (set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;

						case "nin":
							if (!set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
						}
					}
				}
			}
		}
	}
	
	private void find(DBCollection collection, String queryKey, JsonElement queryValue, String fieldKey, boolean contain) {
		long count = collection.count();
		if (count <= 0) {
			System.out.println(" ");
		} else {
			if (queryValue.isJsonPrimitive()) {
				for (long i = 0; i < count; i++) {
					JsonObject doc = collection.getDocument((int)i);
					Set<String> docKeys = doc.keySet();
					if (!docKeys.contains(queryKey)) {
						break;
					}
					if (doc.getAsJsonPrimitive(queryKey).getAsString().equals(queryValue.getAsString())) {
						if (contain) {
							String result = doc.get(fieldKey).getAsString();
							targetDocuments.add(doc);
							System.out.println(fieldKey + result);
						} else {
							
						}
					}				
				}
			} else if (queryValue.isJsonObject()) {
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
						targetDocuments.add(doc);
						System.out.println(doc);
					} else if (queryValueValue.isJsonPrimitive()){
						int value = queryValueValue.getAsInt();
						switch(queryValueKey) {
						case "eq":
							if (doc.getAsJsonObject(queryKey).getAsInt() == value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "gt":
							if (doc.getAsJsonObject(queryKey).getAsInt() > value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "gte":
							if (doc.getAsJsonObject(queryKey).getAsInt() >= value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "lt":
							if (doc.getAsJsonObject(queryKey).getAsInt() < value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "lte":
							if (doc.getAsJsonObject(queryKey).getAsInt() <= value) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
							
						case "ne":
							if (doc.getAsJsonObject(queryKey).getAsInt() != value) {
								targetDocuments.add(doc);
								System.out.println(doc);
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
						case "in":
							if (set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;

						case "nin":
							if (!set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
								targetDocuments.add(doc);
								System.out.println(doc);
							}
							break;
						}
					}
				}
			}
		}
	}
	
	/**
	 * Returns true if there are more documents to be seen
	 */
	public boolean hasNext() {
		hasNextPointer++;
		if(this.targetDocuments.get(hasNextPointer) != null) {
			return true;
		}
		return false;
	}

	/**
	 * Returns the next document
	 */
	public JsonObject next() {
		nextPointer++;
		return this.targetDocuments.get(nextPointer);
	}
	
	/**
	 * Returns the total number of documents
	 */
	public long count() {
		return this.targetDocuments.size();
	}

}

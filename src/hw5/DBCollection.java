package hw5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonStreamParser;


public class DBCollection {

	private DB database;
	private String name;
	public List<JsonObject> documentStorage;
	private File collection;
	/**
	 * Constructs a collection for the given database
	 * with the given name. If that collection doesn't exist
	 * it will be created.
	 * @throws IOException 
	 * 
	 * 
	 */
	public DBCollection(DB database, String name) throws IOException {
		this.database = database;
		this.name = name;
		//may change
		this.documentStorage = new LinkedList<>();
		this.collection = new File(this.database.db.getPath()+"/"+name+".json"); 
		
		if(this.collection.exists()) {
			System.out.println("start to read file");
			//try read the file
			try(FileReader fr = new FileReader(this.collection)){
				if(this.collection.length()!=0) {
					BufferedReader br = new BufferedReader(fr);
					JsonStreamParser parser = new JsonStreamParser(br);
					
					// synchronize on an object shared by threads
					synchronized (parser) { 
					System.out.println("parser has next?"+parser.hasNext());
						while (parser.hasNext()) {
						    JsonObject jo = (JsonObject)parser.next();
						    this.documentStorage.add(jo);
						    System.out.println(jo.toString());
						   }
					}
					//end of parse
				}
				
			}
			catch(IOException e) {
				System.out.println("IO exception");
				e.printStackTrace();
			}
		// if no such file
		}else {
			this.collection.createNewFile();
			 System.out.println("File has been created.");
		}
		
	}
	
	/**
	 * Returns a cursor for all of the documents in
	 * this collection.
	 */
	public DBCursor find() {
		return new DBCursor(this, null, null);
	}
	
	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @return
	 */
	public DBCursor find(JsonObject query) {
		DBCollection col = null;
		try {
			col = new DBCollection(this.database, this.name);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DBCursor result = new DBCursor(col, query, null);
		return result;
	}
	
	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @param projection relational project
	 * @return
	 */
	public DBCursor find(JsonObject query, JsonObject projection) {
		return new DBCursor(this,query,projection);
	}
	
	/**
	 * Inserts documents into the collection
	 * Must create and set a proper id before insertion
	 * When this method is completed, the documents
	 * should be permanently stored on disk.
	 * @param documents
	 * @throws IOException 
	 */
	public void insert(JsonObject... documents) throws IOException {
	  System.out.println("insert");
	  for(int i = 0; i < documents.length;++i) {
	   this.documentStorage.add(documents[i]);
	   System.out.println(documents[i].toString());
	   // add this to file
	   this.writeFile();
	  }
	 }
	
	/**
	 * Locates one or more documents and replaces them
	 * with the update document.
	 * @param query relational select for documents to be updated
	 * @param update the document to be used for the update
	 * @param multi true if all matching documents should be updated
	 * 				false if only the first matching document should be updated
	 */
	public void update(JsonObject query, JsonObject update, boolean multi) {
		Set<String> queryKeys = query.keySet();
		List<JsonObject> another = new LinkedList<>();
		Map<JsonObject, Integer> result = find(this, queryKeys, query, multi);
		Set<JsonObject> set = result.keySet();
		for (JsonObject object : documentStorage) {
			if (!set.contains(object)) {
				another.add(object);
			}
		}
		update(result, another, update);
		documentStorage = another;
	}
	
	private void update(Map<JsonObject, Integer> result, List<JsonObject> another, JsonObject update) {
		Set<String> updateKeys = update.keySet();
		Iterator<String> iter = updateKeys.iterator();
		//only one key-value pair
		String updateKey = iter.next();
		JsonElement updateValue = update.get(updateKey);
		for (JsonObject object : result.keySet()) {
			JsonObject newObject = new JsonObject();
			Set<String> objectKey = object.keySet();
			for (String s : objectKey) {
				if (s.equals(updateKey)) {
					newObject.add(updateKey, updateValue);
				} else {
					newObject.add(s, object.get(s));
				}
			}
			another.add(newObject);
		}
		if (updateKeys.size() > 1) {
			while (iter.hasNext()) {
				updateKey = iter.next();
				updateValue = update.get(updateKey);
				for (JsonObject object : result.keySet()) {
					JsonObject newObject = new JsonObject();
					Set<String> objectKey = object.keySet();
					for (String s : objectKey) {
						if (s.equals(updateKey)) {
							newObject.add(updateKey, updateValue);
						} else {
							newObject.add(s, object.get(s));
						}
					}
					another.add(newObject);
				}
			}
		}
	}
	
	private void updateDetail() {
		
	}
	/**
	 * Removes one or more documents that match the given
	 * query parameters
	 * @param query relational select for documents to be removed
	 * @param multi true if all matching documents should be updated
	 * 				false if only the first matching document should be updated
	 */
	public void remove(JsonObject query, boolean multi) {
		Set<String> queryKeys = query.keySet();
		List<JsonObject> another = new LinkedList<>();
		Map<JsonObject, Integer> result = find(this, queryKeys, query, multi);
		Set<JsonObject> set = result.keySet();
		for (JsonObject object : documentStorage) {
			if (!set.contains(object)) {
				another.add(object);
			}
		}
		documentStorage = another;
	}
	
	private Map<JsonObject, Integer> find(DBCollection collection, Set<String> queryKeys, JsonObject query, boolean multi) {
		long count = collection.count();
		Map<JsonObject, Integer> result = new HashMap<>();
		if (count > 0) {
			Iterator<String> iter = queryKeys.iterator();
			//only one key-value pair
			String queryKey = iter.next();
			JsonElement queryValue = query.get(queryKey);
			if (queryValue.isJsonPrimitive()) {
				primitiveValue(count, collection, queryKey, queryValue, result);
			} else if (queryValue.isJsonObject()) {
				objectValue(count, collection, queryKey, queryValue, result);
			} else if (queryValue.isJsonArray()) {
				arrayValue(count, collection, queryKey, queryValue, result);
			}
			if (queryKeys.size() > 1){
				//more than one key-value pair
				while (iter.hasNext()) {
					queryKey = iter.next();
					queryValue = query.get(queryKey);
					if (queryValue.isJsonPrimitive()) {
						primitiveValue(count, collection, queryKey, queryValue, result);
					} else if (queryValue.isJsonObject()) {
						objectValue(count, collection, queryKey, queryValue, result);
					} else if (queryValue.isJsonArray()) {
						arrayValue(count, collection, queryKey, queryValue, result);
					}
					if (!multi) {
						Collection<Integer> value = result.values();
						if (value.contains(queryKeys.size())) {
							break;
						}
					}
				} 
			}
		}
		return result;
	}
	
	private void primitiveValue(long count, DBCollection collection, String queryKey, JsonElement queryValue, Map<JsonObject, Integer> result) {
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			}
			if (doc.getAsJsonPrimitive(queryKey).getAsString().equals(queryValue.getAsString())) {
				notHaveField(doc, result);
			}				
		}
	}
	
	private void notHaveField(JsonObject doc, Map<JsonObject, Integer> result) {
		Integer num = result.get(doc);
		if (num == null) {
			result.put(doc, 1);
		} else {
			result.put(doc, num++);
		}
	}
	
	private void objectValue(long count, DBCollection collection, String queryKey, JsonElement queryValue, Map<JsonObject, Integer> result) {
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
				notHaveField(doc, result);
			} else if (queryValueValue.isJsonPrimitive()){
				int value = queryValueValue.getAsInt();
				switch(queryValueKey) {
				case "$eq":
					if (doc.getAsJsonObject(queryKey).getAsInt() == value) {
						notHaveField(doc, result);
					}
					break;
					
				case "$gt":
					if (doc.getAsJsonObject(queryKey).getAsInt() > value) {
						notHaveField(doc, result);
					}
					break;
					
				case "$gte":
					if (doc.getAsJsonObject(queryKey).getAsInt() >= value) {
						notHaveField(doc, result);
					}
					break;
					
				case "$lt":
					if (doc.getAsJsonObject(queryKey).getAsInt() < value) {
						notHaveField(doc, result);
					}
					break;
					
				case "$lte":
					if (doc.getAsJsonObject(queryKey).getAsInt() <= value) {
						notHaveField(doc, result);
					}
					break;
					
				case "$ne":
					if (doc.getAsJsonObject(queryKey).getAsInt() != value) {
						notHaveField(doc, result);
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
						notHaveField(doc, result);
					}
					break;

				case "$nin":
					if (!set.contains(doc.getAsJsonObject(queryKey).getAsInt())) {
						notHaveField(doc, result);
					}
					break;
				}
			}
		}
	}
	
	private void arrayValue(long count, DBCollection collection, String queryKey, JsonElement queryValue, Map<JsonObject, Integer> result) {
		for (long i = 0; i < count; i++) {
			JsonObject doc = collection.getDocument((int)i);
			Set<String> docKeys = doc.keySet();
			if (!docKeys.contains(queryKey)) {
				break;
			}
			if (doc.getAsJsonArray(queryKey).getAsString().equals(queryValue.getAsString())){
				notHaveField(doc, result);
			}				
		}
	}
	
	/**
	 * Returns the number of documents in this collection
	 */
	public long count() {
		return this.documentStorage.size();
	}
	
	public String getName() {
		return this.name;
	}
	
	/**
	 * Returns the ith document in the collection.
	 * Documents are separated by a line that contains only a single tab (\t)
	 * Use the parse function from the document class to create the document object
	 */
	public JsonObject getDocument(int i) {
		 JsonObject j = this.documentStorage.get(i);
		return j;
	}
	
	/**
	 * Drops this collection, removing all of the documents it contains from the DB
	 */
	public void drop() {
		int length = this.documentStorage.size();
		for(int i = 0; i < length; ++i) {
			this.documentStorage.remove(i);
		}
	}
	
	private void writeFile() {
		System.out.println("write file start");
		  String fileResult="";
		  try(FileWriter fw = new FileWriter(this.collection)){
		   for(int i = 0; i < this.documentStorage.size();++i) {
		    System.out.println(this.documentStorage.get(i).toString());
//		    System.out.println("insert"+Document.toJsonString(this.documentStorage.get(i))+"in collection 375");
		    fileResult += this.documentStorage.get(i).toString()+"\t\n";
		   }
		   System.out.println("file result"+fileResult);
		   fw.write(fileResult);
		   fw.flush();
		  }catch(IOException e) {
		   e.printStackTrace();
		  }
		  
		  
		 }
}

package hw5;

import java.util.Iterator;
import java.util.LinkedList;

import com.google.gson.JsonObject;

public class DBCursor implements Iterator<JsonObject>{
	private DBCollection collection;
	private JsonObject query;
	private JsonObject fields;
	private LinkedList<JsonObject> targetDocuments;
	private int hasNextPointer;
	private int nextPointer;

	public DBCursor(DBCollection collection, JsonObject query, JsonObject fields) {
		this.collection = collection;
		this.query = query;
		this.fields = fields;
		this.targetDocuments = new LinkedList<>();
		this.hasNextPointer = -1;
		//process query in this part
		// make need a function
		
		// if the query getAllDocument is true, then run if
		if(query.getAsBoolean()) {
			
			
			//if the fields is all projection
			if(fields.getAsBoolean()) {
				
			}else {
				
			}
			
			
		}
		// if the query getAllDocument is false, or cannot find boolean
		else {
			
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

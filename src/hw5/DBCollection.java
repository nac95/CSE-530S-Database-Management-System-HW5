package hw5;

import java.util.LinkedList;
import java.util.List;

import com.google.gson.JsonObject;

public class DBCollection {

	private DB database;
	private String name;
	public LinkedList<JsonObject> documentStorage;
	/**
	 * Constructs a collection for the given database
	 * with the given name. If that collection doesn't exist
	 * it will be created.
	 */
	public DBCollection(DB database, String name) {
		this.database = database;
		this.name = name;
		//may change
		this.documentStorage = new LinkedList<>();
	}
	
	/**
	 * Returns a cursor for all of the documents in
	 * this collection.
	 */
	public DBCursor find() {
		//create query
		JsonObject query = new JsonObject();
		query.addProperty("AllDocuments", true);
		//create field
		JsonObject projection = new JsonObject();
		projection.addProperty("AllProjections", true);
		return new DBCursor(this,query,projection);
	}
	
	/**
	 * Finds documents that match the given query parameters.
	 * 
	 * @param query relational select
	 * @return
	 */
	public DBCursor find(JsonObject query) {
		//create projection
		JsonObject projection = new JsonObject();
		projection.addProperty("AllProjections", true);
		return new DBCursor(this,query,projection);
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
	 */
	public void insert(JsonObject... documents) {
		for(int i = 0; i < documents.length;++i) {
			this.documentStorage.add(documents[i]);
		}
		// add this to file
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
		if(multi) {
			
		}else {
			
		}
	}
	
	/**
	 * Removes one or more documents that match the given
	 * query parameters
	 * @param query relational select for documents to be removed
	 * @param multi true if all matching documents should be updated
	 * 				false if only the first matching document should be updated
	 */
	public void remove(JsonObject query, boolean multi) {
		if(multi) {
			
		}else {
			
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
			this.documentStorage.remove();
		}
	}
}

package hw5;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonStreamParser;


public class DBCollection {

	private DB database;
	private String name;
	public LinkedList<JsonObject> documentStorage;
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
		return new DBCursor(this, query, null);
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
			// add this to file
			//try()
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

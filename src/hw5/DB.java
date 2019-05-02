package hw5;

import java.io.File;

public class DB {
	public String name;
	/**
	 * Creates a database object with the given name.
	 * The name of the database will be used to locate
	 * where the collections for that database are stored.
	 * For example if my database is called "library",
	 * I would expect all collections for that database to
	 * be in a directory called "library".
	 * 
	 * If the given database does not exist, it should be
	 * created.
	 */
	public DB(String name) {
		this.name = name;
	}
	
	/**
	 * Retrieves the collection with the given name
	 * from this database. The collection should be in
	 * a single file in the directory for this database.
	 * 
	 * Note that it is not necessary to read any data from
	 * disk at this time. Those methods are in DBCollection.
	 */
	public DBCollection getCollection(String name) {
		return new DBCollection(this,name);
	}
	
	/**
	 * Drops this database and all collections that it contains
	 */
	public void dropDatabase() {
		File db = new File("testfiles/"+this.name+"");
		if(db.exists()) {
			//delete all the collections first
			String[]entries = db.list();
			if(entries.length!=0) {
				for(String s: entries){
				    File currentFile = new File(db.getPath(),s);
				    currentFile.delete();
				}
			}
			db.delete();
		}
	}
	
	
}

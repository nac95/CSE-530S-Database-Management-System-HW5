package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;

class CursorTester {
	private DB db;
	private DBCollection test;
	
	// construction getter setter no test
	/*
	 *Queries:
	 * 	Find all (done?)
	 * 	Find with relational select
	 * 	Find with projection
	 * 	Conditional operators
	 * 	Embedded Documents and arrays
	 */
	@Before
	public void setup() {
		db = new DB("data");
		test = db.getCollection("test");
	}

	@Test
	public void testFindAll() {
		DBCursor results = test.find();
		assertTrue(results.count() == 3);
		//primitive
		assertTrue(results.hasNext());
		JsonObject d1 = results.next();
		assertTrue(d1.getAsJsonObject()!= null);
		assertTrue(d1.getAsJsonPrimitive("key").getAsString().equals("value"));
		//object
		assertTrue(results.hasNext());
		JsonObject d2 = results.next();
		assertTrue(d2.getAsJsonObject()!= null);
		JsonObject compare = new JsonObject();
		compare.addProperty("key2", "value2");
		assertTrue(d2.getAsJsonObject("embedded").equals(compare));
		//array
		assertTrue(results.hasNext());
		JsonObject d3 = results.next();
		assertTrue(d3.getAsJsonObject()!= null);
		JsonArray array2 = new JsonArray();
		array2.add("one");
		array2.add("two");
		array2.add("three");
		assertTrue(d3.getAsJsonArray("array").equals(array2));
		//end of all
		assertTrue(!results.hasNext());
	}
	
	@Test
	public void testFindQuery() {
		JsonObject jo = new JsonObject();
		jo.addProperty("key", "value");
		DBCursor results = test.find(jo);
		assertTrue(results.count() == 1);
		assertTrue(results.hasNext());
		assertTrue(!results.hasNext());
		assertTrue(results.next().getAsJsonObject()!= null);
		assertTrue(results.next().getAsJsonObject() == null);
	}
	
	@Test
	public void testFindQueryProjection() {		
		JsonObject document = new JsonObject();
		document.addProperty("title", "myTest");
		document.addProperty("course", "cse530");
		document.addProperty("Professor", "Shook");
		
		test.insert(document);
		
		JsonObject query = new JsonObject();
		query.addProperty("Professor", "Shook");
		
		JsonObject projection0 = new JsonObject();
		projection0.addProperty("title", 0);
		
		DBCursor result = test.find(query,projection0);
		assertTrue(result.count() == 1);
		assertTrue(result.hasNext());
		JsonObject doc = result.next();
		assertTrue(doc.getAsJsonObject()!= null);
		assertTrue(doc.getAsJsonObject("title")==null);
		
		JsonObject projection1 = new JsonObject();
		projection1.addProperty("title", 1);
		DBCursor result1 = test.find(query,projection1);
		assertTrue(result1.count() == 1);
		assertTrue(result1.hasNext());
		JsonObject doc1 = result1.next();
		assertTrue(doc1.getAsJsonObject()!= null);
		assertTrue(doc.getAsJsonObject("title")!=null);
		assertTrue(doc.getAsJsonObject("course")==null);
		
	}
	
	@Test
	public void testQueryEmbedded() {
		DB db2 = new DB("data");
		DBCollection test2 = db2.getCollection("test");
		
		JsonObject embedded =  new JsonObject();
		embedded.addProperty("key2", "value2");
		JsonObject outer =  new JsonObject();
		outer.add("embedded",embedded);
		
		DBCursor result = test2.find(outer);
		JsonObject doc = result.next();
		assertTrue(result.count()==1);
		assertTrue(doc.getAsJsonObject()!= null);
	}
	
	

}

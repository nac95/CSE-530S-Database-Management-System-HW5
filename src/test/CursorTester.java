package test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonObject;

import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;

class CursorTester {
	// construction getter setter no test
	/*
	 *Queries:
	 * 	Find all (done?)
	 * 	Find with relational select
	 * 	Find with projection
	 * 	Conditional operators
	 * 	Embedded Documents and arrays
	 */

	@Test
	public void testFindAll() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		DBCursor results = test.find();
		assertTrue(results.count() == 3);
		assertTrue(results.hasNext());
		JsonObject d1 = results.next(); //verify contents?
		assertTrue(results.hasNext());
		JsonObject d2 = results.next();//verify contents?
		assertTrue(results.hasNext());
		JsonObject d3 = results.next();//verify contents?
		assertTrue(!results.hasNext());
	}
	
	@Test
	public void testFindQuery() {
		
	}
	
	@Test
	public void testFindProjection() {
		
	}
	
	
	@Test
	public void testInsert() {
		
	}
	
	@Test
	public void testUpdate() {
		
	}
	
	@Test
	public void testRemove() {
		
	}
	
	@Test
	public void testCount() {
		
	}
	
	@Test
	public void testGetName() {
		
	}

	
	@Test
	public void testQueryEmbedded() {
		DB db = new DB("data");
		DBCollection test = db.getCollection("test");
		
		JsonObject embedded =  new JsonObject();
		embedded.addProperty("key2", "value2");
		JsonObject outer =  new JsonObject();
		outer.add("embedded",embedded);
		
		DBCursor result = test.find(outer);
		JsonObject doc = result.next();
		assertTrue(result.count()==1);
		assertTrue(doc.getAsJsonObject()!= null);
	}
	
	

}

package test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

//import hw1.Database;
import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;

class CollectionTester {
	
	private DB db;
	private DBCollection test;
	
	/*
	 * Things to be tested:
	 * 
	 * Document access
	 * Document insert/update/remove
	 */
	
	@Before
	public void setup() {
		db = new DB("data");
		test = db.getCollection("testCollection");
	}

	
	@Test
	public void testGetDocument() {	
		// test primitive
		JsonObject primitive = test.getDocument(0);
		assertTrue(primitive.getAsJsonPrimitive("key").getAsString().equals("value"));
		//test embedded
		JsonObject embedded = test.getDocument(1);
		JsonObject compare = new JsonObject();
		compare.addProperty("key2", "value2");
		assertTrue(embedded.getAsJsonObject("embedded").getAsJsonObject().equals(compare));
		//test array
		JsonObject array = test.getDocument(2);
		JsonArray array2 = new JsonArray();
		array2.add("one");
		array2.add("two");
		array2.add("three");
		assertTrue(array.getAsJsonObject("array").getAsJsonArray().equals(array2));
	}
	
	@Test
	public void testInsert() {
		JsonObject jo = new JsonObject();
		jo.addProperty("testInsert", "Worked");
		test.insert(jo);
		DBCursor result = test.find(jo);
		assertTrue(result.count()==1);
		JsonObject doc = result.next();
		assertTrue(doc.getAsJsonObject()!= null);
	}
	
	@Test
	public void testInsertMulti() {
		JsonArray jos = new JsonArray();
		for(int i=0; i < 20; ++i) {
			JsonObject document = new JsonObject();
			document.addProperty("testInsertMulti", "test");
			jos.add(document);
		}
		test.insert(jos.getAsJsonObject());
		JsonObject document = new JsonObject();
		document.addProperty("testInsertMulti", "test");
		DBCursor result = test.find(document);
		assertTrue(result.count()==20);
		for(int i = 0; i < 20; ++i) {
			assertTrue(result.hasNext());
			assertTrue(result.next().getAsJsonObject()!= null);
		}
		assertTrue(!result.hasNext());
		
	}
	
	@Test
	public void testRemoveSingle() {
		//Document d;
		JsonObject jo = new JsonObject();
		jo.addProperty("testRemove", "Worked");
		test.remove(jo, true);
		DBCursor result = test.find(jo);
		assertFalse(result.hasNext());
	}
	@Test
	public void testRemoveMulti() {
		JsonObject [] jos = new JsonObject[20];
		for(int i=0; i < jos.length; ++i) {
			jos[i] = new JsonObject();
			jos[i].addProperty("RemoveTest", "test");
		}
		JsonObject jo = new JsonObject();
		jo.addProperty("RemoveTest", "test");
		
		//test multi is False
		test.remove(jo, false);
		DBCursor result1 = test.find(jo);
		assertTrue(result1.count()==19);
		for(int i=0; i < jos.length-1;++i) {
			assertTrue(result1.hasNext());
		}
		assertFalse(result1.hasNext());
		
		//test multi is True
		test.remove(jo, true);
		DBCursor result2 = test.find(jo);
		assertTrue(result2.count()==0);
		assertFalse(result2.hasNext());
	}
	
	
	@Test
	public void testDrop() {
		DBCursor results = test.find();
		assertTrue(results.count() == 0);
		assertTrue(!results.hasNext());
	}
}

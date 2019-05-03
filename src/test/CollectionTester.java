package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.text.ParseException;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

//import hw1.Database;
import hw5.DB;
import hw5.DBCollection;
import hw5.DBCursor;
import hw5.Document;

public class CollectionTester {
	
	private DB db;
	private DBCollection testCollection;
	
	/*
	 * Things to be tested:
	 * 
	 * Document access
	 * Document insert/update/remove
	 */
	
	@Before
	public void setup() throws FileNotFoundException, IOException, ParseException {
		db = new DB("data");
		testCollection = db.getCollection("testCollection");
	}
	
	@Test
	public void testGetDocument() throws IOException, ParseException {	
		
		// test primitive
		JsonObject primitive = testCollection.getDocument(0);
		assertTrue(primitive.getAsJsonPrimitive("key").getAsString().equals("value"));
		//test embedded
		JsonObject embedded = testCollection.getDocument(1);
		JsonObject compare = new JsonObject();
		compare.addProperty("key2", "value2");
		assertTrue(embedded.getAsJsonObject("embedded").getAsJsonObject().equals(compare));
		//test array
		JsonObject array = testCollection.getDocument(2);
		JsonArray array2 = new JsonArray();
		array2.add("one");
		array2.add("two");
		array2.add("three");
		assertTrue(array.getAsJsonArray("array").equals(array2));
	}
	
	@Test
	public void testInsert() throws IOException, ParseException {
//		db = new DB("data");
//		testCollection = db.getCollection("testCollection");
		JsonObject jo = new JsonObject();
		jo.addProperty("testInsert", "Worked");
		System.out.println("not insert");
		testCollection.insert(Document.parse("{\"testInsert\":\"worked\"}"));
		System.out.println("inserted");
		DBCursor result = testCollection.find(jo);
		assertTrue(result.count()==1);
		JsonObject doc = result.next();
		assertTrue(doc.getAsJsonObject()!= null);
	}
//	
//	@Test
//	public void testInsertMulti() throws IOException, ParseException {
//		db = new DB("data");
//		testCollection = db.getCollection("testCollection");
//		testCollection.insert(	   
//		    Document.parse("{ \"state\": \"Missouri\", \"city\": \"Saint Louis\"}"),
//		    Document.parse("{ \"state\": \"Missouri\", \"city\": \"Saint Louis\"}, \"student\":{\"name\":\"A\"}"),
//		    Document.parse("{ \"state\": \"Missouri\", \"city\": \"Saint Louis\"},\"students\":[\"A\",\"B\"]")
//			);
//		JsonObject document = new JsonObject();
//		document.addProperty("state", "Missouri");
//		DBCursor result = testCollection.find(document);
//		assertTrue(result.count()==3);
//		for(int i = 0; i < 3; ++i) {
//			assertTrue(result.hasNext());
//			assertTrue(result.next().getAsJsonObject()!= null);
//		}
//		assertTrue(!result.hasNext());
//		
//	}
	
//	@Test
//	public void testRemoveSingle() throws FileNotFoundException, IOException, ParseException {
//		db = new DB("data");
//		System.out.println("about to get collection");
//		testCollection = db.getCollection("testCollection");
//		System.out.println("got collection");
//		//Document d;
//		JsonObject jo = new JsonObject();
//		jo.addProperty("testRemove", "Worked");
//		testCollection.remove(jo, false);
//		DBCursor result = testCollection.find(jo);
//		assertFalse(result.hasNext());
//	}
//	@Test
//	public void testRemoveMulti() throws FileNotFoundException, IOException, ParseException {
//		db = new DB("data");
//		System.out.println("about to get collection");
//		testCollection = db.getCollection("testCollection");
//		System.out.println("got collection");
//		JsonObject [] jos = new JsonObject[20];
//		for(int i=0; i < jos.length; ++i) {
//			jos[i] = new JsonObject();
//			jos[i].addProperty("RemoveTest", "test");
//		}
//		JsonObject jo = new JsonObject();
//		jo.addProperty("RemoveTest", "test");
//		
//		//test multi is False
//		testCollection.remove(jo, true);
//		DBCursor result1 = testCollection.find(jo);
//		assertTrue(result1.count()==19);
//		for(int i=0; i < jos.length-1;++i) {
//			assertTrue(result1.hasNext());
//		}
//		assertFalse(result1.hasNext());
//		
//		//test multi is True
//		testCollection.remove(jo, true);
//		DBCursor result2 = testCollection.find(jo);
//		assertTrue(result2.count()==0);
//		assertFalse(result2.hasNext());
//	}
//	
//	
//	@Test
//	public void testDrop() throws FileNotFoundException, IOException, ParseException {
//		db = new DB("data");
//		System.out.println("about to get collection");
//		testCollection = db.getCollection("testCollection");
//		System.out.println("got collection");
//		DBCursor results = testCollection.find();
//		assertTrue(results.count() == 0);
//		assertTrue(!results.hasNext());
//	}
}

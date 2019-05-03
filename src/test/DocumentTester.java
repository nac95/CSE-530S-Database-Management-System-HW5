package test;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import hw5.Document;

public class DocumentTester {

	/*
	 * Things to consider testing:
	 * 
	 * Parsing embedded documents
	 * Parsing arrays
	 * 
	 * Object to primitive
	 * Object to embedded document
	 * Object to array
	 */
	@Test
	public void testParse() {
		String json = "{ \"key\": \"value\" }";
		JsonObject results = Document.parse(json);
		assertTrue(results.getAsJsonPrimitive("key").getAsString().equals("value"));
	}
	
	@Test
	public void testParseEmbedded() {
		String json =  "{ \"key\": \"value\", \"embedded\":{\"key2\":\"value2\"}}";
		JsonObject results = Document.parse(json);
		JsonObject oj = new JsonObject();
		oj.addProperty("key2", "value2");
		assertTrue(results.getAsJsonObject("embedded").equals(oj));
	}
	
	@Test
	public void testParseArray() {
		String json =  "{ \"key\": \"value\", \"Array\":[\"one\",\"two\",\"three\"]}";
		JsonObject results = Document.parse(json);
		JsonArray array2 = new JsonArray();
		array2.add("one");
		array2.add("two");
		array2.add("three");
		assertTrue(results.getAsJsonArray("Array").equals(array2));
		
	}

}

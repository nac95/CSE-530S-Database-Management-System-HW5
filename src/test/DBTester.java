package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;

import org.junit.Before;
import org.junit.Test;

import hw5.DB;
import hw5.DBCollection;


public class DBTester {

	/*
	 * Things to consider testing:
	 * 
	 * Properly returns collections
	 * Properly creates new collection
	 * Properly creates new DB (done)
	 * Properly drops db
	 */
	
	@Test
	public void testCreateNewDB() {
		DB hw5 = new DB("hw5");
		assertTrue(new File("testfiles/hw5").exists());
	}
	
	@Test
	public void testCreateNewCollection() throws FileNotFoundException, IOException, ParseException {
		DB hw5 = new DB("hw5");
		DBCollection collection = new DBCollection(hw5, "test");
		assertTrue(new File("testfiles/hw5/test.json").exists());
	}
	
	@Test
	public void testReturnCollection() throws FileNotFoundException, IOException, ParseException {
		DB hw5 = new DB("hw5");
		DBCollection collection = new DBCollection(hw5, "test");
		assertTrue(hw5.getCollection("test").getName().equals("test"));
	}
	
	@Test
	public void testDropDB() {
		DB hw5 = new DB("hw5");
		hw5.dropDatabase();
		assertFalse(new File("testfiles/hw5").exists());
	}

}

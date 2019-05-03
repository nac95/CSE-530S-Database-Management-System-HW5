package test;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import hw5.DB;


class DBTester {

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
	
	

}

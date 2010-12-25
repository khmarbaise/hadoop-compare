package com.soebes.hadoop;

import java.io.File;

import org.testng.annotations.Test;


public class TapeLogReaderTest extends TestBase {

	@Test
	public void firstTest() {
		String[] args = { getTestResourcesDirectory() + File.separator + "logfile-test.log" };
		TapeLogReader.main(args);
	}

}

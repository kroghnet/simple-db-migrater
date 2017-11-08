package net.krogh.sdm;

import static org.junit.Assert.*;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

public class SqlParserTest {

	private static final String SQL_TEST_FILE = "src/test/resources/SqlParserTest.sql";
	
	@Test
	public void testReadfile() throws IOException {
		
		List<String> sqls = SdmSqlParser.readFile(SQL_TEST_FILE);
		
		assertEquals(2, sqls.size());
		assertEquals("here is some sql", sqls.get(0));
		assertEquals("more sql split by comment", sqls.get(1));
	}

	
	@Test
	public void testReadReader() throws IOException {
		
		List<String> sqls = SdmSqlParser.readSql(new FileReader(SQL_TEST_FILE));
		
		assertEquals(2, sqls.size());
		assertEquals("here is some sql", sqls.get(0));
		assertEquals("more sql split by comment", sqls.get(1));
	}

}

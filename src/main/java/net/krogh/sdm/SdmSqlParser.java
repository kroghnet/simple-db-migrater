package net.krogh.sdm;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/*
 * TODO Add support for block comment
 * 
 */
public class SdmSqlParser {

	public static final String SQL_LINE_COMMENT_START = "//";
	public static final String SQL_BLOCK_END = ";";

	public static List<String> readFile(String file) throws IOException {
		return readSql(new FileReader(file));
	}
	
	public static List<String> readSql(Reader reader) throws IOException {
		
		List<String> res = new ArrayList<String>();
		BufferedReader r = new BufferedReader(reader);

		try {

			StringBuffer sql = new StringBuffer();
			String line;

			while ((line = r.readLine()) != null) {

				line = line.trim();

				if (line.length() == 0 || line.startsWith(SQL_LINE_COMMENT_START)) {
					continue;
				}

				if (sql.length() != 0) {
					sql.append(" ");
				}

				if (line.endsWith(SQL_BLOCK_END)) {
					if (line.length() > 1) {
						sql.append(line.substring(0, line.length() - 1));
					}
					res.add(sql.toString());
					sql = new StringBuffer();
				} else {
					sql.append(line);
				}

			}
		} finally {
			r.close();
		}

		return res;
	}

}

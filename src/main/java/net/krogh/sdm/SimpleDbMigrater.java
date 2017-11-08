package net.krogh.sdm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.Query;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * TODO externalize sql
 * TODO unit test
 */
public class SimpleDbMigrater {

    private static final Logger log = LoggerFactory.getLogger(SimpleDbMigrater.class);

    public static final String ENV_OWN_TABLE = "sdm.own.table";
    
    private static final String OWN_TABLE;
    private static final String DEFAULT_OWN_TABLE = "db_upgrade";
    
    static {
    	String ownTable = System.getProperty(ENV_OWN_TABLE);
    	if(ownTable != null) {
    		log.info("Using table from JVM property " + ENV_OWN_TABLE + ": " + ownTable);
    	} else {
    		ownTable = System.getenv(ENV_OWN_TABLE);
        	if(ownTable != null) {
        		log.info("Using table from environment variable " + ENV_OWN_TABLE + ": " + ownTable);
        	}
    	}
    	OWN_TABLE = ownTable != null ? ownTable : DEFAULT_OWN_TABLE;
    }

    private static final String COL_ID = "id";
    private static final String COL_APPLY_DATE = "apply_date";
    private static final String COL_NAME = "name";
    
    private static final String SQL_OWN_TABLE_EXISTS = "SELECT COUNT(*) FROM " + OWN_TABLE;
    private static final String SQL_CREATE_OWN_TABLE = String.format(
    				"CREATE TABLE %s ( " + 
    				"%s INTEGER NOT NULL, " + 
    				"%s TIMESTAMP NOT NULL, " + 
    				"%s VARCHAR(255) DEFAULT NULL, " + 
    				"PRIMARY KEY (%2$s) )", 
    				OWN_TABLE, COL_ID, COL_APPLY_DATE, COL_NAME);
    private static final String SQL_GET_IDS_ORDERED_DESC = String.format("SELECT %s FROM %s ORDER BY %1$s DESC", COL_ID, OWN_TABLE);
    private static final String SQL_CHECK_UPGRADE_EXISTS = String.format("SELECT %s FROM %s WHERE name=?", COL_ID, OWN_TABLE);
    private static final String SQL_INSERT_ENTRY = String.format("INSERT INTO %s (%s,%s,%s) values (?,?,?)", OWN_TABLE, COL_ID, COL_APPLY_DATE, COL_NAME);

    private static AtomicBoolean done = new AtomicBoolean(false);

    private static void failIfDone() {
        synchronized (done) {
            if(done.get()) {
                throw new IllegalStateException("Simple database upgrade already run");
            }
            done.set(true);
        }
    }
    
    public static void migrateFromFiles(EntityManager entityManager, String path) throws IOException {
    	migrateFromFiles(entityManager, OWN_TABLE, path);
    }
    
    /**
     * Apply migration files from a folder 
     * 
     * @param entityManager
     * @param path path to the folder containing the upgrade *.sql files
     * @throws IOException
     */
    public static void migrateFromFiles(EntityManager entityManager, String upgradeTable, String path) throws IOException {
    	failIfDone();
    	applyUpgrades(entityManager, listAvailableUpgradesInFolder(path));
    }

    /**
     * Apply migration resources found at a specific resource location, enabling embedding of 
     * sql files in an application jar file
     * 
     * Currently {@link ClassLoader#getSystemClassLoader()} is used to load the resources
     * @param entityManager
     * @param path the resource path containing the upgrade *.sql files
     * @throws IOException
     */
    public static void migrateFromResourcePath(EntityManager entityManager, String path) throws IOException {
    	failIfDone();
    	applyUpgrades(entityManager, listAvailableUpgradesInResourcePath(path));
    }
    
    /**
     * Apply any upgrades found in the specified path 
     * 
     * @param entityManager
     * @param path
     * @throws IOException
     */
    private static void applyUpgrades(EntityManager entityManager, List<InputStreamProvider> upgrades) throws IOException {
        
        ensureOwnTable(entityManager);
        
        Collections.sort(upgrades, new Comparator<InputStreamProvider>() {
			@Override
			public int compare(InputStreamProvider o1, InputStreamProvider o2) {
				return o1.name.compareTo(o2.name);
			}
		});
        
        EntityTransaction transaction = null;
        
        try {
        	transaction = entityManager.getTransaction();
        	transaction.begin();
            
            int nextId = getHigestId(entityManager) + 1;

            int applied = 0;
            
            for(InputStreamProvider upgr : upgrades) {
                if (!upgradeExists(entityManager, upgr.name)) {
                    log.warn("Applying upgrade: " + upgr.name);
                    Reader r = null;
                    try {
                    	r = new InputStreamReader(upgr.createStream(), Charset.forName("utf-8"));
                        List<String> sqls = SdmSqlParser.readSql(r);
                        for (String sql : sqls) {
                            log.debug("Upgrade query: " + sql);
                            entityManager.createNativeQuery(sql).executeUpdate();
                        }
                    } finally {
                    	if(r != null) {
                    		r.close();
                    	}
                    }
                    addUpdateEntry(entityManager, nextId++, upgr.name);
                    applied++;
                } else {
                    log.debug("Skipping applied upgrade: " + upgr.name);
                }
            }

        	transaction.commit();
        	log.info("Applied " + applied + " upgrades");
        } catch (IOException e) {
    		log.error("Error applying upgrades", e);
    		throw e;
        } finally {
        	if(transaction!= null && transaction.isActive() && transaction.getRollbackOnly()) {
        		transaction.rollback();
        	}
        }
    }
    
    private static List<InputStreamProvider> listAvailableUpgradesInResourcePath(String path) throws IOException {
    	
        List<InputStreamProvider> list = new ArrayList<InputStreamProvider>();
        
        InputStream stream = ClassLoader.getSystemResourceAsStream(path);

        if(!path.endsWith("/")) {
        	path += "/";
        }
        
        if(stream != null) {
        	BufferedReader reader = null;
        	try {
	        	reader = new BufferedReader(new InputStreamReader(stream));
	        	String r;
	        	while((r = reader.readLine()) != null) {
	        		list.add(new ResourceInputStreamProvider(r, path + r));
	        	}
        	} finally {
        		if(reader != null) {
        			reader.close();
        		}
        	}
        }
        
        return list;
    }

    protected static List<InputStreamProvider> listAvailableUpgradesInFolder(String folderName) throws IOException {

        log.info("Checking upgrade folder: " + folderName);

        ArrayList<String> files = new ArrayList<String>();
        
        for(File f : new File(folderName).listFiles()) {
        	if(f.getName().endsWith(".sql")) {
        		files.add(f.getName());
        	}
        }
        
        Collections.sort(files);
        
        List<InputStreamProvider> res = new ArrayList<InputStreamProvider>();
        
        for(String f : files) {
        	res.add(new FileInputStreamProvider(f, folderName + File.separator + f));
        }
        
        return res;
    }

    private static int getHigestId(EntityManager entityManager) {
    	@SuppressWarnings("unchecked")
		List<Integer> list = entityManager.createNativeQuery(SQL_GET_IDS_ORDERED_DESC).getResultList();
    	return list == null || list.isEmpty() ? 0 : list.get(0);
    }
    
    private static boolean upgradeExists(EntityManager entityManager, String name) {
    	Query query = entityManager.createNativeQuery(String.format(SQL_CHECK_UPGRADE_EXISTS, name));
    	query.setParameter(1, name);
    	List<?> list = query.getResultList();
        return list != null && !list.isEmpty();
    }

    private static void addUpdateEntry(EntityManager entityManager, int id,  String name) {
        Query query = entityManager.createNativeQuery(SQL_INSERT_ENTRY);
        query.setParameter(1, id);
        query.setParameter(2, new Date());
        query.setParameter(3, name);
        query.executeUpdate();
    }

    private static void ensureOwnTable(EntityManager entityManager) {
    	
        try {
        	entityManager.createNativeQuery(SQL_OWN_TABLE_EXISTS).getResultList();
            log.debug("Upgrade table exists");
            return;
        } catch (Exception e) {
            log.info("Table " + OWN_TABLE + " does not exist; creating (Error: " + e.getClass().getSimpleName() + "(" + e.getMessage() + ")");
        }
        
    	EntityTransaction transaction = null;
    	
        try {
        	transaction = entityManager.getTransaction();
        	transaction.begin();
        	createOwnTable(entityManager);
			transaction.commit();
        } finally {
        	if(transaction!= null && transaction.isActive() && transaction.getRollbackOnly()) {
       			transaction.rollback();
        	}
        }
    }

    private static void createOwnTable(EntityManager entityManager) {
        log.debug(SQL_CREATE_OWN_TABLE);
        entityManager.createNativeQuery(SQL_CREATE_OWN_TABLE).executeUpdate();
    }
    
    private abstract static class InputStreamProvider {
    	String name;
    	public InputStreamProvider(String name) {
			this.name = name;
		}
		abstract InputStream createStream() throws IOException;
    }
    
    private static class ResourceInputStreamProvider extends InputStreamProvider {
    	
    	String path;

		public ResourceInputStreamProvider(String name, String path) {
			super(name);
			this.path = path;
		}
		
		@Override
		InputStream createStream() throws IOException {
			return ClassLoader.getSystemClassLoader().getResourceAsStream(path);
		}
    }
    
    private static class FileInputStreamProvider extends InputStreamProvider {
    	
    	String path;

		public FileInputStreamProvider(String name, String path) {
			super(name);
			this.path = path;
		}
		
		@Override
		InputStream createStream() throws IOException {
			return new FileInputStream(path);
		}
    }

}

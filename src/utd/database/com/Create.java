package utd.database.com;

import java.io.File;
import java.io.RandomAccessFile;

public class Create
{

	Utility utility = Utility.getInstance();

	public String[] getTokens(String userCommand)
	{
		userCommand = userCommand.replace('(', '#').replace(',', '#').replace(')', ' ').trim();
		return userCommand.split("#");
	}

	public void table(String userCommand)
	{
		try
		{
			String database = utility.getSeletedDatabase();
			if(database == null)
			{
				System.out.println("Please select database");
				return;
			}
			String[] tokens = getTokens(userCommand);
			String tableName = tokens[0].trim().split(" ")[2];
			
			if(!utility.isTablePresent(tableName, false)) 
			{
				tokens[0] = "rowid int";
				RandomAccessFile tables = new RandomAccessFile(IUtitlityConstants.ALL_TABLE_TBL, "rw");
				tables.seek(tables.length());
				tables.writeByte(0);
				tables.writeByte(database.length());
				tables.writeBytes(database);				
				tables.writeByte(0);
				tables.writeByte(tableName.length());
				tables.writeBytes(tableName);
				tables.close();
					
				RandomAccessFile columns = new RandomAccessFile(IUtitlityConstants.DATABASE_PATH + File.separator + database + File.separator + "columns.tbl", "rw");
				columns.seek(columns.length());
				for (String token : tokens) {
					
					token = token.trim();
					if ((token != null) && (!token.isEmpty())) {
						columns.writeByte(0);
						if (token.contains("primary key")) {
							token = token.replace("primary key", "primarykey");
						}
						if (token.contains("not nullable")) {
							token = token.replace("not nullable", "notnullable");
						}
						String columnAttr = tableName + "#"
								+ token.replaceAll("  ", " ").replaceAll(" ", "#").trim();
						columns.writeByte(columnAttr.length());
						columns.writeBytes(columnAttr);
					}
				}
				columns.close();
				RandomAccessFile table = new RandomAccessFile(IUtitlityConstants.DATABASE_PATH + File.separator + database + File.separator + tableName + ".tbl", "rw");
				table.close();
				System.out.println("Table is created Successfully");
			} else {
				System.out.println("Table is already created");
			}	
		} catch (Exception e) {
			System.out.println("Error, while Creating a table");
		}
	}

	public void database(String userCommand)
	{
		String[] userCommandTokens = userCommand.trim().split(" ");
		try 
		{
			String dbName = userCommandTokens[2];
			if (utility.isDatabaseExist(dbName))
			{
				System.out.println("Database \"" + dbName + "\" exists.");
				return;
			}
			File newAllDB = new File("data" + File.separator + "catalog");
			if(!newAllDB.exists()) {
				newAllDB.mkdirs();
			}
			RandomAccessFile allDbFile = new RandomAccessFile(IUtitlityConstants.ALL_DATABASE_TBL, "rw");
			allDbFile.seek(allDbFile.length());
			allDbFile.writeByte(0);
			allDbFile.writeByte(dbName.length());
			allDbFile.writeBytes(dbName);
			allDbFile.close();
			
			File newDBDir = new File("data" + File.separator + dbName);
			newDBDir.mkdirs();
				
		}catch(Exception e) {
			System.out.println(e);
		}
			
		}
}

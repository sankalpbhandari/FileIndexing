package utd.database.com;

import java.io.File;
import java.io.RandomAccessFile;



public class Basic {

	Utility utility = Utility.getInstance();

	public void showTables(String userCommand)
	{
		try
		{
			RandomAccessFile database = new RandomAccessFile("data" + File.separator + "user_data" + File.separator + utility.getSeletedDatabase() + File.separator + "tables.tbl", "rw");
			boolean isRecordPresent = false;
			while(database.getFilePointer() < database.length())
			{
				int isDeleted = database.readByte();
				byte length = database.readByte();
				byte[] bytes = new byte[length];
				database.read(bytes, 0, bytes.length);
				if (isDeleted == 0)
				{
					isRecordPresent = true;
					System.out.println(new String(bytes));
				}
				database.readInt();
			}
			if(!isRecordPresent)
				System.out.println("No table is present for " + utility.getSeletedDatabase() + " database");
			database.close();
		}
		catch (Exception e)
		{
			System.out.println("Error, while fetching values from database");
		}
	}

	public void showDatabases(String userCommand) 
	{
		try
		{
			java.io.File file = new java.io.File(IUtitlityConstants.ALL_DATABASE_TBL);
			if((file.exists()) && (!file.isDirectory()))
			{
				RandomAccessFile databases = new RandomAccessFile(IUtitlityConstants.ALL_DATABASE_TBL, "rw");
				boolean isRecordPresent = false;
				while(databases.getFilePointer() < databases.length())
				{
					int isDeleted = databases.readByte();
					byte length = databases.readByte();
					byte[] bytes = new byte[length];
					databases.read(bytes, 0, bytes.length);
					if(isDeleted == 0)
					{
						System.out.println(new String(bytes));
						isRecordPresent = true;
					}
				}
				if(!isRecordPresent)
					System.out.println("No database is present in the system");
				databases.close();
			}
			else
			{
				System.out.println("No database is present");
			}
		}
		catch (Exception e)
		{
			System.out.println("Error, while fetching values from database" + e.getMessage());
		}
	}

	public void useDatabase(String databaseName)
	{
		try
		{
			RandomAccessFile databases = new RandomAccessFile("data" + File.separator + "catalog" + File.separator + "databases.tbl", "rw");
			boolean isDatabasePresent = false;
			while (databases.getFilePointer() < databases.length()) 
			{
				int isDeleted = databases.readByte();
				byte length = databases.readByte();
				byte[] bytes = new byte[length];
				databases.read(bytes, 0, bytes.length);
				if ((databaseName.equals(new String(bytes).trim())) && (isDeleted == 0))
				{
					utility.setSeletedDatabase(databaseName);
					System.out.println("Switched to database " + databaseName);
					isDatabasePresent = true;
				}
			}
			databases.close();
			if (!isDatabasePresent)
				System.out.println("Database " + databaseName + " is not present in the system");
		}
		catch (Exception e)
		{
			System.out.println("Error, while fetching values from database");
		}
	}
}

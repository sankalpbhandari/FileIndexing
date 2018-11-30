package utd.database.com;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * 
 * @author nikitakothari
 * 
 *         Drop table or database
 */

public class Drop {

	Utility utility = Utility.getInstance();

	public void table(String userCommand) {
		try {
			String[] tokens = userCommand.split(" ");
			String tableName = tokens[2];

			RandomAccessFile database = new RandomAccessFile(utility.getSeletedDatabase() + ".tables.tbl", "rw");
			while (database.getFilePointer() < database.length()) {
				int isDeleted = database.readByte();
				byte length = database.readByte();
				byte[] bytes = new byte[length];
				database.read(bytes, 0, bytes.length);
				if ((tableName.equals(new String(bytes))) && (isDeleted == 0)) {
					database.seek(database.getFilePointer() - length - 2L);
					database.writeByte(1);
					break;
				}
				database.readInt();
			}
			database.close();
			utility.markAllColumnsDeleted(tableName);
			File file = new File(utility.getSeletedDatabase() + "." + tableName + ".tbl");
			file.delete();
			System.out.println("Record is deleted Successfully");
		} catch (Exception e) {
			System.out.println("Error, while dropping a table");
		}
	}

	public void database(String userCommand) {
		try {
			String[] tokens = userCommand.split(" ");
			String databaseName = tokens[2];
			if(!utility.isDatabaseExist(databaseName)) {
				System.out.println("Database " + databaseName + " is not present");
				return;
			}			
			RandomAccessFile databases = new RandomAccessFile(IUtitlityConstants.ALL_DATABASE_TBL, "rw");
			while (databases.getFilePointer() < databases.length()) {
				int isDeleted = databases.readByte();
				byte length = databases.readByte();
				byte[] bytes = new byte[length];
				databases.read(bytes, 0, bytes.length);
				String databaseNameFromTable = new String(bytes);
				if ((isDeleted == 0) && (databaseNameFromTable.equals(databaseName))) {
					databases.seek(databases.getFilePointer() - databaseNameFromTable.length() - 2L);
					databases.writeByte(1);
					break;
				}
			}

			databases.close();

//			RandomAccessFile database = new RandomAccessFile(databaseName + ".tables.tbl", "rw");
//			while (database.getFilePointer() < database.length()) {
//				database.readByte();
//				byte length = database.readByte();
//				byte[] bytes = new byte[length];
//				database.read(bytes, 0, bytes.length);
//				String tableName = databaseName + "." + new String(bytes) + ".tbl";
//				File file = new File(tableName);
//				file.delete();
//				database.readInt();
//			}
//			database.close();
//			File file = new File(databaseName + ".tables.tbl");
//			file.delete();
//			file = new File(databaseName + ".columns.tbl");
//			file.delete();
			
			File delDB = new File(IUtitlityConstants.DATABASE_PATH + File.separator + databaseName);
			delDB.delete();
			System.out.println("Database is deleted Successfully");
		} catch (Exception e) {
			System.out.println("Error, while dropping a Database");
		}
	}
}

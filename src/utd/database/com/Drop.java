package utd.database.com;

import java.io.File;
import java.io.RandomAccessFile;

public class Drop {

    Utility utility = Utility.getInstance();

    public void table(String userCommand) {
        try {
            String[] tokens = userCommand.split(" ");
            String tableName = tokens[2];
            if (!utility.isTablePresent(tableName, true)) {
                return;
            }
            RandomAccessFile tables = new RandomAccessFile(IUtitlityConstants.ALL_TABLE_TBL, "rw");
            while (tables.getFilePointer() < tables.length()) {
                tables.readByte();
                byte lengthDB = tables.readByte();
                byte[] bytesDB = new byte[lengthDB];
                tables.read(bytesDB, 0, bytesDB.length);

                int isDeletedTable = tables.readByte();
                byte lengthTable = tables.readByte();
                byte[] bytestbl = new byte[lengthTable];
                tables.read(bytestbl, 0, bytestbl.length);
                String tblName = new String(bytestbl);
                String dbName = new String(bytesDB);
                if ((isDeletedTable == 0) && (dbName.equals(utility.getSeletedDatabase())) && tblName.equals(tableName)) {
                    tables.seek(tables.getFilePointer() - tblName.length() - 2L);
                    tables.writeByte(1);
                    break;
                }
            }

            tables.close();

            File deltable = new File(IUtitlityConstants.DATABASE_PATH + File.separator +
                    utility.getSeletedDatabase() + File.separator + tableName + ".tbl");
            deltable.delete();
            System.out.println("Table is deleted Successfully");
        } catch (Exception e) {
            System.out.println("Error, while dropping a Database");
        }
    }

    public void database(String userCommand) {
        try {
            String[] tokens = userCommand.split(" ");
            String databaseName = tokens[2];
            if (!utility.isDatabaseExist(databaseName)) {
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

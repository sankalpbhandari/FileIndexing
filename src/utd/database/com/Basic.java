package utd.database.com;

import java.io.RandomAccessFile;


class Basic {

    Utility utility = Utility.getInstance();

    void showTables() {
        try {
            RandomAccessFile table = new RandomAccessFile(IUtitlityConstants.ALL_TABLE_TBL, "rw");
            boolean isRecordPresent = false;
            if (utility.getSeletedDatabase() == null) {
                System.out.println("The database is deleted");
            } else {
                while (table.getFilePointer() < table.length()) {

                    int isDeletedDB = table.readByte();
                    byte lengthDB = table.readByte();
                    byte[] bytes = new byte[lengthDB];
                    table.read(bytes, 0, bytes.length);
                    int isDeletedTable = table.readByte();
                    byte lengthTable = table.readByte();
                    byte[] bytestbl = new byte[lengthTable];
                    table.read(bytestbl, 0, bytestbl.length);
                    if (isDeletedDB == 0 && isDeletedTable == 0 && new String(bytes).equals(utility.getSeletedDatabase())) {
                        isRecordPresent = true;
                        System.out.println(new String(bytestbl));
                    }
//				table.readByte();
                }
                if (!isRecordPresent)
                    System.out.println("No table is present for " + utility.getSeletedDatabase() + " database");
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error, while fetching values from database");
        }

    }

    void showDatabases() {
        try {
            java.io.File file = new java.io.File(IUtitlityConstants.ALL_DATABASE_TBL);
            if ((file.exists()) && (!file.isDirectory())) {
                RandomAccessFile databases = new RandomAccessFile(IUtitlityConstants.ALL_DATABASE_TBL, "rw");
                boolean isRecordPresent = false;
                while (databases.getFilePointer() < databases.length()) {
                    int isDeleted = databases.readByte();
                    byte length = databases.readByte();
                    byte[] bytes = new byte[length];
                    databases.read(bytes, 0, bytes.length);
                    if (isDeleted == 0) {
                        System.out.println(new String(bytes));
                        isRecordPresent = true;
                    }
                }
                if (!isRecordPresent)
                    System.out.println("No database is present in the system");
                databases.close();
            } else {
                System.out.println("No database is present");
            }
        } catch (Exception e) {
            System.out.println("Error, while fetching values from database" + e.getMessage());
        }
    }

    boolean checkDatabasePresent(String databaseName) {
        try {
            utility.setSeletedDatabase(null);
            RandomAccessFile databases = new RandomAccessFile(IUtitlityConstants.ALL_DATABASE_TBL, "rw");
            boolean isDatabasePresent = false;
            while (databases.getFilePointer() < databases.length()) {
                int isDeleted = databases.readByte();
                byte length = databases.readByte();
                byte[] bytes = new byte[length];
                databases.read(bytes, 0, bytes.length);
                if ((databaseName.equals(new String(bytes).trim())) && (isDeleted == 0)) {
                    utility.setSeletedDatabase(databaseName);
                    isDatabasePresent = true;
                }
            }
            databases.close();
            return isDatabasePresent;
        } catch (Exception e) {
            System.out.println("Error, while fetching values from database");
        }
        return false;
    }

    void useDatabase(String databaseName) {
        boolean isDatabasePresent;
        isDatabasePresent = checkDatabasePresent(databaseName);
        if (isDatabasePresent)
            System.out.println("Switched to database " + databaseName);
        else
            System.out.println("Database " + databaseName + " is not present in the system");
    }

}

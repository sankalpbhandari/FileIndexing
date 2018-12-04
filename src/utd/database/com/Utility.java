package utd.database.com;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Utility {
    private static Utility utility = new Utility();
    private final String DEFAULT_DATABASE = null;
    private final String DEFAULT_TABLE_COLUMN = null;
    private String selectedDatabase;

    private Utility() {

    }

    public static Utility getInstance() {
        return utility;
    }

    static String displayLine(String displayChar) {
        StringBuilder displayString = new StringBuilder();
        for (int i = 0; i < 80; i++) {
            displayString.append(displayChar);
        }
        return displayString.toString();
    }

    static void splashScreen() {
        System.out.println(displayLine("-"));
        System.out.println("Welcome to DavisBaseLite");
        System.out.println("DavisBaseLite Version " + getInstance().getVersion());
        System.out.println(getInstance().getCopyRight());
        System.out.println("\nType \"help;\" to display supported commands.");
        System.out.println(displayLine("-"));
    }

    List<Column> getColumns(String tableName) {
        List<Column> columns = new java.util.ArrayList<>();
        try {
            if (utility.isTablePresent(tableName, true)) {
                RandomAccessFile table = new RandomAccessFile(IUtitlityConstants.DATABASE_PATH + File.separator + utility.getSeletedDatabase() + File.separator + "columns.tbl", "rw");
                while (table.getFilePointer() < table.length()) {
                    int isDeleted = table.readByte();
                    byte length = table.readByte();
                    byte[] bytes = new byte[length];
                    table.read(bytes, 0, bytes.length);
                    String[] column = new String(bytes).replaceAll("#", " ").split(" ");
                    if ((column[0].equals(tableName)) && (isDeleted == 0)) {
                        Column c = new Column();
                        c.setColumnName(column[1]);
                        c.setDataType(column[2]);
                        c.setPrimary(false);
                        c.setNotNullable(false);
                        if (column.length == 4) {
                            if (column[3].equals("primarykey")) {
                                c.setPrimary(true);
                            } else if (column[3].equals("notnull")) {
                                c.setNotNullable(true);
                            }
                        }
                        columns.add(c);
                    }
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error");
        }

        return columns;
    }

    String[] getColumnName(String tableName) {
        java.util.List<Column> columns = getColumns(tableName);
        String[] columnNames = new String[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            columnNames[i] = columns.get(i).getColumnName();
        }
        return columnNames;
    }

    public void markAllColumnsDeleted(String tableName) {
        try {
            if (utility.isTablePresent(tableName, true)) {
                RandomAccessFile table = new RandomAccessFile(utility.getSeletedDatabase() + ".columns.tbl", "rw");
                while (table.getFilePointer() < table.length()) {
                    int isDeleted = table.readByte();
                    byte length = table.readByte();
                    byte[] bytes = new byte[length];
                    table.read(bytes, 0, bytes.length);
                    String[] column = new String(bytes).replaceAll("#", " ").split(" ");
                    if ((column[0].equals(tableName)) && (isDeleted == 0)) {
                        long tablePointer = table.getFilePointer();
                        table.seek(tablePointer - length - 1L);
                        table.writeByte(1);
                        table.seek(tablePointer);
                    }
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error");
        }
    }

    String getPrompt() {
        return IUtitlityConstants.PROMPT;
    }

    String getVersion() {
        return IUtitlityConstants.VERSION;
    }

    private String getCopyRight() {
        return IUtitlityConstants.COPYRIGHT;
    }

    long getPageSize() {
        return IUtitlityConstants.PAGESIZE;
    }

    public String getDefaultDatabase() {
        return DEFAULT_DATABASE;
    }

    public String getDefaultTableColumn() {
        return DEFAULT_TABLE_COLUMN;
    }

    String getSeletedDatabase() {
        Basic basic = new Basic();
        boolean checkDB = basic.checkDatabasePresent(selectedDatabase);
        if (checkDB)
            return selectedDatabase;
        else
            return null;
    }

    void setSeletedDatabase(String seletedDatabase) {
        this.selectedDatabase = seletedDatabase;
    }

    boolean isTablePresent(String tableName, boolean showMessage) {
        try {
            File file = new File(IUtitlityConstants.DATABASE_PATH + File.separator + getSeletedDatabase() + File.separator + tableName + ".tbl");
            if ((file.exists()) && (!file.isDirectory())) {
                return true;
            }
            if (showMessage) {
                System.out.println("Table " + tableName + " is not present");
            }
        } catch (Exception e) {
            return false;
        }

        return false;
    }

    long convertStringToDate(String dateString) {
        String pattern = "MM:dd:yyyy";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        try {
            Date date = format.parse(dateString);
            return date.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Date().getTime();
    }

    String convertDateToString(long date) {
        String pattern = "MM:dd:yyyy";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Date d = new Date(date);
        return format.format(d);
    }

    String convertDateTimeToString(long date) {
        String pattern = "YYYY-MM-DD_hh:mm:ss";
        SimpleDateFormat format = new SimpleDateFormat(pattern);
        Date d = new Date(date);
        return format.format(d);
    }

    boolean isDatabaseExist(String dbName) {
        File dBFile = new File("data" + File.separator + dbName);
        return dBFile.exists();
    }

    public String getDATABASE_PATH() {
        return IUtitlityConstants.DATABASE_PATH;
    }
}

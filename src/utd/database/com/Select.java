package utd.database.com;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Select {
    Utility utility = Utility.getInstance();

    public Select() {

    }

    public void select(String userCommand) {
        try {
            String user_col = StringUtils.substringBetween(userCommand, "select", "from").trim();
            String tableName, filter;
            String[] filterArray = new String[2];
            if (userCommand.contains("where")) {
                tableName = StringUtils.substringBetween(userCommand, "from", "where").trim();
                filter = StringUtils.substringAfter(userCommand, "where").trim();
                filterArray = filter.split("=");
            } else
                tableName = StringUtils.substringAfter(userCommand, "from").trim();
            String[] user_cols = new String[0];
            if (!user_col.equals("*")) {
                user_cols = StringUtils.substringBetween(userCommand, "select", "from").split(",");
                for (int i = 0; i < user_cols.length; i++)
                    user_cols[i] = user_cols[i].trim();
            }
            if (utility.isTablePresent(tableName, true)) {
                RandomAccessFile table = new RandomAccessFile(IUtitlityConstants.DATABASE_PATH +
                        File.separator + utility.getSeletedDatabase() + File.separator + tableName + ".tbl",
                        "rw");
                if (table.length() > 0L) {
                    java.util.List<Column> columns = utility.getColumns(tableName);
                    table.readByte();
                    int cells = table.readByte();
                    table.readShort();
                    long rightPointer = table.readInt();
                    ArrayList<Short> cellPointers = new ArrayList<Short>();
                    for (int i = 0; i < cells; i++) {
                        cellPointers.add(Short.valueOf(table.readShort()));
                    }
                    boolean nextPage = true;
                    if (user_col.equals("*")) {
                        for (int index = 0; index < columns.size(); index++) {
                            String name = columns.get(index).getColumnName();
                            System.out.print(name + "\t\t");
                        }
                    } else {
                        for (int index = 0; index < user_cols.length; index++) {
                            String name = user_cols[index];
                            System.out.print(name + "\t\t");
                        }
                    }
                    System.out.println();
                    while (nextPage) {
                        for (int i = 0; i < cellPointers.size(); i++) {
                            table.seek(((Short) cellPointers.get(i)).shortValue());
                            Map<String, String> map = new HashMap<>();
                            for (Column column : columns) {
                                if (column.getDataType().equals("int")) {
                                    map.put(column.getColumnName(), " " + table.readInt());
                                } else if (column.getDataType().equals("tinyint")) {
                                    map.put(column.getColumnName(), " " + table.readByte());
                                } else if (column.getDataType().equals("smallint")) {
                                    map.put(column.getColumnName(), " " + table.readShort());
                                } else if (column.getDataType().equals("bigint")) {
                                    map.put(column.getColumnName(), " " + table.readLong());
                                } else if (column.getDataType().equals("real")) {
                                    map.put(column.getColumnName(), " " + table.readFloat());
                                } else if (column.getDataType().equals("double")) {
                                    map.put(column.getColumnName(), " " + table.readDouble());
                                } else if (column.getDataType().equals("date")) {
                                    map.put(column.getColumnName(), " " + utility.convertDateToString(table.readLong()));
                                } else if (column.getDataType().equals("datetime")) {
                                    map.put(column.getColumnName(), " " + utility.convertDateTimeToString(table.readLong()));
                                } else {
                                    int length = table.readByte();
                                    byte[] bytes = new byte[length];
                                    table.read(bytes, 0, bytes.length);
                                    map.put(column.getColumnName(), " " + new String(bytes));
                                }
                            }
                            if (user_col.equals("*")) {
                                if (userCommand.contains("where")) {
                                    if (map.get(filterArray[0]).trim().equals(filterArray[1].trim())) {
                                        printAll(columns, map);
                                    }
                                } else {
                                    printAll(columns, map);
                                }
                            } else {
                                if (userCommand.contains("where")) {
                                    if (map.get(filterArray[0]).trim().equals(filterArray[1].trim())) {
                                        printSome(user_cols, map);
                                    }
                                } else {
                                    printSome(user_cols, map);
                                }
                            }
                        }
                        if (rightPointer != 0L) {
                            table.seek(rightPointer);
                            table.readByte();
                            cells = table.readByte();
                            table.readShort();
                            rightPointer = table.readInt();
                            cellPointers = new ArrayList<Short>();
                            for (int i = 0; i < cells; i++) {
                                cellPointers.add(Short.valueOf(table.readShort()));
                            }
                        } else {
                            nextPage = false;
                        }
                    }
                } else {
                    System.out.println("No record present");
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error, While fectching records from table");
        }
    }

    private void printSome(String[] user_cols, Map<String, String> map) {
        for (String user_col : user_cols) {
            String name = map.get(user_col);
            System.out.print(name + "\t\t");
        }
        System.out.println();
    }

    private void printAll(List<Column> columns, Map<String, String> map) {
        for (Column column : columns) {
            String name = map.get(column.getColumnName());
            System.out.print(name + "\t\t");
        }
        System.out.println();
    }
    public boolean isKeyAlreadyPresent(String userCommand) {
        try {
            String[] tokens = userCommand.split(" ");
            String tableName = tokens[3].trim();
            if (utility.isTablePresent(tableName, true)) {
                String filter = userCommand.substring(userCommand.indexOf("where") + 5, userCommand.length()).trim();
                String[] filterArray = filter.split("=");
                RandomAccessFile table = new RandomAccessFile(IUtitlityConstants.DATABASE_PATH+
                        File.separator+utility.getSeletedDatabase() + File.separator + tableName + ".tbl",
                        "rw");
                if (table.length() > 0L) {
                    java.util.List<Column> columns = utility.getColumns(tableName);

                    table.readByte();
                    int cells = table.readByte();
                    table.readShort();
                    long rightPointer = table.readInt();
                    ArrayList<Short> cellPointers = new ArrayList();
                    for (int i = 0; i < cells; i++) {
                        cellPointers.add(Short.valueOf(table.readShort()));
                    }
                    boolean nextPage = true;
                    while (nextPage) {
                        for (int i = 0; i < cellPointers.size(); i++) {
                            table.seek(((Short) cellPointers.get(i)).shortValue());

                            for (Column column : columns) {
                                if (column.getDataType().equals("int")) {
                                    String value = "" + table.readInt();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("tinyint")) {
                                    String value = "" + table.readByte();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("smallint")) {
                                    String value = "" + table.readShort();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("bigint")) {
                                    String value = "" + table.readLong();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("real")) {
                                    String value = "" + table.readFloat();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("double")) {
                                    String value = "" + table.readDouble();
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("date")) {
                                    String value = utility.convertDateToString(table.readLong());
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }

                                } else if (column.getDataType().equals("datetime")) {
                                    String value = utility.convertDateTimeToString(table.readLong());
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }
                                } else {
                                    int length = table.readByte();
                                    byte[] bytes = new byte[length];
                                    table.read(bytes, 0, bytes.length);
                                    String value = " " + new String(bytes);
                                    if (column.getColumnName().equals(filterArray[0])) {
                                        if (!value.equals(filterArray[1]))
                                            break;
                                        return true;
                                    }
                                }
                            }
                        }

                        if (rightPointer != 0L) {
                            table.seek(rightPointer);
                            table.readByte();
                            cells = table.readByte();
                            table.readShort();
                            rightPointer = table.readInt();
                            cellPointers = new ArrayList<Short>();
                            for (int i = 0; i < cells; i++) {
                                cellPointers.add(Short.valueOf(table.readShort()));
                            }
                        } else {
                            nextPage = false;
                        }
                    }
                }
                table.close();
            }
        } catch (Exception e) {
            System.out.println("Error, While fectching records from table");
        }

        return false;
    }

}

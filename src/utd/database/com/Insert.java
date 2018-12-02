package utd.database.com;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;

class Insert {

    Utility utility = Utility.getInstance();
    private Select select = new Select();

    private String[] getTokens(String userCommand) {
        userCommand = userCommand.replace('(', '#').replace(')', ' ').trim();
        return userCommand.split("#");
    }

    void insertRecord(String userCommand) {
        try {
            String dbName = utility.getSeletedDatabase();
            if (dbName == null) {
                System.out.println("Database not Found");
                return;
            }
            String[] tokens = getTokens(userCommand);
            String tableName = tokens[0].trim().split(" ")[2];
            java.util.List<Column> columns = utility.getColumns(tableName);
            java.util.List<String> columnNames = Arrays.asList(utility.getColumnName(tableName));

            int rows = 0;
            RandomAccessFile table = new RandomAccessFile(IUtitlityConstants.ALL_TABLE_TBL, "rw");
            long pos = 0L;
            while (table.getFilePointer() < table.length()) {
                table.readByte();
                byte lengthDB = table.readByte();
                byte[] bytes = new byte[lengthDB];
                table.read(bytes, 0, bytes.length);
                table.readByte();
                byte lengthTable = table.readByte();
                byte[] bytestbl = new byte[lengthTable];
                table.read(bytestbl, 0, bytestbl.length);
                rows = table.readInt();
                String databaseName = new String(bytes);
                String databaseTableName = new String(bytestbl);
                pos = table.getFilePointer();
                if (databaseTableName.equals(tableName) && databaseName.equals(utility.getSeletedDatabase())) {
                    rows++;
                    table.seek(table.getFilePointer() - 4L);
                    table.writeInt(rows);
                    break;
                }
            }
            int index = 1;
            String values[] = new String[columns.size()];
            for (int i = 0; i < values.length; i++)
                values[i] = "null";
            if (tokens.length == 3) {
                String[] user_cols = tokens[1].replace("values", "").trim().split(",");
                index = 2;
                String[] user_values = tokens[index].trim().split(",");
                if (user_cols.length != user_values.length) {
                    System.out.println("Incorrect size of tuple");
                    return;
                }
                int new_index;
                values[0] = "" + rows;
                for (int i = 0; i < user_cols.length; i++) {
                    new_index = columnNames.indexOf(user_cols[i].trim());
                    if (new_index == -1) {
                        System.out.println("Column " + user_cols[i] + "  not found");
                        return;
                    }
                    values[new_index] = user_values[i].trim();
                }
            }
            if (index == 1) {
                tokens[index] = (rows + "," + tokens[index]);
                values = tokens[index].trim().split(",");
            }
            int recordSize = 0;
            boolean isError = false;
            if (columns.size() == values.length) {
                for (int i = 0; i < values.length; i++) {
                    if ((columns.get(i).isNotNullable()) || (columns.get(i).isPrimary())) {
                        if ((values[i] == null) || (values[i].equals("null"))) {
                            isError = true;
                        }
                        if (columns.get(i).isPrimary()) {
                            isError = select.isKeyAlreadyPresent("select * from " + tableName + " where "
                                    + columns.get(i).getColumnName() + "=" + values[i]);
                        }
                        if (isError)
                            break;
                    }
                    switch (columns.get(i).getDataType()) {
                        case "int":
                            recordSize += 4;
                            break;
                        case "tinyint":
                            recordSize++;
                            break;
                        case "smallint":
                            recordSize += 2;
                            break;
                        case "bigint":
                            recordSize += 8;
                            break;
                        case "real":
                            recordSize += 4;
                            break;
                        case "double":
                            recordSize += 8;
                            break;
                        case "date":
                            recordSize += 8;
                            break;
                        case "datetime":
                            recordSize += 8;
                            break;
                        default:
                            recordSize += values[i].length() + 1;
                            break;
                    }
                }
            } else {
                System.out.println("Incorrect size of tuple");
                return;
            }

            if (!isError) {
                table.seek(pos);
                BPlusTree btree = new BPlusTree();
                BPlusTree.tableName = IUtitlityConstants.DATABASE_PATH + File.separator + utility.getSeletedDatabase() + File.separator + tableName + ".tbl";
                long pointer = btree.insert(recordSize);
                RandomAccessFile table_data = new RandomAccessFile(BPlusTree.tableName, "rw");
                table_data.seek(pointer);
                for (int i = 0; i < values.length; i++) {
                    String datatype = columns.get(i).getDataType();
                    if (values[i].equals("null") || values[i] == null)
                        datatype = "text";
                    switch (datatype) {
                        case "int":
                            table_data.writeInt(Integer.parseInt(values[i]));
                            break;
                        case "tinyint":
                            table_data.writeByte(Byte.parseByte(values[i]));
                            break;
                        case "smallint":
                            table_data.writeInt(Short.parseShort(values[i]));
                            break;
                        case "bigint":
                            table_data.writeLong(Long.parseLong(values[i]));
                            break;
                        case "real":
                            table_data.writeFloat(Float.parseFloat(values[i]));
                            break;
                        case "double":
                            table_data.writeDouble(Double.parseDouble(values[i]));
                            break;
                        case "date":
                            table_data.writeLong(utility.convertStringToDate(values[i]));
                            break;
                        case "datetime":
                            table_data.writeLong(Long.parseLong(values[i]));
                            break;
                        default:
                            table_data.writeByte(values[i].length());
                            table_data.writeBytes(values[i]);
                            break;
                    }
                }
                table_data.close();
                System.out.println("Record is inserted Successfully");
            } else {
                System.out.println("Primary key should be unique\nor\nNot Null Field can't be null");
            }
            table.close();
        } catch (Exception e) {
            System.out.println("Error, while inserting a record ");
        }
    }
}

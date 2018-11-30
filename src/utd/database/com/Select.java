package utd.database.com;

import java.io.RandomAccessFile;
import java.util.ArrayList;

public class Select 
{
	Utility utility = Utility.getInstance();
	
	public Select() 
	{
		
	}

	public void select(String userCommand) {
		try {
			String[] tokens = userCommand.split(" ");
			String tableName = tokens[3].trim();
			if (utility.isTablePresent(tableName, true)) {
				RandomAccessFile table = new RandomAccessFile(utility.getSeletedDatabase() + "." + tableName + ".tbl",
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
					while (nextPage) {
						for (int i = 0; i < cellPointers.size(); i++) {
							table.seek(((Short) cellPointers.get(i)).shortValue());
							for (Column column : columns) {
								if (column.getDataType().equals("int")) {
									System.out.print(" " + table.readInt());
								} else if (column.getDataType().equals("tinyint")) {
									System.out.print(" " + table.readByte());
								} else if (column.getDataType().equals("smallint")) {
									System.out.print(" " + table.readShort());
								} else if (column.getDataType().equals("bigint")) {
									System.out.print(" " + table.readLong());
								} else if (column.getDataType().equals("real")) {
									System.out.print(" " + table.readFloat());
								} else if (column.getDataType().equals("double")) {
									System.out.print(" " + table.readDouble());
								} else if (column.getDataType().equals("date")) {
									System.out.print(" " + utility.convertDateToString(table.readLong()));
								} else if (column.getDataType().equals("datetime")) {
									System.out.print(" " + utility.convertDateTimeToString(table.readLong()));
								} else {
									int length = table.readByte();
									byte[] bytes = new byte[length];
									table.read(bytes, 0, bytes.length);
									System.out.print(" " + new String(bytes));
								}
							}
							System.out.println();
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
}

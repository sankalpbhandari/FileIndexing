package utd.database.com;

import java.io.RandomAccessFile;

public class Insert {

	Utility utility = Utility.getInstance();
	SelectWhere selectWhere = new SelectWhere();

	public String[] getTokens(String userCommand) {
		userCommand = userCommand.replace('(', '#').replace(')', ' ').trim();
		return userCommand.split("#");
	}

	public void insertRecord(String userCommand) {
		try {
			String[] tokens = getTokens(userCommand);
			String tableName = tokens[0].trim().split(" ")[2];
			java.util.List<Column> columns = utility.getColumns(tableName);

			int rows = 0;
			RandomAccessFile databases = new RandomAccessFile(utility.getSeletedDatabase() + ".tables.tbl", "rw");
			long pos = -1L;
			while (databases.getFilePointer() < databases.length()) {
				databases.readByte();
				byte length = databases.readByte();
				byte[] bytes = new byte[length];
				databases.read(bytes, 0, bytes.length);
				String databaseTableName = new String(bytes);
				pos = databases.getFilePointer();
				rows = databases.readInt();
				if (databaseTableName.equals(tableName)) {
					rows++;
					break;
				}
			}

			tokens[1] = (rows + "," + tokens[1]);
			String[] values = tokens[1].trim().split(",");

			int recordSize = 0;
			boolean isError = false;
			if (columns.size() == values.length) {
				for (int i = 0; i < values.length; i++) {
					if ((((Column) columns.get(i)).isNotNullable()) || (((Column) columns.get(i)).isPrimary())) {
						if ((values[i] == null) || (values[i] == "null")) {
							isError = true;
						}
						if (((Column) columns.get(i)).isPrimary()) {
							isError = selectWhere.isKeyAlreadyPresent("select * from " + tableName + " where "
									+ ((Column) columns.get(i)).getColumnName() + "=" + values[i]);
						}
						if (isError)
							break;
					}
					if (((Column) columns.get(i)).getDataType().equals("int")) {
						recordSize += 4;
					} else if (((Column) columns.get(i)).getDataType().equals("tinyint")) {
						recordSize++;
					} else if (((Column) columns.get(i)).getDataType().equals("smallint")) {
						recordSize += 2;
					} else if (((Column) columns.get(i)).getDataType().equals("bigint")) {
						recordSize += 8;
					} else if (((Column) columns.get(i)).getDataType().equals("real")) {
						recordSize += 4;
					} else if (((Column) columns.get(i)).getDataType().equals("double")) {
						recordSize += 8;
					} else if (((Column) columns.get(i)).getDataType().equals("date")) {
						recordSize += 8;
					} else if (((Column) columns.get(i)).getDataType().equals("datetime")) {
						recordSize += 8;
					} else {
						recordSize += values[i].length() + 1;
					}
				}
			}

			if (!isError) {
				databases.seek(pos);
				databases.writeInt(rows);
				BPlusTree btree = new BPlusTree();
				BPlusTree.tableName = utility.getSeletedDatabase() + "." + tableName + ".tbl";
				long pointer = btree.insert(recordSize);
				RandomAccessFile table = new RandomAccessFile(BPlusTree.tableName, "rw");
				table.seek(pointer);
				for (int i = 0; i < values.length; i++) {
					if (((Column) columns.get(i)).getDataType().equals("int")) {
						table.writeInt(Integer.parseInt(values[i]));
					} else if (((Column) columns.get(i)).getDataType().equals("tinyint")) {
						table.writeByte(Byte.parseByte(values[i]));
					} else if (((Column) columns.get(i)).getDataType().equals("smallint")) {
						table.writeInt(Short.parseShort(values[i]));
					} else if (((Column) columns.get(i)).getDataType().equals("bigint")) {
						table.writeLong(Long.parseLong(values[i]));
					} else if (((Column) columns.get(i)).getDataType().equals("real")) {
						table.writeFloat(Float.parseFloat(values[i]));
					} else if (((Column) columns.get(i)).getDataType().equals("double")) {
						table.writeDouble(Double.parseDouble(values[i]));
					} else if (((Column) columns.get(i)).getDataType().equals("date")) {
						table.writeLong(utility.convertStringToDate(values[i]));
					} else if (((Column) columns.get(i)).getDataType().equals("datetime")) {
						table.writeLong(Long.parseLong(values[i]));
					} else {
						table.writeByte(values[i].length());
						table.writeBytes(values[i]);
					}
				}
				table.close();
				System.out.println("Record is inserted Successfully");
			} else {
				System.out.println("Primary key should be unique");
				System.out.println("or");
				System.out.println("Nullable Field can't be null");
			}
			databases.close();
		} catch (Exception e) {
			System.out.println("Error, while inserting a record");
		}
	}
}

package utd.database.com;


public class Column {
    private String columnName;
    private String dataType;
    private boolean isPrimary;
    private boolean isNotNullable;

    public Column() {

    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public boolean isPrimary() {
        return isPrimary;
    }

    public void setPrimary(boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    public boolean isNotNullable() {
        return isNotNullable;
    }

    public void setNotNullable(boolean isNullable) {
        isNotNullable = isNullable;
    }
}

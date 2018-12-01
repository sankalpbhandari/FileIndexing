package utd.database.com;

class Help {

    void get() {
        System.out.println(Utility.displayLine("*"));
        System.out.println("SUPPORTED COMMANDS");
        System.out.println("All commands below are case insensitive");
        System.out.println();
        System.out.println("\tCREATE database database_name;                   Create new database.");
        System.out.println(
                "\tCREATE table table_name(id int, name varchar);   Create new table under respective database.");
        System.out.println("\tUSE database_name;                               Switched to new database");
        System.out.println(
                "\tSHOW tables;                                     Display all tables under respective database.");
        System.out.println(
                "\tSHOW databases;                                  Display all the databases present in the system.");
        System.out.println("\tINSERT into table_name values (value1, value2);  Display all records in the table.");
        System.out.println("\tSELECT * FROM table_name;                        Display all records in the table.");
        System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  Display records whose rowid is <id>.");
        System.out.println("\tDELETE * FROM table_name WHERE rowid = <value>;  Delete records whose rowid is <id>.");
        System.out.println("\tDROP TABLE table_name;                           Remove table data and its schema.");
        System.out.println("\tDROP Database database_name;                     Remove database and its table.");
        System.out.println("\tVERSION;                                         Show the program version.");
        System.out.println("\tHELP;                                            Show this help information");
        System.out.println("\tEXIT;                                            Exit the program");
        System.out.println();
        System.out.println();
        System.out.println(Utility.displayLine("*"));
    }
}

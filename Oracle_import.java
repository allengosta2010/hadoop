import org.apache.sqoop.tool.ImportTool;
import com.cloudera.sqoop.*;
import java.io.IOException;
import java.sql.*;
import java.util.Objects;

import static java.lang.System.exit;

public class Oracle_import {

    private static final String url = "jdbc:oracle:thin:rprt/rprt@(DESCRIPTION =\n" +
            "    (ADDRESS = (PROTOCOL = TCP)(HOST = e-scan)(PORT = 1521))\n" +
            "    (CONNECT_DATA =\n" +
            "      (SERVER = DEDICATED)\n" +
            "      (SERVICE_NAME = irbis)\n" +
            "    )))\n";

    public static void main(String[] args) throws SQLException, InterruptedException, IOException {
        String query = "UPDATE reports.hadoop_pgw_inputdata set STATE='1' where STATE='0'";
        String query1 = "Select * from reports.hadoop_pgw_inputdata";

        try(Connection con = DriverManager.getConnection(url);Statement stmt = con.createStatement();
        Statement stmt1 = con.createStatement();ResultSet rt = stmt1.executeQuery(query1))
        {
            while (rt.next()) {
                String date1 = rt.getString(7);
                if(Objects.equals(date1,"0"))
                {
                    export();
                    stmt.executeUpdate(query);
                    exit(1);
                }
                else
                {
                    System.out.println("No such column");
                }
            }
        }
        catch (SQLException sqlEx)
        {
            sqlEx.printStackTrace();
        }
        System.gc();
    }

    private static int export()
    {
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(url);

        options.setSqlQuery("select ID,SERVEDIMEISV,SERVEDIMSI,SERVEDMSISDN,FIRSTDATETIME,SECONDDATETIME from reports.hadoop_pgw_inputdata where $CONDITIONS and STATE='0' order by DATA_ID");
        options.setNumMappers(1);
        options.setTargetDir("/user/abulal/test");

        options.setHiveDatabaseName("pgw");
        options.setOverwriteHiveTable(true);
        options.setHiveTableName("pgw_oracle");
        options.setHiveImport(true);

        options.setFieldsTerminatedBy('\001');
        options.setLinesTerminatedBy('\n');
        options.getVerbose();
        return new ImportTool().run(options);
    }
}
import java.io.IOException;
import java.sql.*;

public class Oracle_update {
    private static final String url = "jdbc:oracle:thin:rprt/rprt@(DESCRIPTION =\n" +
            "    (ADDRESS = (PROTOCOL = TCP)(HOST = e-scan)(PORT = 1521))\n" +
            "    (CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = irbis))))\n";

    public static void main(String[] args) throws SQLException, InterruptedException, IOException {
        String query = "UPDATE reports.hadoop_pgw_inputdata set STATE='2' where STATE='1'";

        try(Connection con = DriverManager.getConnection(url); Statement stmt1 = con.createStatement())
        {
            ResultSet rt = stmt1.executeQuery(query);
            rt.close();
        }
        catch (SQLException sqlEx)
        {
            sqlEx.printStackTrace();
        }
        System.gc();
    }
}
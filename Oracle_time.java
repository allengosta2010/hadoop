import java.sql.*;
//import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
public class Oracle_time {
    private static final String url = "jdbc:oracle:thin:rprt/rprt@(DESCRIPTION =\n" +
            "    (ADDRESS = (PROTOCOL = TCP)(HOST = e-scan)(PORT = 1521))\n" +
            "    (CONNECT_DATA =\n" +
            "      (SERVER = DEDICATED)\n" +
            "      (SERVICE_NAME = irbis)\n" +
            "    )))\n";

    public static void main(String args[]) throws SQLException {
        Calendar calendar = Calendar.getInstance();
        java.sql.Timestamp ourJavaDateObject = new java.sql.Timestamp(calendar.getTime().getTime());
        String query = "INSERT INTO reports.hadoop_pgw_timedownload (LastTime) VALUES (?)";
        String query1 = "Truncate table reports.hadoop_pgw_timedownload";
        try (Connection con = DriverManager.getConnection(url); Statement st1 = con.createStatement(); PreparedStatement st = con.prepareStatement(query);) {
            st1.executeQuery(query1);
            st.setTimestamp(1, ourJavaDateObject);
            st.executeUpdate();

        } catch (SQLException sqlEx) {
            sqlEx.printStackTrace();
        }
        System.gc();
    }
}
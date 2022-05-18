import com.cloudera.sqoop.SqoopOptions;
import org.apache.sqoop.tool.ExportTool;
import java.sql.*;

public class Oracle_export {

    private static final String url = "jdbc:oracle:thin:rprt/rprt@(DESCRIPTION =\n" +
            "    (ADDRESS = (PROTOCOL = TCP)(HOST = e-scan)(PORT = 1521))\n" +
            "    (CONNECT_DATA =\n" +
            "      (SERVER = DEDICATED)\n" +
            "      (SERVICE_NAME = irbis)\n" +
            "    )))\n";

    public static void main(String[] args) throws SQLException {
        String query = "UPDATE reports.HADOOP_PGW_INPUTDATA set STATE='2' where STATE='1'";
        String query1 = "insert into reports.HADOOP_PGW_OUT \n" +
                "    select distinct pgw_out1.numrec,\n" +
                "        regexp_substr(pgw_out1.datavolumefbcdownlink, '[^/_]+',1,level) datavolumefbcdownlink,\n" +
                "        regexp_substr(pgw_out1.datavolumefbcuplink , '[^/_]+',1,level) datavolumefbcuplink,\n" +
                "        nvl(regexp_substr(pgw_out1.duration, '[^/_]+',1,level),0) duration,\n" +
                "        pgw_out1.pgwaddress,\n" +
                "        pgw_out1.servedimeisv,\n" +
                "        pgw_out1.servedimsi,\n" +
                "        pgw_out1.servedmsisdn,\n" +
                "        to_char(to_date(regexp_substr(pgw_out1.timeoffirstusage , '[^/_+]+',1,level),'yyyy.mm.dd HH24:MI:SS'),'dd.mm.yyyy HH24:MI:SS') timeoffirstusage,\n" +
                "        to_char(to_date(regexp_substr(pgw_out1.timeoflastusage , '[^/_+]+',1,level),'yyyy.mm.dd HH24:MI:SS'),'dd.mm.yyyy HH24:MI:SS') timeoflastusage,\n" +
                "        regexp_replace(pgw_out1.userlocationinformation,'[^[[:digit:]]]*') userlocationinformation,\n" +
                "        regexp_replace(pgw_out1.cell_id,'[^[[:digit:]]]*') cell_id,\n" +
                "        pgw_out1.servedpdp_pdnaddress,\n" +
                "        pgw_out1.id\n" +
                "        from reports.HADOOP_PGW_OUT_TEST pgw_out1\n " +
                "        where regexp_substr(pgw_out1.timeoffirstusage , '[^/_+]+',1,level)!='3:0'\n" +
                "        connect by pgw_out1.numrec=prior pgw_out1.numrec and prior dbms_random.value is not null\n" +
                "        and (   regexp_substr(pgw_out1.datavolumefbcdownlink, '[^/_]+',1,level) is not null\n" +
                "        or regexp_substr(pgw_out1.datavolumefbcuplink , '[^/_]+',1,level) is not null\n" +
                "        )\n" +
                "\n" +
                "\n" +
                "\n";
        String query2="TRUNCATE table reports.HADOOP_PGW_OUT_TEST";
        String query3 = "Select * from reports.HADOOP_PGW_OUT_TEST";
        export();
        try(Connection con = DriverManager.getConnection(url);Statement stmt = con.createStatement();ResultSet rt = stmt.executeQuery(query3))
        {

            stmt.executeUpdate(query1);
            stmt.executeUpdate(query2);
            stmt.executeUpdate(query);
        }
        catch (SQLException sqlEx)
        {
            sqlEx.printStackTrace();
        }
        System.gc();
    }

    public static int export()
    {
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(url);
        options.setTableName("reports.hadoop_pgw_out_test");
        options.setColumns(new String[]
                {
                        "numrec",
                        "DATAVOLUMEFBCDOWNLINK",
                        "DATAVOLUMEFBCUPLINK",
                        "DURATION",
                        "PGWADDRESS",
                        "SERVEDIMEISV",
                        "SERVEDIMSI",
                        "SERVEDMSISDN",
                        "TIMEOFFIRSTUSAGE",
                        "TIMEOFLASTUSAGE",
                        "USERLOCATIONINFORMATION",
                        "CELL_ID",
                        "SERVEDPDP_PDNADDRESS",
                        "ID"
                }
            );
        options.setExplicitOutputDelims(true);
        options.setExportDir("/user/hive/warehouse/pgw.db/pgw_out1/*.*");
        options.setNumMappers(1);
        options.setNullStringValue("null");
        options.setNullNonStringValue("null");
        options.setInputLinesTerminatedBy('\n');
        options.setInputFieldsTerminatedBy(';');
        int ret = new ExportTool().run(options);
        return ret;
    }
}
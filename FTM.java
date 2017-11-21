import java.sql.*;
public class FTM
{

	public static void main(String[] args) throws SQLException, ClassNotFoundException
	{
		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/PA2Bank", args[0], args[1]);
		DatabaseMetaData dbm = conn.getMetaData();
		ResultSet rs = dbm.getTables(null, null, "influence", null);
		Statement stmt_a = conn.createStatement();
		if(rs.next())
		{	
			stmt_a.executeUpdate("DROP TABLE influence");
		}
		stmt_a.executeUpdate("CREATE VIEW a_view AS SELECT src, tgt, timestamp, cname AS src_name FROM transfer LEFT JOIN depositor ON src = ano");
		stmt_a.executeUpdate("CREATE VIEW b_view AS SELECT src, tgt, timestamp, src_name, cname AS dest_name FROM a_view LEFT JOIN depositor ON tgt = ano");
		stmt_a.executeUpdate("SELECT src_name, dest_name, timestamp INTO name_transfer FROM b_view");
		Statement stmt_1 = conn.createStatement();
		Statement stmt_2 = conn.createStatement();
		Statement stmt_3 = conn.createStatement();
		Statement stmt_4 = conn.createStatement();
		Statement stmt_5 = conn.createStatement();
		Statement stmt_6 = conn.createStatement();
		Statement stmt_7 = conn.createStatement();
		Statement stmt_8 = conn.createStatement();
		Statement stmt_9 = conn.createStatement();
		Statement stmt_10 = conn.createStatement();
		String updateStr = "SELECT*INTO result_table FROM name_transfer";
		stmt_1.executeUpdate(updateStr);
		String updateStr_1 = "SELECT*INTO middle_table FROM name_transfer";
		stmt_2.executeUpdate(updateStr_1);
		ResultSet rset_1 = stmt_3.executeQuery("SELECT*FROM middle_table");
		ResultSet rset_T_old;
		String delete_temp_T = "DROP TABLE temp_T";
		String create_temp_T = "SELECT*INTO temp_T FROM result_table";
		String combined_view = "CREATE VIEW combinedView AS SELECT X.src_name, Y.dest_name, Y.timestamp from result_table X, result_table Y WHERE X.dest_name = Y.src_name AND X.timestamp <= Y.timestamp";
		PreparedStatement updateStud = null;
		while(rset_1.next())
		{
			rset_T_old = stmt_10.executeQuery("SELECT*FROM result_table");
			stmt_6.executeUpdate(combined_view);
			String updateStr_T = "INSERT INTO result_table SELECT*FROM combinedView";
			stmt_4.executeUpdate(updateStr_T);
			stmt_9.executeUpdate("DROP VIEW combinedView");
			stmt_5.executeUpdate(create_temp_T);
			while(rset_T_old.next())
			{
				String SRC = rset_T_old.getString(1);
				String DEST = rset_T_old.getString(2);
				updateStud = conn.prepareStatement("DELETE FROM temp_T WHERE src_name LIKE ? AND dest_name LIKE ?");
				updateStud.setString(1, SRC);
				updateStud.setString(2, DEST);
				updateStud.executeUpdate();
			}
			rset_1.close();
			rset_1 = stmt_7.executeQuery("SELECT*FROM temp_T");
			stmt_8.executeUpdate(delete_temp_T);
			rset_T_old.close();
		}
		stmt_a.executeUpdate("DROP TABLE middle_table");
		stmt_a.executeUpdate("DROP VIEW b_view");
		stmt_a.executeUpdate("DROP VIEW a_view");
		stmt_a.executeUpdate("DROP TABLE name_transfer");
		stmt_a.executeUpdate("CREATE VIEW result_view AS SELECT DISTINCT * FROM result_table");
		stmt_a.executeUpdate("SELECT DISTINCT src_name AS from, dest_name AS to INTO influence FROM result_view");
		stmt_a.executeUpdate("DROP VIEW result_view");
		stmt_a.executeUpdate("DROP TABLE result_table");
	}
}

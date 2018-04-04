import java.sql.*;
public class JDBC_TEST 
{

	public static void main(String[] args) throws SQLException, ClassNotFoundException
	{
		Class.forName("org.postgresql.Driver");
		Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/postgres");
		Statement stmt_1 = conn.createStatement();
		ResultSet result_set = stmt_1.executeQuery("SELECT*FROM customer");
		Statement stmt_2 = conn.createStatement();
		String updateStr = "INSERT INTO customer (name, credit) VALUES ('KK', 999)";
		stmt_2.executeUpdate(updateStr);
		while(result_set.next())
		{
			String name = result_set.getString(1);
			System.out.println(name);
		}
		result_set.close();
		result_set = stmt_1.executeQuery("SELECT*FROM customer");
		while(result_set.next())
		{
			String name = result_set.getString(1);
			System.out.println(name);
		}
	}
}

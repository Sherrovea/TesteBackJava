package testeConexao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class Main {

	public static void main(String[] args) throws SQLException {
		String jdbcUrl = "jdbc:postgresql://localhost:5432/teste";
	    String username = "sher";
	    String password = "sher";
	    String insertSQL = "INSERT INTO tb_customer_account(id_customer, cpf_cnpj, nm_customer, is_active, vl_total) VALUES (?, ?, ?, ?, ?)";
    	String selectMedia = "select * " + 
    			"from tb_customer_account " + 
    			"where vl_total > 560 " + 
    			"and id_customer > 1500 " + 
    			"and id_customer < 2700";
	    String selectOrdenacao = "select * " + 
	    		"from tb_customer_account " + 
	    		"where vl_total > 560 " + 
	    		"and id_customer > 1500 " + 
	    		"and id_customer < 2700 " + 
	    		"order by vl_total desc";
	    String truncate = "truncate tb_customer_account";
    	
	    Connection conn = null;
	    PreparedStatement stmt = null;
	    Statement stmt2 = null;
	    Statement stmt3 = null;
	    Statement stmt4 = null;
	    ResultSet rs = null;

	    try {
	    	conn = DriverManager.getConnection(jdbcUrl, username, password);

	      	stmt = conn.prepareStatement(insertSQL);
	 
	      	System.out.println("Começou a carga do banco");
	      	
			for(int i=1000; i < 3000; i++) {
			    stmt.setInt(1, i);
			    stmt.setString(2, "1234567" + String.valueOf(i));
			    stmt.setString(3, "teste " + String.valueOf(i));
			    stmt.setBoolean(4, true);
			    stmt.setDouble(5, i/4);
			    
			    stmt.executeUpdate();
			}
			
			System.out.println("Terminou a carga do banco");
			
			stmt2 = conn.createStatement();
		    rs = stmt2.executeQuery(selectMedia);

		    double media = 0;
		    int contador = 0;
		    
            while(rs.next()) {
		        media = media + rs.getDouble(5);
		        contador = contador + 1;
		    }
            
            System.out.println("A média é: " + media/contador);
            System.out.println("Quantidade registros: " + contador);
            
            stmt3 = conn.createStatement();
		    rs = stmt3.executeQuery(selectOrdenacao);

            while(rs.next()) {
		        System.out.println("Nome: " + rs.getString(3) + " .Valor: " + rs.getDouble(5));
		    }
            
            stmt4 = conn.createStatement();
		    stmt4.executeUpdate(truncate);

            
	    } catch (Exception e) {
	        e.printStackTrace();
	    } finally {
	    	if(conn != null)
	    		conn.close();
	    }
	}
}
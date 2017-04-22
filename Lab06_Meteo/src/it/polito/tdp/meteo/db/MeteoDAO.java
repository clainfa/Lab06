package it.polito.tdp.meteo.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.management.RuntimeErrorException;

import it.polito.tdp.meteo.bean.Rilevamento;

public class MeteoDAO {

	public List<Rilevamento> getAllRilevamenti() {

		final String sql = "SELECT Localita, Data, Umidita FROM situazione ORDER BY data ASC";

		List<Rilevamento> rilevamenti = new ArrayList<Rilevamento>();

		try {
			Connection conn = DBConnect.getInstance().getConnection();
			PreparedStatement st = conn.prepareStatement(sql);

			ResultSet rs = st.executeQuery();

			while (rs.next()) {

				Rilevamento r = new Rilevamento(rs.getString("Localita"), rs.getDate("Data"), rs.getInt("Umidita"));
				rilevamenti.add(r);
			}

			conn.close();
			return rilevamenti;

		} catch (SQLException e) {

			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	public List<Rilevamento> getAllRilevamentiLocalitaMese(int mese, String localita) {
		final String sql = "SELECT Data, Umidita FROM situazione WHERE Localita=? and MONTH(Data)=?";
		List<Rilevamento> rilevamentiLocalMese= new ArrayList<Rilevamento>();
		try{
			Connection conn= DBConnect.getInstance().getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, localita);
			st.setInt(2, mese);
			ResultSet res= st.executeQuery();
			if(res.next()){
				Rilevamento r= new Rilevamento(localita, res.getDate("Data"), res.getInt("Umidita"));
				rilevamentiLocalMese.add(r);
			}
			conn.close();
			return rilevamentiLocalMese;
			
		}catch(SQLException e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}
	/*
	 * voglio l'umidita media data una localita e un mese
	 */

	public Double getAvgRilevamentiLocalitaMese(int mese, String localita) {
		String sql = "SELECT AVG(Umidita) as UmiditaMedia FROM situazione WHERE Localita=? and MONTH(Data)=?";
		try{
			Connection conn = DBConnect.getInstance().getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1,localita);
			st.setInt(2, mese);
			ResultSet res = st.executeQuery();
			double result = 0.0;
			if(res.next()){
				result= res.getDouble("UmiditaMedia");
			}
			conn.close();
			return result;
			
		}catch(SQLException e){
			e.printStackTrace();
			throw new RuntimeException(e);
			
		}

		
	}

}

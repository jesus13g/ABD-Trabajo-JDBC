package lsi.ubu.servicios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.excepciones.AlquilerCochesException;
import lsi.ubu.util.PoolDeConexiones;

public class ServicioImpl implements Servicio {
	private static final Logger LOGGER = LoggerFactory.getLogger(ServicioImpl.class);

	private static final int DIAS_DE_ALQUILER = 4;

	public void alquilar(String nifCliente, String matricula, Date fechaIni, Date fechaFin) throws SQLException {
		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		Connection con = null;
		PreparedStatement st = null;
		ResultSet rs = null;
		
		if (fechaIni == null) {
			throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
		}
		
		if (fechaFin == null) {
			LOGGER.error("Se debe definir la fecha de fin.");
		}
		

		/*
		 * El calculo de los dias se da hecho
		 */
		long diasDiff = DIAS_DE_ALQUILER;
		if (fechaFin != null) {
			diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());
			if (diasDiff < 1) {
				throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
			}
		}
 
		try {
			con = pool.getConnection();
			con.setAutoCommit(false);
			
			
			st = con.prepareStatement("SELECT COUNT(*) FROM clientes WHERE NIF = ?");
			st.setString(1, nifCliente);
			rs = st.executeQuery();
			
			if (rs.next() && rs.getInt(1) == 0) {
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
			}
			
			rs.close();
			st.close();

			st = con.prepareStatement("SELECT COUNT(*) FROM vehiculos WHERE matricula = ?");
			st.setString(1, matricula);
			rs = st.executeQuery();
			
			if (rs.next() && rs.getInt(1) == 0) {
				System.out.println("Lanza excep de vehiculo inexistente");
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
			}
			
			rs.close();
			st.close();
			
			String SqlDisp = "SELECT COUNT(*) FROM reservas WHERE matricula = ? AND (fecha_ini <= ? AND fecha_fin >= ?)";
			st = con.prepareStatement(SqlDisp);
			st.setString(1, matricula);
			st.setDate(2, new java.sql.Date(fechaFin.getTime()));
			st.setDate(3, new java.sql.Date(fechaIni.getTime()));
			rs = st.executeQuery();
			
			if (rs.next() && rs.getInt(1) > 0) {
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_OCUPADO);
			}
			
			rs.close();
			st.close();
			
			st = con.prepareStatement("SELECT SEQ_RESERVAS.NEXTVAL FROM DUAL");
			rs = st.executeQuery();
			int idReserva = -1;
			if(rs.next()) {
				idReserva = rs.getInt(1);
			}
			
			rs.close();
			st.close();
			
			st = con.prepareStatement("INSERT INTO Reservas (idReserva, cliente, matricula, fecha_ini, fecha_fin) VALUES (?, ?, ?, ?, ?)");
			st.setInt(1, idReserva);
			st.setString(2, nifCliente);
			st.setString(3, matricula);
			st.setDate(4, new java.sql.Date(fechaIni.getTime()));
			st.setDate(5, new java.sql.Date(fechaFin.getTime()));
			
			st.executeUpdate();
			con.commit();
			
		} catch (SQLException e) {
			// Completar por el alumno
			if (con != null) try {con.rollback(); } catch (SQLException ex) {LOGGER.error(ex.getMessage());}
			LOGGER.debug(e.getMessage());

			throw e;

		} finally {
			if (rs != null) try {rs.close(); } catch (SQLException e) {}
			if (st != null) try {st.close(); } catch (SQLException e) {}
			if (con != null) try {con.close(); } catch (SQLException e) {}
		}
	}
}

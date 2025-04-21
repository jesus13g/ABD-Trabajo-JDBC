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

			// ### mi codigo ### //
			// Comprobar si el cliente existe
			st = con.prepareStatement("SELECT COUNT(*) FROM Clientes WHERE NIF = ?");
			st.setString(1, nifCliente);
			rs = st.executeQuery();
			
			if (rs.next() && rs.getInt(1) == 0) {
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
			}

			rs.close();
			st.close();

			// Comprobar si el vehiculo existe
			st = con.prepareStatement("SELECT COUNT(*) FROM Vehículos WHERE matricula = ?");
			st.setString(1, matricula);
			rs = st.executeQuery();

			if (rs.next() && rs.getInt(1) == 0) {
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_EXIST);
			}

			rs.close();
			st.close();

			// Comprobar si el vehiculo está disponible
			String sqlDisponibilidad = "SELECT COUNT(*) FROM Reservas WHERE matricula = ? AND (fecha_ini <= ? AND fecha_fin >= ?)";
			st = con.prepareStatement(sqlDisponibilidad);
			st.setString(1, matricula);
			st.setDate(2, new java.sql.Date(fechaFin.getTime()));
			st.setDate(3, new java.sql.Date(fechaIni.getTime()));
			rs = st.executeQuery();

			if (rs.next() && rs.getInt(1) > 0) {
				throw new AlquilerCochesException(AlquilerCochesException.VEHICULO_NO_DISPONIBLE);
			}



		} catch (SQLException e) {
			// Completar por el alumno

			LOGGER.debug(e.getMessage());

			throw e;

		} finally {
			/* A rellenar por el alumnado*/
		}
	}
}

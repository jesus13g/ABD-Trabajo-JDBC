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
			
			st = con.prepareStatement("SELECT COUNT(*) FROM Clientes WHERE NIF = ?");
			st.setString(1, nifCliente);
			rs = st.executeQuery();
			
			if (rs.next() && rs.getInt(1) == 0) {
				throw new AlquilerCochesException(AlquilerCochesException.CLIENTE_NO_EXIST);
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

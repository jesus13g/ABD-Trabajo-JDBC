package lsi.ubu.servicios;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
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
		
		boolean fechaFinWasNull = false;

		if (fechaIni == null) {
		    throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
		}

		if (fechaFin == null) {
		    fechaFinWasNull = true;
		    Calendar cal = Calendar.getInstance();
		    cal.setTime(fechaIni);
		    cal.add(Calendar.DAY_OF_MONTH, DIAS_DE_ALQUILER - 1);
		    fechaFin = cal.getTime();
		}
	
		/*
		 * El calculo de los dias se da hecho, modificado
		 */
		long diasDiff = TimeUnit.MILLISECONDS.toDays(fechaFin.getTime() - fechaIni.getTime());
		if (fechaFinWasNull) {
		    diasDiff += 1; // Sumar 1 solo si la fechaFin la calculamos nosotros
		}
		if (diasDiff < 1) {
		    throw new AlquilerCochesException(AlquilerCochesException.SIN_DIAS);
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
			
			st = con.prepareStatement("SELECT seq_reservas.NEXTVAL FROM DUAL");
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
			
			// Obtener nuevo número de factura
            st = con.prepareStatement("SELECT seq_num_fact.NEXTVAL FROM DUAL");
            rs = st.executeQuery();
            int nroFactura = -1;
            if (rs.next()) {
                nroFactura = rs.getInt(1);
            }
            rs.close();
            st.close();

            // Obtener precio por día
            st = con.prepareStatement("SELECT m.precio_cada_dia, m.id_modelo FROM Modelos m JOIN Vehiculos v ON m.id_modelo = v.id_modelo WHERE v.matricula = ?");
            st.setString(1, matricula);
            rs = st.executeQuery();
            double precioDia = 0.0;
            int idModelo = 0;
            if (rs.next()) {
                precioDia = rs.getDouble(1);
                idModelo = rs.getInt(2);
            }
            rs.close();
            st.close();

            double importeDias = diasDiff * precioDia;

            // Obtener capacidad de depósito y tipo de combustible
            st = con.prepareStatement("SELECT m.capacidad_deposito, m.tipo_combustible FROM Modelos m JOIN Vehiculos v ON m.id_modelo = v.id_modelo WHERE v.matricula = ?");
            st.setString(1, matricula);
            rs = st.executeQuery();
            double capacidadDeposito = 0.0;
            String tipoCombustible = "";
            if (rs.next()) {
                capacidadDeposito = rs.getDouble(1);
                tipoCombustible = rs.getString(2);
            }
            rs.close();
            st.close();

            // Obtener precio por litro
            st = con.prepareStatement("SELECT precio_por_litro FROM Precio_Combustible WHERE tipo_combustible = ?");
            st.setString(1, tipoCombustible);
            rs = st.executeQuery();
            double precioLitro = 0.0;
            if (rs.next()) {
                precioLitro = rs.getDouble(1);
            }
            rs.close();
            st.close();

            double importeDeposito = capacidadDeposito * precioLitro;

            // Insertar factura
            double importeTotal = importeDias + importeDeposito;
            st = con.prepareStatement("INSERT INTO Facturas (nroFactura, importe, cliente) VALUES (?, ?, ?)");
            st.setInt(1, nroFactura);
            st.setDouble(2, importeTotal);
            st.setString(3, nifCliente);
            st.executeUpdate();
            st.close();

            // Insertar línea de factura - alquiler
            st = con.prepareStatement("INSERT INTO Lineas_Factura (nroFactura, concepto, importe) VALUES (?, ?, ?)");
            String conceptoAlquiler = diasDiff + " dias de alquiler, vehiculo modelo " + idModelo;
            st.setInt(1, nroFactura);
            st.setString(2, conceptoAlquiler);
            st.setDouble(3, importeDias);
            st.executeUpdate();
            st.close();

            // Insertar línea de factura - depósito lleno
            st = con.prepareStatement("INSERT INTO Lineas_Factura (nroFactura, concepto, importe) VALUES (?, ?, ?)");
            String conceptoDeposito = "Deposito lleno de " + (int)capacidadDeposito + " litros de " + tipoCombustible;
            st.setInt(1, nroFactura);
            st.setString(2, conceptoDeposito);
            st.setDouble(3, importeDeposito);
            st.executeUpdate();
            st.close();
            
			con.commit();
			
		} catch (SQLException e) {
			// Completar por el alumno
			if (con != null) try {con.rollback(); } catch (SQLException ex) {LOGGER.error(ex.getMessage());}
			LOGGER.error(e.getMessage());

			throw e;

		} finally {
			if (rs != null) try {rs.close(); } catch (SQLException e) {}
			if (st != null) try {st.close(); } catch (SQLException e) {}
			if (con != null) try {con.close(); } catch (SQLException e) {}
		}
	}
}

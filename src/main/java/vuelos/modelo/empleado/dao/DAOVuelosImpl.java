package vuelos.modelo.empleado.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vuelos.modelo.empleado.beans.AeropuertoBean;
import vuelos.modelo.empleado.beans.AeropuertoBeanImpl;
import vuelos.modelo.empleado.beans.DetalleVueloBean;
import vuelos.modelo.empleado.beans.DetalleVueloBeanImpl;
import vuelos.modelo.empleado.beans.InstanciaVueloBean;
import vuelos.modelo.empleado.beans.InstanciaVueloBeanImpl;
import vuelos.modelo.empleado.beans.UbicacionesBean;
import vuelos.modelo.empleado.beans.UbicacionesBeanImpl;
import vuelos.modelo.empleado.dao.datosprueba.DAOVuelosDatosPrueba;

public class DAOVuelosImpl implements DAOVuelos {

	private static Logger logger = LoggerFactory.getLogger(DAOVuelosImpl.class);
	
	//conexión para acceder a la Base de Datos
	private Connection conexion;
	
	public DAOVuelosImpl(Connection conexion) {
		this.conexion = conexion;
	}

	@Override
	public ArrayList<InstanciaVueloBean> recuperarVuelosDisponibles(Date fechaVuelo, UbicacionesBean origen, UbicacionesBean destino)  throws Exception {
		/** 
		 * TODO Debe retornar una lista de vuelos disponibles para ese día con origen y destino según los parámetros. 
		 *      Debe propagar una excepción si hay algún error en la consulta.    
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOVuelosImpl(...)).  
		 */

		 
		
		//Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales.
		//return resultado;
		// Fin datos estáticos de prueba.
		
		ArrayList<InstanciaVueloBean> resultado = new ArrayList<InstanciaVueloBean>();
				   
		
		String sql = "select distinct nro_vuelo, modelo, dia_sale, hora_sale, hora_llega, tiempo_estimado, fecha, \r\n"
				+ "Codigo_Aero_Sale, Nombre_Aero_Sale, aOrigen.telefono as Tel_Aero_Sale, aOrigen.direccion\r\n"
				+ " as Dir_Aero_Sale, Codigo_Aero_Llega, Nombre_Aero_Llega, aDestino.telefono as Tel_Aero_Llega,\r\n"
				+ " aDestino.direccion as Dir_Aero_Llega from vuelos_disponibles, aeropuertos as aOrigen, \r\n"
				+ " aeropuertos as aDestino where Ciudad_Sale = '"+origen.getCiudad()+"' and Ciudad_Llega = '"+destino.getCiudad()+"'\r\n"
				+ " and fecha = '"+(fechaVuelo.getYear()+1900)+"-"+(fechaVuelo.getMonth()+1)+"-"+fechaVuelo.getDate()+"' "
				+ "and aOrigen.codigo = Codigo_Aero_Sale and aDestino.codigo = Codigo_Aero_Llega; ";

		logger.debug("SQL: {}", sql);
		
		try{ 		
			Statement select = conexion.createStatement();
			ResultSet rs= select.executeQuery(sql);
			while (rs.next()) {
				logger.debug("Se recuperó el item con nrovuelo {},modelo {}, diaSalida {}, horaSalida {}, horallegada {}, tiempo estimado {}, fecha vuelo {}"
						, rs.getString("nro_vuelo"), rs.getString("modelo"), rs.getString("dia_sale"), rs.getString("hora_sale"), rs.getString("hora_llega")
						,rs.getTime("tiempo_estimado"), rs.getDate("fecha")); 	
				InstanciaVueloBean b= new InstanciaVueloBeanImpl();
				b.setTiempoEstimado(rs.getTime("tiempo_estimado"));
				b.setNroVuelo(rs.getString("nro_vuelo"));
				b.setModelo(rs.getString("modelo"));
				b.setHoraSalida(rs.getTime("hora_sale"));
				b.setHoraLlegada(rs.getTime("hora_llega"));
				b.setFechaVuelo(rs.getDate("fecha"));
				b.setDiaSalida(rs.getString("dia_sale"));
				AeropuertoBean as = new AeropuertoBeanImpl();
				as.setCodigo(rs.getString("Codigo_Aero_Sale"));
				as.setDireccion(rs.getString("Dir_Aero_Sale"));
				as.setNombre(rs.getString("Nombre_Aero_Sale"));
				as.setTelefono(rs.getString("Tel_Aero_Sale"));
				as.setUbicacion(origen);
				AeropuertoBean al = new AeropuertoBeanImpl();
				al.setCodigo(rs.getString("Codigo_Aero_Llega"));
				al.setDireccion(rs.getString("Dir_Aero_Llega"));
				al.setNombre(rs.getString("Nombre_Aero_Llega"));
				al.setTelefono(rs.getString("Tel_Aero_Llega"));
				al.setUbicacion(destino);
				b.setAeropuertoSalida(as);
				b.setAeropuertoLlegada(al);
				resultado.add(b);			
			 }
			  
		} catch (SQLException ex)
		{			
			logger.error("SQLException: " + ex.getMessage());
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());
			throw new Exception("Error inesperado al consultar la B.D.");
		}
		return resultado;
		
	}

	@Override
	public ArrayList<DetalleVueloBean> recuperarDetalleVuelo(InstanciaVueloBean vuelo) throws Exception {
		/** 
		 * TODO Debe retornar una lista de clases, precios y asientos disponibles de dicho vuelo.		   
		 *      Debe propagar una excepción si hay algún error en la consulta.    
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOVuelosImpl(...)).
		 */
		//Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales.
		//ArrayList<DetalleVueloBean> resultado = DAOVuelosDatosPrueba.generarDetalles(vuelo);
		logger.debug("fecha: {}", vuelo.getFechaVuelo());
		
		ArrayList<DetalleVueloBean> resultado = new ArrayList<DetalleVueloBean>();
		String sql= "select precio, clase, asientos_disponibles  from vuelos_disponibles where nro_vuelo = '"+vuelo.getNroVuelo()+"' and "
				+ "fecha = '"+vuelo.getFechaVuelo() +"';";
				       

		logger.debug("SQL: {}", sql);
	
		try{ 		
			Statement select = conexion.createStatement();
			ResultSet rs= select.executeQuery(sql);
			while (rs.next()) {
				logger.debug("Se recuperó el item con nrovuelo {}, precio {}, clase {}, asientos disponibles {}"
						, vuelo.getNroVuelo(), rs.getLong("precio"), rs.getString("clase"), rs.getInt("asientos_disponibles")); 	
				DetalleVueloBean d= new DetalleVueloBeanImpl();
				d.setAsientosDisponibles(rs.getInt("asientos_disponibles"));
				d.setClase( rs.getString("clase"));
				d.setPrecio( rs.getLong("precio"));
				d.setVuelo(vuelo);
				resultado.add(d);			
			 }
			  
		} catch (SQLException ex)
		{			
			logger.error("SQLException: " + ex.getMessage());
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());
			throw new Exception("Error inesperado al consultar la B.D.");
		}
	
		return resultado; 
		// Fin datos estáticos de prueba.
	}
}

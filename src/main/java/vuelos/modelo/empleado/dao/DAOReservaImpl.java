package vuelos.modelo.empleado.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vuelos.modelo.empleado.beans.AeropuertoBean;
import vuelos.modelo.empleado.beans.AeropuertoBeanImpl;
import vuelos.modelo.empleado.beans.DetalleVueloBean;
import vuelos.modelo.empleado.beans.DetalleVueloBeanImpl;
import vuelos.modelo.empleado.beans.EmpleadoBean;
import vuelos.modelo.empleado.beans.EmpleadoBeanImpl;
import vuelos.modelo.empleado.beans.InstanciaVueloBean;
import vuelos.modelo.empleado.beans.InstanciaVueloBeanImpl;
import vuelos.modelo.empleado.beans.InstanciaVueloClaseBean;
import vuelos.modelo.empleado.beans.InstanciaVueloClaseBeanImpl;
import vuelos.modelo.empleado.beans.PasajeroBean;
import vuelos.modelo.empleado.beans.PasajeroBeanImpl;
import vuelos.modelo.empleado.beans.ReservaBean;
import vuelos.modelo.empleado.beans.ReservaBeanImpl;
import vuelos.modelo.empleado.beans.UbicacionesBean;
import vuelos.modelo.empleado.beans.UbicacionesBeanImpl;
import vuelos.modelo.empleado.dao.datosprueba.DAOReservaDatosPrueba;

public class DAOReservaImpl implements DAOReserva {

	private static Logger logger = LoggerFactory.getLogger(DAOReservaImpl.class);
	
	//conexión para acceder a la Base de Datos
	private Connection conexion;
	
	public DAOReservaImpl(Connection conexion) {
		this.conexion = conexion;
	}
		
	
	@Override
	public int reservarSoloIda(PasajeroBean pasajero, 
							   InstanciaVueloBean vuelo, 
							   DetalleVueloBean detalleVuelo,
							   EmpleadoBean empleado) throws Exception {
		logger.info("Realiza la reserva de solo ida con pasajero {}", pasajero.getNroDocumento());
		
		/**
		 * TODO (parte 2) Realizar una reserva de ida solamente llamando al Stored Procedure (S.P.) correspondiente. 
		 *      Si la reserva tuvo exito deberá retornar el número de reserva. Si la reserva no tuvo éxito o 
		 *      falla el S.P. deberá propagar un mensaje de error explicativo dentro de una excepción.
		 *      La demás excepciones generadas automáticamente por algun otro error simplemente se propagan.
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOReservaImpl(...)).
		 *		
		 * 
		 * @throws Exception. Deberá propagar la excepción si ocurre alguna. Puede capturarla para loguear los errores
		 *		   pero luego deberá propagarla para que el controlador se encargue de manejarla.
		 *
		 * try (CallableStatement cstmt = conexion.prepareCall("CALL PROCEDURE reservaSoloIda(?, ?, ?, ?, ?, ?, ?)"))
		 * {
		 *  ...
		 * }
		 * catch (SQLException ex){
		 * 			logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 *  		throw ex;
		 * } 
		 */
		
		int nroReserva = 0;
		try
		 {
			CallableStatement cstmt = conexion.prepareCall("CALL reservaSoloIda(?, ?, ?, ?, ?, ?, ?);");
			cstmt.setString(1, pasajero.getTipoDocumento());
			cstmt.setInt(2, pasajero.getNroDocumento()); 
			cstmt.setInt(3, empleado.getLegajo());
			cstmt.setString(4, vuelo.getNroVuelo()); 
			cstmt.setDate(5, vuelo.getFechaVuelo()); 
			cstmt.setString(6, detalleVuelo.getClase()); 
			cstmt.registerOutParameter(7, 0);
			cstmt.execute();
			nroReserva = cstmt.getInt(7);
			logger.debug("codigo de reserva: {}.", cstmt.getInt(7));
		 }
		 catch (SQLException ex){
			 logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
			 throw ex;
		}
		if (nroReserva == 0) {
			throw new Exception("No se pudo realizar la reserva. Verifique que los datos sean correctos, y que haya asientos disponibles.");
		}
		return nroReserva;
		// Fin datos estáticos de prueba.
	}
	
	@Override
	public int reservarIdaVuelta(PasajeroBean pasajero, 
				 				 InstanciaVueloBean vueloIda,
				 				 DetalleVueloBean detalleVueloIda,
				 				 InstanciaVueloBean vueloVuelta,
				 				 DetalleVueloBean detalleVueloVuelta,
				 				 EmpleadoBean empleado) throws Exception {
		
		logger.info("Realiza la reserva de ida y vuelta con pasajero {}", pasajero.getNroDocumento());
		/**
		 * TODO (parte 2) Realizar una reserva de ida y vuelta llamando al Stored Procedure (S.P.) correspondiente. 
		 *      Si la reserva tuvo exito deberá retornar el número de reserva. Si la reserva no tuvo éxito o 
		 *      falla el S.P. deberá propagar un mensaje de error explicativo dentro de una excepción.
		 *      La demás excepciones generadas automáticamente por algun otro error simplemente se propagan.
		 *      
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOReservaImpl(...)).
		 * 
		 * @throws Exception. Deberá propagar la excepción si ocurre alguna. Puede capturarla para loguear los errores
		 *		   pero luego deberá propagarla para que se encargue el controlador.
		 *
		 * try (CallableStatement ... )
		 * {
		 *  ...
		 * }
		 * catch (SQLException ex){
		 * 			logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
		 *  		throw ex;
		 * } 
		 */
		
		int nroReserva = 0;
		try
		 {
			CallableStatement cstmt = conexion.prepareCall("CALL reservaIdaVuelta(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
			cstmt.setString(1, pasajero.getTipoDocumento());
			cstmt.setInt(2, pasajero.getNroDocumento()); 
			cstmt.setInt(3, empleado.getLegajo()); 
			cstmt.setString(4, vueloIda.getNroVuelo()); 
			cstmt.setDate(5, vueloIda.getFechaVuelo()); 
			cstmt.setString(6, detalleVueloIda.getClase()); 
			cstmt.registerOutParameter(7, 0);
			cstmt.setString(8, vueloVuelta.getNroVuelo()); 
			cstmt.setDate(9, vueloVuelta.getFechaVuelo()); 
			cstmt.setString(10, detalleVueloVuelta.getClase());
			
			cstmt.execute();
			nroReserva = cstmt.getInt(7);
		 }
		 catch (SQLException ex){
			 logger.debug("Error al consultar la BD. SQLException: {}. SQLState: {}. VendorError: {}.", ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
			 throw ex;
		}
		return nroReserva;
		// Fin datos estáticos de prueba.
	}
	
	@Override
	public ReservaBean recuperarReserva(int codigoReserva) throws Exception {
		
		logger.info("Solicita recuperar información de la reserva con codigo {}", codigoReserva);
		
		/**
		 * TODO (parte 2) Debe realizar una consulta que retorne un objeto ReservaBean donde tenga los datos de la
		 *      reserva que corresponda con el codigoReserva y en caso que no exista generar una excepción.
		 *
		 * 		Debe poblar la reserva con todas las instancias de vuelo asociadas a dicha reserva y 
		 * 		las clases correspondientes.
		 * 
		 * 		Los objetos ReservaBean además de las propiedades propias de una reserva tienen un arraylist
		 * 		con pares de instanciaVuelo y Clase. Si la reserva es solo de ida va a tener un unico
		 * 		elemento el arreglo, y si es de ida y vuelta tendrá dos elementos. 
		 * 
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOReservaImpl(...)).
		 */
		/*
		 * Importante, tenga en cuenta de setear correctamente el atributo IdaVuelta con el método setEsIdaVuelta en la ReservaBean
		 */
		// Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales.
		//ReservaBean reserva = DAOReservaDatosPrueba.getReserva();
		ReservaBean reserva = new ReservaBeanImpl();
		
		String sql = "select * from reservas, reserva_vuelo_clase, vuelos_disponibles where reservas.numero  = "+codigoReserva+" and reservas.numero = reserva_vuelo_clase.numero\r\n"
				+ "and vuelos_disponibles.fecha = reserva_vuelo_clase.fecha_vuelo and vuelos_disponibles.nro_vuelo = reserva_vuelo_clase.vuelo\r\n"
				+ "and vuelos_disponibles.clase = reserva_vuelo_clase.clase;";
		
		java.sql.Statement stmt = conexion.createStatement();
		ResultSet rs =  stmt.executeQuery(sql);
		
		if (rs != null) {
			rs.next();
			EmpleadoBean emp = new EmpleadoBeanImpl();
			DAOEmpleado daoEmp = new DAOEmpleadoImpl(this.conexion);
			emp = daoEmp.recuperarEmpleado(rs.getInt("legajo"));
			reserva.setEmpleado(emp);
			
			PasajeroBean pas = new PasajeroBeanImpl();
			DAOPasajero daoPas = new DAOPasajeroImpl(this.conexion);
			pas = daoPas.recuperarPasajero(rs.getString("doc_tipo"), rs.getInt("doc_nro"));
			reserva.setPasajero(pas);
			
			reserva.setNumero(codigoReserva);
			reserva.setEstado(rs.getString("estado"));
			reserva.setFecha(rs.getDate("fecha"));
			reserva.setVencimiento(rs.getDate("vencimiento"));
			
			ArrayList<InstanciaVueloClaseBean> listaVuelos = new ArrayList<InstanciaVueloClaseBean>();
			
			UbicacionesBean ubiAeroSaleIda = new UbicacionesBeanImpl();
			ubiAeroSaleIda.setCiudad(rs.getString("Ciudad_Sale"));
			ubiAeroSaleIda.setEstado(rs.getString("Estado_Sale"));
			ubiAeroSaleIda.setPais(rs.getString("Pais_Sale"));
			
			AeropuertoBean aeroSaleIda = new AeropuertoBeanImpl();
			aeroSaleIda.setCodigo(rs.getString("Codigo_Aero_Sale"));
			aeroSaleIda.setNombre(rs.getString("Nombre_Aero_Sale"));
			aeroSaleIda.setUbicacion(ubiAeroSaleIda);
			
			UbicacionesBean ubiAeroLlegaIda = new UbicacionesBeanImpl();
			ubiAeroLlegaIda.setCiudad(rs.getString("Ciudad_Llega"));
			ubiAeroLlegaIda.setEstado(rs.getString("Estado_Llega"));
			ubiAeroLlegaIda.setPais(rs.getString("Pais_Llega"));
			
			AeropuertoBean aeroLlegaIda = new AeropuertoBeanImpl();
			aeroLlegaIda.setCodigo(rs.getString("Codigo_Aero_Llega"));
			aeroLlegaIda.setNombre(rs.getString("Nombre_Aero_Llega"));
			aeroLlegaIda.setUbicacion(ubiAeroLlegaIda);
			
			InstanciaVueloBean instanciaIda = new InstanciaVueloBeanImpl();
			instanciaIda.setAeropuertoLlegada(aeroLlegaIda);
			instanciaIda.setAeropuertoSalida(aeroSaleIda);
			instanciaIda.setDiaSalida(rs.getString("dia_sale"));
			instanciaIda.setFechaVuelo(rs.getDate("fecha_vuelo"));
			instanciaIda.setHoraLlegada(rs.getTime("hora_llega"));
			instanciaIda.setHoraSalida(rs.getTime("hora_sale"));
			instanciaIda.setModelo(rs.getString("modelo"));
			instanciaIda.setNroVuelo(rs.getString("nro_vuelo"));
			instanciaIda.setTiempoEstimado(rs.getTime("tiempo_estimado"));
			
			
			DetalleVueloBean detalleIda = new DetalleVueloBeanImpl();
			detalleIda.setAsientosDisponibles(rs.getInt("asientos_disponibles"));
			detalleIda.setClase(rs.getString("Clase"));
			detalleIda.setPrecio(rs.getFloat("Precio"));
			detalleIda.setVuelo(instanciaIda);
			
			InstanciaVueloClaseBean ida = new InstanciaVueloClaseBeanImpl();
			ida.setClase(detalleIda);
			ida.setVuelo(instanciaIda);
			
			listaVuelos.add(ida);
			
			
			logger.debug("hay un vuelo de ida");
			logger.debug(String.valueOf(listaVuelos.size()));
			if(rs.next()) {
				reserva.setEsIdaVuelta(true);
				
				UbicacionesBean ubiAeroSaleVuelta = new UbicacionesBeanImpl();
				ubiAeroSaleVuelta.setCiudad(rs.getString("Ciudad_Sale"));
				ubiAeroSaleVuelta.setEstado(rs.getString("Estado_Sale"));
				ubiAeroSaleVuelta.setPais(rs.getString("Pais_Sale"));
				
				AeropuertoBean aeroSaleVuelta = new AeropuertoBeanImpl();
				aeroSaleVuelta.setCodigo(rs.getString("Codigo_Aero_Sale"));
				aeroSaleVuelta.setNombre(rs.getString("Nombre_Aero_Sale"));
				aeroSaleVuelta.setUbicacion(ubiAeroSaleVuelta);
				
				UbicacionesBean ubiAeroLlegaVuelta = new UbicacionesBeanImpl();
				ubiAeroLlegaVuelta.setCiudad(rs.getString("Ciudad_Llega"));
				ubiAeroLlegaVuelta.setEstado(rs.getString("Estado_Llega"));
				ubiAeroLlegaVuelta.setPais(rs.getString("Pais_Llega"));
				
				AeropuertoBean aeroLlegaVuelta = new AeropuertoBeanImpl();
				aeroLlegaVuelta.setCodigo(rs.getString("Codigo_Aero_Llega"));
				aeroLlegaVuelta.setNombre(rs.getString("Nombre_Aero_Llega"));
				aeroLlegaVuelta.setUbicacion(ubiAeroLlegaVuelta);
				
				InstanciaVueloBean instanciaVuelta = new InstanciaVueloBeanImpl();
				instanciaVuelta.setAeropuertoLlegada(aeroLlegaVuelta);
				instanciaVuelta.setAeropuertoSalida(aeroSaleVuelta);
				instanciaVuelta.setDiaSalida(rs.getString("dia_sale"));
				instanciaVuelta.setFechaVuelo(rs.getDate("fecha_vuelo"));
				instanciaVuelta.setHoraLlegada(rs.getTime("hora_llega"));
				instanciaVuelta.setHoraSalida(rs.getTime("hora_sale"));
				instanciaVuelta.setModelo(rs.getString("modelo"));
				instanciaVuelta.setNroVuelo(rs.getString("nro_vuelo"));
				instanciaVuelta.setTiempoEstimado(rs.getTime("tiempo_estimado"));
				
				
				DetalleVueloBean detalleVuelta = new DetalleVueloBeanImpl();
				detalleVuelta.setAsientosDisponibles(rs.getInt("asientos_disponibles"));
				detalleVuelta.setClase(rs.getString("Clase"));
				detalleVuelta.setPrecio(rs.getFloat("Precio"));
				detalleVuelta.setVuelo(instanciaVuelta);
				
				InstanciaVueloClaseBean vuelta = new InstanciaVueloClaseBeanImpl();
				vuelta.setClase(detalleVuelta);
				vuelta.setVuelo(instanciaVuelta);
				
				listaVuelos.add(vuelta);
				
				
				logger.debug("hay un vuelo de vuelta");
				logger.debug(rs.getString("vuelo"));
				logger.debug(String.valueOf(listaVuelos.size()));
			} else {
				reserva.setEsIdaVuelta(false);
			}
			
			
			reserva.setVuelosClase(listaVuelos);
			logger.debug("Se recuperó la reserva: {}, {}", reserva.getNumero(), reserva.getEstado());
			logger.debug(String.valueOf(listaVuelos.size()));
		} else {
			throw new Exception("No hay una reserva con esas características");
		}
		
		return reserva;
		// Fin datos estáticos de prueba.
	}


	

}

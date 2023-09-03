package vuelos.modelo.empleado.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import vuelos.modelo.empleado.beans.EmpleadoBean;
import vuelos.modelo.empleado.beans.EmpleadoBeanImpl;

public class DAOEmpleadoImpl implements DAOEmpleado {

	private static Logger logger = LoggerFactory.getLogger(DAOEmpleadoImpl.class);
	
	//conexión para acceder a la Base de Datos
	private Connection conexion;
	
	public DAOEmpleadoImpl(Connection c) {
		this.conexion = c;
	}


	@Override
	public EmpleadoBean recuperarEmpleado(int legajo) throws Exception {
		logger.info("recupera el empleado que corresponde al legajo {}.", legajo);
		
		/**
		 * TODO Debe recuperar de la B.D. los datos del empleado que corresponda al legajo pasado 
		 *      como parámetro y devolver los datos en un objeto EmpleadoBean. Si no existe el legajo 
		 *      deberá retornar null y si ocurre algun error deberá generar y propagar una excepción.	
		 *       
		 *      Nota: para acceder a la B.D. utilice la propiedad "conexion" que ya tiene una conexión
		 *      establecida con el servidor de B.D. (inicializada en el constructor DAOEmpleadoImpl(...)). 
		 */		
		
		/*
		 * Datos estáticos de prueba. Quitar y reemplazar por código que recupera los datos reales.  
		 */		
		String sql= "select *  from empleados where legajo = '"+legajo+"';";
		EmpleadoBean empleado = null;
		
		//hace consulta
		java.sql.Statement stmt = conexion.createStatement();
		ResultSet rs =  stmt.executeQuery(sql);
		if (rs != null) {
			rs.next();
			empleado = new EmpleadoBeanImpl();
			empleado.setLegajo(legajo);
			rs.getString("apellido");
			empleado.setApellido(rs.getString("apellido"));
			empleado.setNombre(rs.getString("nombre"));
			empleado.setTipoDocumento(rs.getString("doc_tipo"));
			empleado.setNroDocumento(rs.getInt("doc_nro"));
			empleado.setDireccion(rs.getString("direccion"));
			empleado.setTelefono(rs.getString("telefono"));
			empleado.setCargo(null);
			empleado.setPassword(rs.getString("password")); // md5(9);
			empleado.setNroSucursal(0);
		}
		
		return empleado;
		// Fin datos estáticos de prueba.
	}

}

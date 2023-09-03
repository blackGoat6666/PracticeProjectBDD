CREATE DATABASE vuelos;
USE vuelos;
DROP user IF EXISTS ''@localhost; 

CREATE TABLE ubicaciones (
 pais VARCHAR(20) NOT NULL, 
 estado VARCHAR(20) NOT NULL, 
 ciudad VARCHAR(20) NOT NULL,
 huso INT NOT NULL,


 CONSTRAINT pk_ubicaciones 
 PRIMARY KEY (pais, estado, ciudad),
 
 CONSTRAINT huso_Ck CHECK (huso BETWEEN -12 AND 12)
) ENGINE=InnoDB;

CREATE TABLE modelos_avion (
 modelo VARCHAR(20) NOT NULL, 
 fabricante VARCHAR(20) NOT NULL, 
 cabinas int UNSIGNED  NOT NULL,
 cant_asientos int UNSIGNED NOT NULL,
 
 
 CONSTRAINT pk_modelos_avion 
 PRIMARY KEY (modelo)
 
) ENGINE=InnoDB;

CREATE TABLE clases (
 nombre VARCHAR(20) NOT NULL, 
 porcentaje DECIMAL(2,2) UNSIGNED NOT NULL , 

 CONSTRAINT pk_modelos_avion 
 PRIMARY KEY (nombre),

CONSTRAINT porcentaje_Ck CHECK (porcentaje BETWEEN 0 AND 0.99)

) ENGINE=InnoDB;

CREATE TABLE comodidades (
 codigo INT UNSIGNED NOT NULL, 
 descripcion TEXT NOT NULL, 

 CONSTRAINT pk_comodidades
 PRIMARY KEY (codigo)
 

) ENGINE=InnoDB;

CREATE TABLE pasajeros (
 doc_tipo VARCHAR(45) NOT NULL, 
 doc_nro INT UNSIGNED NOT NULL,
 apellido VARCHAR(20) NOT NULL, 
 nombre VARCHAR(20) NOT NULL, 
 direccion VARCHAR(40) NOT NULL, 
 telefono VARCHAR(15) NOT NULL, 
 nacionalidad VARCHAR(20) NOT NULL, 
 
 CONSTRAINT pk_pasajeros
 PRIMARY KEY (doc_tipo, doc_nro)
 
) ENGINE=InnoDB;

CREATE TABLE empleados (
 legajo INT UNSIGNED NOT NULL, 
 password CHAR(32) NOT NULL, 
 doc_tipo VARCHAR(45) NOT NULL, 
 doc_nro INT UNSIGNED NOT NULL,
 apellido VARCHAR(20) NOT NULL, 
 nombre VARCHAR(20) NOT NULL, 
 direccion VARCHAR(40) NOT NULL, 
 telefono VARCHAR(15) NOT NULL, 

 CONSTRAINT pk_empleados
 PRIMARY KEY (legajo)
 ) ENGINE=InnoDB;
 
CREATE TABLE reservas (
 numero INT UNSIGNED NOT NULL AUTO_INCREMENT, 
 fecha DATE NOT NULL, 
 vencimiento DATE NOT NULL, 
 estado VARCHAR(15) NOT NULL, 
 doc_tipo VARCHAR(45) NOT NULL, 
 doc_nro INT UNSIGNED NOT NULL,
 legajo INT UNSIGNED NOT NULL,

 CONSTRAINT pk_reservas
 PRIMARY KEY (numero),
 
 CONSTRAINT FK_reservas_doc_tipo_doc_nro
 FOREIGN KEY (doc_tipo, doc_nro) REFERENCES pasajeros (doc_tipo, doc_nro)
 ON DELETE RESTRICT ON UPDATE CASCADE,
 
CONSTRAINT FK_reservas_legajo
FOREIGN KEY (legajo) REFERENCES empleados (legajo)
ON DELETE RESTRICT ON UPDATE CASCADE
 
) ENGINE=InnoDB;

CREATE TABLE posee (
 clase VARCHAR(20) NOT NULL,
 comodidad  INT UNSIGNED, 
 
 CONSTRAINT pk_posee
 PRIMARY KEY (clase, comodidad),
 
 CONSTRAINT FK_posee_clase
 FOREIGN KEY (clase) REFERENCES clases (nombre)
 ON DELETE RESTRICT ON UPDATE CASCADE,
 
 CONSTRAINT FK_posee_comodidad
 FOREIGN KEY (comodidad) REFERENCES comodidades (codigo)
 ON DELETE RESTRICT ON UPDATE CASCADE
 
) ENGINE=InnoDB;
CREATE TABLE aeropuertos (
 codigo VARCHAR(45) NOT NULL, 
 nombre VARCHAR(40) NOT NULL, 
 telefono VARCHAR(15) NOT NULL,
 direccion VARCHAR(30) NOT NULL,
 pais VARCHAR(20) NOT NULL, 
 estado VARCHAR(20) NOT NULL, 
 ciudad VARCHAR(20) NOT NULL,
 
 CONSTRAINT pk_aeropuertos
 PRIMARY KEY (codigo),
 
 CONSTRAINT FK_aeropuertos_pais_estado_ciudad
 FOREIGN KEY (pais, estado, ciudad) REFERENCES ubicaciones(pais, estado, ciudad)
 ON DELETE RESTRICT ON UPDATE CASCADE

) ENGINE=InnoDB;

CREATE TABLE vuelos_programados (
 numero VARCHAR(10) NOT NULL, 
 aeropuerto_salida VARCHAR(45) NOT NULL, 
 aeropuerto_llegada VARCHAR(45) NOT NULL, 

 CONSTRAINT pk_vuelos_programados 
 PRIMARY KEY (numero),
 
 CONSTRAINT FK_vuelos_programados_aeropuerto_salida
 FOREIGN KEY (aeropuerto_salida) REFERENCES aeropuertos(codigo)
    ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT FK_vuelos_programados_aeropuerto_llegada
 FOREIGN KEY (aeropuerto_llegada) REFERENCES aeropuertos(codigo)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB;

CREATE TABLE salidas (
 vuelo VARCHAR(10) NOT NULL, 
 dia enum('do','lu','ma','mi','ju','vi','sa'),
 hora_sale TIME NOT NULL,
 hora_llega TIME NOT NULL,
 modelo_avion VARCHAR(20) NOT NULL, 


 CONSTRAINT pk_salidas
 PRIMARY KEY (vuelo, dia),
 
 CONSTRAINT FK_salidas_vuelo
 FOREIGN KEY (vuelo) REFERENCES vuelos_programados(numero)
 ON DELETE RESTRICT ON UPDATE CASCADE,
 
 CONSTRAINT FK_salidas_modelo_avion
 FOREIGN KEY (modelo_avion) REFERENCES modelos_avion(modelo)
 ON DELETE RESTRICT ON UPDATE CASCADE
 
) ENGINE=InnoDB;



CREATE TABLE instancias_vuelo (
 vuelo VARCHAR(10) NOT NULL, 
 fecha DATE NOT NULL, 
 dia enum('do','lu','ma','mi','ju','vi','sa') NOT NULL,
 estado VARCHAR(15),

 CONSTRAINT pk_instancias_vuelo 
 PRIMARY KEY (vuelo, fecha),
 
 CONSTRAINT FK_instancias_vuelo_vuelo_dia
 FOREIGN KEY (vuelo, dia) REFERENCES salidas(vuelo, dia)
 ON DELETE RESTRICT ON UPDATE CASCADE
 
 ) ENGINE=InnoDB;

CREATE TABLE brinda (
 vuelo VARCHAR(10) NOT NULL,
 dia enum('do','lu','ma','mi','ju','vi','sa'),
 clase VARCHAR(20) NOT NULL, 
 precio DECIMAL(7,2) UNSIGNED NOT NULL,
 cant_asientos 	SMALLINT UNSIGNED NOT NULL, 
 
 CONSTRAINT pk_brinda
 PRIMARY KEY (vuelo, dia, clase),
 
 CONSTRAINT  FK_brinda_vuelo_dia
 FOREIGN KEY (vuelo, dia) REFERENCES salidas(vuelo, dia)
 ON DELETE RESTRICT ON UPDATE CASCADE,
 
 CONSTRAINT  FK_brinda_clase
 FOREIGN KEY (clase) REFERENCES clases(nombre)
 ON DELETE RESTRICT ON UPDATE CASCADE
 
) ENGINE=InnoDB;

CREATE TABLE reserva_vuelo_clase (
 numero INT UNSIGNED NOT NULL AUTO_INCREMENT, 
 vuelo VARCHAR(10) NOT NULL, 
 fecha_vuelo DATE NOT NULL, 
 clase VARCHAR(20) NOT NULL, 
 
 CONSTRAINT pk_reserva_vuelo_clase
 PRIMARY KEY (numero, vuelo, fecha_vuelo),
 
 CONSTRAINT FK_reserva_vuela_clase_numero
 FOREIGN KEY (numero) REFERENCES reservas(numero) 
 ON DELETE RESTRICT ON UPDATE CASCADE,

 CONSTRAINT FK_reserva_vuela_clase_vuelo_fecha_vuelo
 FOREIGN KEY (vuelo, fecha_vuelo) REFERENCES  instancias_vuelo(vuelo, fecha) 
 ON DELETE RESTRICT ON UPDATE CASCADE,
 
 CONSTRAINT FK_reserva_vuela_clase_clase
 FOREIGN KEY (clase) REFERENCES clases(nombre) 
 ON DELETE RESTRICT ON UPDATE CASCADE

) ENGINE=InnoDB;


CREATE TABLE asientos_reservados (
 vuelo VARCHAR(10) NOT NULL, 
 fecha DATE NOT NULL, 
 clase VARCHAR(20) NOT NULL, 
 cantidad INT UNSIGNED NOT NULL,
 
 CONSTRAINT pk_asientos_reservados
 PRIMARY KEY (vuelo, fecha, clase),
 
 CONSTRAINT FK_asientos_reservados_vuelo_fecha
 FOREIGN KEY (vuelo, fecha) REFERENCES instancias_vuelo(vuelo,fecha)
 ON DELETE RESTRICT ON UPDATE CASCADE,

 CONSTRAINT FK_asientos_reservados_clase
 FOREIGN KEY (clase) REFERENCES clases(nombre)
 ON DELETE RESTRICT ON UPDATE CASCADE
 
) ENGINE=InnoDB;

CREATE VIEW vuelos_disponibles as 
   SELECT DISTINCT 
          sal.vuelo as "nro_vuelo", sal.modelo_avion as "modelo", sal.dia as "dia_sale", sal.hora_sale as "hora_sale", sal.hora_llega as "hora_llega", 
          TIMEDIFF(TIMEDIFF(sal.hora_llega, MAKETIME(ubiaero2.huso,0,0)), TIMEDIFF(sal.hora_sale, MAKETIME(ubiaero1.huso,0,0))) as "tiempo_estimado",
          iv.fecha as "fecha",
          aero1.codigo as "Codigo_Aero_Sale", aero1.nombre as "Nombre_Aero_Sale", aero1.ciudad as "Ciudad_Sale", aero1.estado as "Estado_Sale", aero1.pais as "Pais_Sale", 
          aero2.codigo as "Codigo_Aero_Llega", aero2.nombre as "Nombre_Aero_Llega", aero2.ciudad as "Ciudad_Llega", aero2.estado as "Estado_Llega", aero2.pais as "Pais_Llega",
          b.precio as "Precio", c.nombre as "Clase",
          ((`b`.`cant_asientos` + ROUND((`c`.`porcentaje` * `b`.`cant_asientos`),
                0)) - `ar`.`cantidad`) AS `asientos_disponibles`
          
   FROM salidas sal JOIN instancias_vuelo iv ON sal.vuelo = iv.vuelo AND sal.dia = iv.dia 
        JOIN brinda b ON b.vuelo = sal.vuelo AND b.dia = sal.dia 
        JOIN clases c ON b.clase =c.nombre 
        JOIN vuelos_programados vp ON sal.vuelo = vp.numero 
        JOIN aeropuertos aero1 ON aero1.codigo = vp.aeropuerto_salida 
        JOIN ubicaciones as ubiaero1 ON aero1.pais = ubiaero1.pais AND aero1.estado = ubiaero1.estado AND aero1.ciudad = ubiaero1.ciudad  
        JOIN aeropuertos aero2 ON aero2.codigo = vp.aeropuerto_llegada
        JOIN ubicaciones as ubiaero2 ON aero2.pais = ubiaero2.pais AND aero2.estado = ubiaero2.estado AND aero2.ciudad = ubiaero2.ciudad
        JOIN asientos_reservados as ar ON sal.vuelo = ar.vuelo and iv.fecha = ar.fecha and c.nombre = ar.clase;
        
DELIMITER $$
USE `vuelos`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `reservaIdaVuelta`(doc_tipo VARCHAR(45), doc_nro INT, legajo INT, vuelo1 VARCHAR(10), fecha_vuelo1 DATE, clase1 VARCHAR(20), out nro_reserva int, vuelo2 VARCHAR(10), fecha_vuelo2 DATE, clase2 VARCHAR(20))
BEGIN
	DECLARE estado varchar(15);
	DECLARE no_error BOOLEAN DEFAULT TRUE;
	DECLARE nroreserva INT;
	DECLARE codigo_SQL  CHAR(5) DEFAULT '00000';	 
	DECLARE codigo_MYSQL INT DEFAULT 0;
	DECLARE mensaje_error TEXT;
    
     # mas info en: http://dev.mysql.com/doc/refman/5.0/en/declare-handler.html
     DECLARE EXIT HANDLER FOR SQLEXCEPTION 	 	 
	  BEGIN 
		GET DIAGNOSTICS CONDITION 1  codigo_MYSQL= MYSQL_ERRNO,  
		                             codigo_SQL= RETURNED_SQLSTATE, 
									 mensaje_error= MESSAGE_TEXT;
	    SELECT 'SQLEXCEPTION!, reserva abortada' AS resultado, 
		        codigo_MySQL, codigo_SQL,  mensaje_error;		
        ROLLBACK;
	  END;
	SET estado = "Confirmada";
    
    select * from asientos_reservados where asientos_reservados.vuelo = vuelo1
    and asientos_reservados.fecha = fecha_vuelo1 and asientos_reservados.clase = clase1 for update;
    
        select * from asientos_reservados where asientos_reservados.vuelo = vuelo2
    and asientos_reservados.fecha = fecha_vuelo2 and asientos_reservados.clase = clase2 for update;

    IF not EXISTS (SELECT * FROM pasajeros WHERE pasajeros.doc_nro = doc_nro and pasajeros.doc_tipo = doc_tipo) or 
		not EXISTS (SELECT * FROM empleados WHERE empleados.legajo = legajo) or 
        not EXISTS (SELECT * FROM instancias_vuelo WHERE instancias_vuelo.vuelo = vuelo1 and instancias_vuelo.fecha = fecha_vuelo1) or
        not EXISTS (SELECT * FROM instancias_vuelo WHERE instancias_vuelo.vuelo = vuelo2 and instancias_vuelo.fecha = fecha_vuelo2)
        then 
        SELECT "Alguno de los datos es incorrecto" AS Resultado;
        SET no_error = false;
end if;
    
IF (exists(select * from vuelos_disponibles WHERE vuelos_disponibles.nro_vuelo = vuelo1 and vuelos_disponibles.fecha = fecha_vuelo1
and vuelos_disponibles.Clase = clase1 and vuelos_disponibles.asientos_disponibles <= 0 ) or 
exists(select * from vuelos_disponibles WHERE vuelos_disponibles.nro_vuelo = vuelo2 and vuelos_disponibles.fecha = fecha_vuelo2
and vuelos_disponibles.Clase = clase2 and vuelos_disponibles.asientos_disponibles <= 0 )) THEN
SELECT "No hay asientos disponibles" AS Resultado;
        SET no_error = false;
END IF;

if (  exists (select * from brinda, asientos_reservados
	where brinda.vuelo = asientos_reservados.vuelo and brinda.clase = asientos_reservados.clase
	and asientos_reservados.clase = clase1 and asientos_reservados.vuelo = vuelo1 and asientos_reservados.fecha = fecha_vuelo1
	and brinda.cant_asientos < asientos_reservados.cantidad ) or
    exists(select * from brinda, asientos_reservados
	where brinda.vuelo = asientos_reservados.vuelo and brinda.clase = asientos_reservados.clase
	and asientos_reservados.clase = clase2 and asientos_reservados.vuelo = vuelo2 and asientos_reservados.fecha = fecha_vuelo2
	and brinda.cant_asientos < asientos_reservados.cantidad )  )then
	set estado = 'en espera';
end if;

START TRANSACTION;
if no_error then
    
	INSERT INTO reservas(doc_tipo, doc_nro, legajo, fecha, vencimiento, estado) 
	VALUES (doc_tipo, doc_nro, legajo, CONCAT(YEAR(NOW()),'-',month(now()),'-', day(now())),CONCAT((YEAR(NOW())+1),'-',month(now()),'-', day(now())), estado);
    
    select last_insert_id() into nroreserva;
    SET nro_reserva = nroreserva;
    
    INSERT INTO reserva_vuelo_clase(numero, vuelo, fecha_vuelo, clase) 
	VALUES (nroreserva, vuelo1, fecha_vuelo1, clase1);
    
	INSERT INTO reserva_vuelo_clase(numero, vuelo, fecha_vuelo, clase) 
	VALUES (nroreserva, vuelo2, fecha_vuelo2, clase2);
    
	UPDATE asientos_reservados SET cantidad = cantidad+1 where asientos_reservados.vuelo = vuelo1
    and asientos_reservados.fecha = fecha_vuelo1 and asientos_reservados.clase = clase1;
    
    UPDATE asientos_reservados SET cantidad = cantidad+1 where asientos_reservados.vuelo = vuelo2
    and asientos_reservados.fecha = fecha_vuelo2 and asientos_reservados.clase = clase2;
    
end if;
COMMIT;

END$$

DELIMITER ;
;


DELIMITER $$
USE `vuelos`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `reservaSoloIda`(doc_tipo VARCHAR(45), doc_nro INT, legajo INT, vuelo VARCHAR(10), fecha_vuelo DATE, clase VARCHAR(20), out numero_reserva int)
BEGIN
    DECLARE no_error BOOLEAN DEFAULT TRUE;
    DECLARE estado varchar(15);
    DECLARE numeroreserva INT;
    DECLARE codigo_SQL  CHAR(5) DEFAULT '00000';	 
	DECLARE codigo_MYSQL INT DEFAULT 0;
	DECLARE mensaje_error TEXT;
    
     # mas info en: http://dev.mysql.com/doc/refman/5.0/en/declare-handler.html
     DECLARE EXIT HANDLER FOR SQLEXCEPTION 	 	 
	  BEGIN 
		GET DIAGNOSTICS CONDITION 1  codigo_MYSQL= MYSQL_ERRNO,  
		                             codigo_SQL= RETURNED_SQLSTATE, 
									 mensaje_error= MESSAGE_TEXT;
	    SELECT 'SQLEXCEPTION!, reserva abortada' AS resultado, 
		        codigo_MySQL, codigo_SQL,  mensaje_error;		
        ROLLBACK;
	  END;
    
    SET estado = "Confirmada";
    
    select * from asientos_reservados where asientos_reservados.vuelo = vuelo
    and asientos_reservados.fecha = fecha_vuelo and asientos_reservados.clase = clase for update;
    
    IF not EXISTS (SELECT * FROM pasajeros WHERE pasajeros.doc_nro = doc_nro and pasajeros.doc_tipo = doc_tipo) or 
not EXISTS (SELECT * FROM empleados WHERE empleados.legajo = legajo) or 
        not EXISTS (SELECT * FROM instancias_vuelo WHERE instancias_vuelo.vuelo = vuelo and instancias_vuelo.fecha = fecha_vuelo)
        then 
        SELECT "Alguno de los datos es incorrecto" AS Resultado;
        SET no_error = false;
end if;
    
IF exists(select * from vuelos_disponibles WHERE vuelos_disponibles.nro_vuelo = vuelo and vuelos_disponibles.fecha = fecha_vuelo
and vuelos_disponibles.Clase = clase and vuelos_disponibles.asientos_disponibles <= 0 ) THEN
SELECT "No hay asientos disponibles" AS Resultado;
        SET no_error = false;
END IF;

if exists (select * from brinda, asientos_reservados
	where brinda.vuelo = asientos_reservados.vuelo and brinda.clase = asientos_reservados.clase
	and asientos_reservados.clase = clase and asientos_reservados.vuelo = vuelo and asientos_reservados.fecha = fecha_vuelo
	and brinda.cant_asientos < asientos_reservados.cantidad ) then
	set estado = 'en espera';
end if;

    
START TRANSACTION;
if no_error then
    
	INSERT INTO reservas( doc_tipo, doc_nro, legajo, fecha, vencimiento, estado) 
	VALUES ( doc_tipo, doc_nro, legajo, CONCAT(YEAR(NOW()),'-',month(now()),'-', day(now())),CONCAT((YEAR(NOW())+1),'-',month(now()),'-', day(now())), estado);
	
    select last_insert_id() into numeroreserva;
    SET numero_reserva = numeroreserva;
    
    INSERT INTO reserva_vuelo_clase(numero, vuelo, fecha_vuelo, clase) 
	VALUES (numeroreserva, vuelo, fecha_vuelo, clase);
    
	UPDATE asientos_reservados SET cantidad = cantidad+1 where asientos_reservados.vuelo = vuelo
    and asientos_reservados.fecha = fecha_vuelo and asientos_reservados.clase = clase;
end if;
COMMIT;
END$$

DELIMITER ;
;


CREATE USER 'admin'@'localhost'  IDENTIFIED BY 'admin';


GRANT ALL PRIVILEGES ON vuelos.* TO 'admin'@'localhost' WITH GRANT OPTION;

CREATE USER 'empleado'@'%'  IDENTIFIED BY 'empleado';
GRANT SELECT ON vuelos.* TO 'empleado'@'%';
GRANT EXECUTE ON vuelos.* TO 'empleado'@'%';
GRANT UPDATE, INSERT, DELETE ON vuelos.reservas TO 'empleado'@'%';
GRANT UPDATE, INSERT, DELETE ON vuelos.pasajeros TO 'empleado'@'%';
GRANT UPDATE, INSERT, DELETE ON vuelos.reserva_vuelo_clase TO 'empleado'@'%';

CREATE USER 'cliente'@'%'  IDENTIFIED BY 'cliente';
GRANT SELECT ON vuelos_disponibles to 'cliente'@'%' ;

DELIMITER $$

CREATE TRIGGER inicializar_asientos_reservados
AFTER INSERT ON instancias_vuelo
FOR EACH ROW
BEGIN
   -- Insertar registros en la tabla 'asientos_reservados' para cada clase
   INSERT INTO asientos_reservados (vuelo, fecha, clase, cantidad)
   SELECT NEW.vuelo, NEW.fecha, clases.nombre, 0
   FROM clases;
END$$

DELIMITER ;
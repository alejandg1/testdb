CREATE DATABASE ALQUILER_VEHICULOS
USE ALQUILER_VEHICULOS

CREATE TABLE MARCA
(
  IDMARCA INT PRIMARY KEY,
  NOMBRE VARCHAR(100) UNIQUE
)

CREATE TABLE MODELO
(
  IDMODELO INT PRIMARY KEY,
  IDMARCA INT REFERENCES MARCA(IDMARCA),
  NOMBRE VARCHAR(100) UNIQUE
)

CREATE TABLE AUTO
(
  IDAUTO INT PRIMARY KEY,
  IDMODELO INT REFERENCES MODELO(IDMODELO),
  PLACA VARCHAR(10) UNIQUE,
  ANIO INT DEFAULT 2023,
  DETALLES VARCHAR(500) NULL,
  COLOR VARCHAR(20),
  PRECIO REAL DEFAULT 0.00,
  ESTADO VARCHAR(50) DEFAULT 'DISPONIBLE'
)

CREATE TABLE CLIENTE
(
  IDCLIENTE INT PRIMARY KEY,
  NOMBRES VARCHAR(100) NOT NULL,
  CEDULA VARCHAR(10) UNIQUE,
  TELEFONO VARCHAR(10),
  FECHA_NAC DATE DEFAULT GETDATE(),
  GENERO INT DEFAULT 1,
)

CREATE TABLE VENTA
(
  IDVENTA INT PRIMARY KEY,
  IDCLIENTE INT REFERENCES CLIENTE(IDCLIENTE),
  FECHA_REGISTRO DATE DEFAULT GETDATE(),
  SUBTOTAL REAL DEFAULT 0.00,
  IVA REAL DEFAULT 0.00,
  DSCTO REAL DEFAULT 0.00,
  TOTAL REAL DEFAULT 0.00
)

CREATE TABLE VENTADETALLE
(
  IDVENTADETALLE INT PRIMARY KEY IDENTITY,
  IDVENTA INT REFERENCES VENTA(IDVENTA),
  IDAUTO INT REFERENCES AUTO(IDAUTO),
  SUBTOTAL REAL DEFAULT 0.00
)

-- Procedimiento almacenado
-- Crear un procedimiento almacenado para poder crear una venta con detalles aleatorios,se debe tomar en cuenta lo siguiente.
--     1. Se debe enviar como parámetro el número de cédula del cliente y la fecha registro.
--     2. El id de la venta se genera dentro del procedimiento.
--     3. El id de la tienda debe seleccionar de forma aleatoria de las que se tiene disponible, no se puede seleccionar tiendas que ya estén usadas en una venta.
--     4. Debe aplicar BEGIN, COMMIT, ROLLBACK y el empleo del TRY y CATCH.
--     5. Si la fecha de registro es nula o vacía debe tomar la fecha actual.
--     6. Si el cliente no existe entonces no debe realizar ningún proceso de inserción.
-- Cursor
-- En el mismo procedimiento almacenado crear un cursor que me permita insertar el detalle de los venta los registros de animales de forma aleatoria tomando en cuenta lo siguiente.
--     1. La cantidad de registros a insertar en el detalle será un número aleatorio, si el cliente su cédula empieza con un número par será 1 a 3 si es un número impar será de 4 a 6.
--     2. Los autos a insertar deben ser seleccionados de forma aleatoria y filtrando el año por el año de la fecha del registro y que su estado sea DISPONIBLE.
--     3. Se debe calcular los totales tanto del VENTADETALLE y VENTA por cada iteración del cursor.
--     4. Si el cliente nunca ha realizado una venta entonces se le otorgará un descuento del 35% en la venta.

CREATE OR ALTER PROCEDURE SP_VENTA
@CEDULA VARCHAR(10),
@FECHA_REGISTRO DATE
AS
  BEGIN TRY
      BEGIN TRANSACTION
          DECLARE @IDVENTA INT
          DECLARE @IDCLIENTE INT
          DECLARE @IDAUTO INT
          DECLARE @CANT INT
          DECLARE @PRECIO REAL
          DECLARE @SUBTOTAL REAL = 0.00
          DECLARE @IVA REAL = 0.00
          DECLARE @DSCTO REAL = 0.00
          DECLARE @TOTAL REAL = 0.00
          SELECT TOP 1 @IDCLIENTE = IDCLIENTE FROM CLIENTE WHERE CEDULA = @CEDULA
          IF @IDCLIENTE IS NULL
              RAISERROR('EL CLIENTE NO EXISTE EN LA BASE DE DATOS', 16, 1);
          IF @FECHA_REGISTRO IS NULL OR @FECHA_REGISTRO = ''
              SELECT @FECHA_REGISTRO = GETDATE()
          IF NOT EXISTS(SELECT * FROM VENTA WHERE IDCLIENTE = @IDCLIENTE)
             SET @DSCTO = 0.35
          SELECT @IDVENTA = ISNULL(MAX(IDVENTA)+1, 1) FROM VENTA
          INSERT INTO VENTA(IDVENTA,IDCLIENTE,FECHA_REGISTRO) VALUES(@IDVENTA,@IDCLIENTE,@FECHA_REGISTRO)
          SELECT @CANT = CASE
                    WHEN @CEDULA LIKE '[02468]%' THEN FLOOR(RAND() * (3 - 1) + 1)
                    ELSE FLOOR(RAND() * (6 - 4) + 4)
          END
          DECLARE CURSOR_AUTO CURSOR
          FOR SELECT TOP(@CANT) IDAUTO, PRECIO FROM AUTO WHERE ESTADO = 'DISPONIBLE' AND ANIO = YEAR(@FECHA_REGISTRO) ORDER BY NEWID()
          OPEN CURSOR_AUTO
          FETCH CURSOR_AUTO INTO @IDAUTO, @PRECIO
          WHILE @@FETCH_STATUS = 0
          BEGIN
               INSERT INTO VENTADETALLE VALUES(@IDVENTA,@IDAUTO,@PRECIO)
               FETCH CURSOR_AUTO INTO @IDAUTO, @PRECIO
          END
          CLOSE CURSOR_AUTO
          DEALLOCATE CURSOR_AUTO
          SELECT @SUBTOTAL = SUM(SUBTOTAL) FROM VENTADETALLE WHERE IDVENTA = @IDVENTA
          SET @IVA = @SUBTOTAL * 0.12
          SET @DSCTO = @SUBTOTAL * @DSCTO
          SET @TOTAL = @SUBTOTAL + @IVA - @DSCTO
          UPDATE VENTA SET SUBTOTAL=@SUBTOTAL,IVA=@IVA,DSCTO=@DSCTO,TOTAL=@TOTAL WHERE IDVENTA = @IDVENTA
      COMMIT TRANSACTION
  END TRY
BEGIN CATCH
  ROLLBACK TRANSACTION
  SELECT ERROR_MESSAGE()
END CATCH

-- Trigger
-- Crear un trigger para actualizar el estado de un vehículo cuando haya sido insertado un registro en la tabla VENTADETALLE para identificar los carros alquilados.

CREATE OR ALTER TRIGGER TR_VENTADETALLE
ON VENTADETALLE AFTER INSERT
AS
BEGIN
     DECLARE @IDAUTO INT
     SELECT @IDAUTO = IDAUTO FROM INSERTED
     UPDATE AUTO SET ESTADO = 'ALQUILADO' WHERE IDAUTO = @IDAUTO
END
CREATE DATABASE TIENDA_MASCOTAS
USE TIENDA_MASCOTAS

CREATE TABLE TIENDA
(
  IDTIENDA INT PRIMARY KEY,
  NOMBRE VARCHAR(100),
  TELEFONO VARCHAR(10),
  DIRECCION VARCHAR(100),
  EMAIL VARCHAR(50) 
)

CREATE TABLE RAZA
(
  IDRAZA INT PRIMARY KEY,
  NOMBRE VARCHAR(100) UNIQUE
)

CREATE TABLE TIPO
(
  IDTIPO INT PRIMARY KEY,
  NOMBRE VARCHAR(100) UNIQUE
)

CREATE TABLE ANIMAL
(
  IDANIMAL INT PRIMARY KEY,
  IDTIPO INT REFERENCES TIPO(IDTIPO),
  IDRAZA INT REFERENCES RAZA(IDRAZA),
  DETALLES VARCHAR(500) NULL,
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
  IDTIENDA INT REFERENCES TIENDA(IDTIENDA),
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
  IDANIMAL INT REFERENCES ANIMAL(IDANIMAL),
  SUBTOTAL REAL DEFAULT 0.00
)

-- Procedimiento almacenado
-- Crear un procedimiento almacenado para poder crear una venta con detalles aleatorios,se debe tomar en cuenta lo siguiente.
--     1. El único requisito a enviar en el procedimiento será la fecha de registro.
--     2. Se debe recorrer todos los clientes de mayor a edad a menor para crear las ventas.
--     3. El id de la venta se genera dentro del procedimiento.
--     4. El id de la tienda debe seleccionar de forma aleatoria de las que se tiene disponible.
--     5. Debe aplicar BEGIN, COMMIT, ROLLBACK y el empleo del TRY y CATCH.
--     6. La fecha va aumentando de 1 día en 1.
-- Cursor
-- En el mismo procedimiento almacenado crear un cursor que me permita insertar el detalle de los venta los registros de animales de forma aleatoria tomando en cuenta lo siguiente.
--     1. La cantidad de registros a insertar en el detalle será un número aleatorio entre 1 a 3.
--     2. Los animales a insertar debe ser de forma aleatoria y solo los que tengan estado disponible.
--     3. La cantidad en el registro de VENTADETALLE siempre será de 1.
--     4. Se debe calcular los totales tanto del VENTADETALLE y VENTA por cada iteración del cursor.
--     5. Si la edad está entre 40 a 50 años entonces se le otorgará un descuento del 40% en la venta.

CREATE OR ALTER PROCEDURE SP_VENTA
@FECHA_REGISTRO DATE
AS
  BEGIN TRY
      BEGIN TRANSACTION
          DECLARE @IDTIENDA INT = 0
          DECLARE @IDCLIENTE INT = 0
          DECLARE @EDAD INT    
          DECLARE @IDVENTA INT   
          DECLARE @IDANIMAL INT
          DECLARE @CANT INT
          DECLARE @PRECIO REAL = 0.00
          DECLARE @SUBTOTAL REAL = 0.00
          DECLARE @IVA REAL = 0.00
          DECLARE @DSCTO REAL = 0.00
          DECLARE @TOTAL REAL = 0.00
          DECLARE CURSOR_CLIENTE CURSOR
          FOR SELECT IDCLIENTE, DATEDIFF(YEAR, FECHA_NAC, GETDATE()) FROM CLIENTE WHERE DATEDIFF(YEAR, FECHA_NAC, GETDATE()) >= 18
          OPEN CURSOR_CLIENTE
          FETCH CURSOR_CLIENTE INTO @IDCLIENTE, @EDAD
          WHILE @@FETCH_STATUS = 0
          BEGIN
               SELECT TOP 1 @IDTIENDA = IDTIENDA FROM TIENDA ORDER BY NEWID()
			   SELECT @IDVENTA = ISNULL(MAX(IDVENTA)+1, 1) FROM VENTA
               INSERT INTO VENTA(IDVENTA,IDCLIENTE,IDTIENDA,FECHA_REGISTRO) VALUES(@IDVENTA,@IDCLIENTE,@IDTIENDA,@FECHA_REGISTRO)
               SELECT @CANT =  FLOOR(RAND() * (3 - 1) + 1)
               DECLARE CURSOR_ANIMAL CURSOR
               FOR SELECT TOP(@CANT) IDANIMAL, PRECIO FROM ANIMAL WHERE ESTADO = 'DISPONIBLE' ORDER BY NEWID()
               OPEN CURSOR_ANIMAL
               FETCH CURSOR_ANIMAL INTO @IDANIMAL, @PRECIO
               WHILE @@FETCH_STATUS = 0
               BEGIN
                   INSERT INTO VENTADETALLE VALUES(@IDVENTA,@IDANIMAL,@PRECIO)
                   FETCH CURSOR_ANIMAL INTO @IDANIMAL, @PRECIO
               END
               CLOSE CURSOR_ANIMAL
               DEALLOCATE CURSOR_ANIMAL
               SET @DSCTO = IIF(@EDAD BETWEEN 40 AND 50, 0.40, 0.00)
               SELECT @SUBTOTAL = SUM(SUBTOTAL) FROM VENTADETALLE WHERE IDVENTA = @IDVENTA
               SET @IVA = @SUBTOTAL * 0.12
               SET @DSCTO = @SUBTOTAL * @DSCTO
               SET @TOTAL = @SUBTOTAL + @IVA - @DSCTO
               UPDATE VENTA SET SUBTOTAL=@SUBTOTAL,IVA=@IVA,DSCTO=@DSCTO,TOTAL=@TOTAL WHERE IDVENTA = @IDVENTA
               SELECT @FECHA_REGISTRO = DATEADD(DAY, 1, @FECHA_REGISTRO)
               FETCH CURSOR_CLIENTE INTO @IDCLIENTE, @EDAD
          END
          CLOSE CURSOR_CLIENTE
          DEALLOCATE CURSOR_CLIENTE
      COMMIT TRANSACTION
  END TRY
BEGIN CATCH
  ROLLBACK TRANSACTION
  SELECT ERROR_MESSAGE()
END CATCH

-- Trigger
-- Crear un trigger para cuando se inserte un animal en el VENTADETALLE actualizar el estado del animal a vendido.

CREATE OR ALTER TRIGGER TR_VENTADETALLE
ON VENTADETALLE AFTER INSERT
AS
BEGIN
     DECLARE @IDANIMAL INT
     SELECT @IDANIMAL = IDANIMAL FROM INSERTED
     UPDATE ANIMAL SET ESTADO = 'VENDIDO' WHERE IDANIMAL = @IDANIMAL
END
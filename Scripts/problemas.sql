
update desechos set enabled  = 1
delete from inventario_materiales  



-- A) DIRTY READ 

-- query 1 
DECLARE @TVP1 IdsRecipientesType INSERT INTO @TVP1 SELECT idRecipiente FROM recipiente WHERE idRecipiente IN (1006)   
DECLARE @TVP2 IdsRecipientesType INSERT INTO @TVP2 SELECT idRecipiente FROM recipiente WHERE idRecipiente IN (1002)   
DECLARE @movimientos InfoMovimientoType INSERT INTO @movimientos (tipoDesecho, peso, recipientes) VALUES ('carton', '7', '1') 
EXEC RegistrarMovimiento @producerName = 'MC Donalds', @index = 1, @recipientesDando = @TVP1, @recipientesRecibiendo = @TVP2, @info = @movimientos, @idrecolector = 1, @idcamion = 1, @idresponsable = 1

-- query 2 
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechos @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

/* 
lo que va a pasar es que el query 1 va a tratar de insertar el movimiento pero al revisar que uno de los contenedores 
no es apto para el movimiento entonces va a provocar un error y va a hacer rollback, pero antes de que esto ocurra el query 2 va a haber leido 
los desechos insertados por el query 1 antes de hacer rollback, entonces va a intentar procesarlos pero en el momento que los vaya a procesar 
va a ver que estos desechos ya no existen por lo que va a generar un error al tener datos incorrectos
*/

-- DIRTY READ ARREGLADO

-- query 1 
DECLARE @TVP1 IdsRecipientesType INSERT INTO @TVP1 SELECT idRecipiente FROM recipiente WHERE idRecipiente IN (1006)   
DECLARE @TVP2 IdsRecipientesType INSERT INTO @TVP2 SELECT idRecipiente FROM recipiente WHERE idRecipiente IN (1002)   
DECLARE @movimientos InfoMovimientoType INSERT INTO @movimientos (tipoDesecho, peso, recipientes) VALUES ('carton', '7', '1') 
EXEC RegistrarMovimientoFinal @producerName = 'MC Donalds', @index = 1, @recipientesDando = @TVP1, @recipientesRecibiendo = @TVP2, @info = @movimientos, @idrecolector = 1, @idcamion = 1, @idresponsable = 1


-- query 2 
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosFinal @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp
/*
se cambia el isolation level a read committed en ProcesarDesechosFinal
se cambia el isolation level a repeatable read en RegistrarMovimientoFinal
y se ponen primero las validaciones en RegistrarMovimientoFinal para que no se inserte data incorrecta
*/


-- B) LOST UPDATE 

-- query 1
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton'),('organico');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosLU @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

-- query 2 
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechos @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

SELECT * FROM desechos

/*
el query 1 hace un update de los desechos de carton y organico pero luego va a revisar de que el organico no se puede procesar entonces hacer
rollback pero antes de que esto ocurra el query 2 hace el update pero esta vez completandolo correctamenet, pero al hacer el rollback del 
query 1 este segundo update se borra y se genera el LOST UPDATE
*/

-- LOST UPDATE ARREGLADO 
-- query 1
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton'),('organico');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosLUFinal @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

-- query 2 
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosFinal @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

SELECT * FROM desechos  
/*
se cambia el isolation level a repeatable read
*/





-- C) PHANTOM 

-- query 1 
EXEC ValidarDesechosMateriales @desechoacomparar = 'carton'


-- query 2
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechos @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp


/*
lo que esta ocurriendo es que se esta primero corriendo el query 1 el cual lee los pesos de los desehos procesados y luego el de los materiales 
en el inventario pero entre estas lecturas el query 2 esta marcando desechos como procesados y registrando materiales nuevos en el inventario
por lo que cuando se hace la lectura de los materiales y se hace la comparacion con los desechos estos no van a ser acorde debido a que se genera
el phantom de estos materiales
*/
 

-- PHANTOM ARREGLADO
-- query 1
EXEC ValidarDesechosMaterialesFinal @desechoacomparar = 'carton'

-- query 2
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosFinal @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

/*
se cambia el isolation level a read committed
y agregar lockhold en los select para que estos no cambien en ValidarDesechosMateriales
*/






-- D) DEADLOCK
-- query 1
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('plastico'),('aluminio');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosDL @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

-- query 2
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('aluminio'),('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosDL @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

/*
El query 1 y el query 2 ejecutan el procedimiento simultáneamente con diferentes conjuntos de parámetros. 
Ambas sesiones alcanzan el punto en el que seleccionan filas para procesar en la tabla "desechos". Sin embargo, antes de que el query 1
 pueda actualizar la tabla para deshabilitar las filas procesadas, el query 2 también selecciona las mismas filas. 
Como resultado, el query 1 intenta actualizar la tabla pero es bloqueada por el bloqueo compartido de el query 2. 
De manera similar, el query 2 intenta actualizar la tabla pero es bloqueada por el bloqueo compartido del query 1. 
Esto genera un bloqueo mutuo, ya que ambas sesiones esperan a que la otra libere los bloqueos, lo que genera el deadlock.
*/


-- query 1
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('plastico'),('aluminio');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosDLFinal @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

-- query 2
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('aluminio'),('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechosDLFinal @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp
/*
vi que al tener que prevenir el deadlock el serializble no funcionaba porque este mas bien ocasionaba que ocurriera
al quedarse el procedure esperando pero si lo pongo con read commited no ocurre
*/

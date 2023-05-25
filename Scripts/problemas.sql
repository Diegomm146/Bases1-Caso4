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
se cambia el isolation level a serializable en RegistrarMovimientoFinal
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
se cambia el isolation level a read committed
*/





-- C) PHANTOM 

-- query 1 
EXEC ValidarDesechosMateriales @desechoacomparar = 'carton'


-- query 2
DECLARE @tvp desechosaprocesar INSERT INTO @tvp (nombre) VALUES ('carton');
DECLARE @puntorecoleccion int  SET @puntorecoleccion = 1;

EXEC ProcesarDesechos @idpuntorecoleccion = @puntorecoleccion, @desechosaprocesar = @tvp

update desechos set enabled  = 1
delete from inventario_materiales  

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




-- JOB PARA STORED PROCEDURES
-- Crear el job para recompilar los stored procedures una vez por semana:
USE msdb;
GO
EXEC dbo.sp_add_job
    @job_name = N'RecompilarStoredProcedures',
    @enabled = 1,
    @description = N'Recompila todos los stored procedures una vez por semana';

-- Configurar el horario de ejecución (una vez por semana, los lunes a las 00:00)
EXEC dbo.sp_add_schedule
    @schedule_name = N'RecompilarStoredProceduresSchedule',
    @freq_type = 8, -- Weekly
    @freq_interval = 1, -- Monday
    @active_start_time = 0; -- 00:00

-- Asociar el job con el horario de ejecución
EXEC dbo.sp_attach_schedule
    @job_name = N'RecompilarStoredProcedures',
    @schedule_name = N'RecompilarStoredProceduresSchedule';

-- Configurar el paso del job para recompilar los stored procedures
EXEC dbo.sp_add_jobstep
    @job_name = N'RecompilarStoredProcedures',
    @step_name = N'RecompilarStoredProceduresStep',
    @command = N'USE EsencialVerde; EXEC sp_recompile;', -- Recompila todos los stored procedures en la base de datos especificada
    @database_name = N'YourDatabaseName',
    @on_success_action = 1; -- Quita de la lista de pasos

-- Iniciar el job manualmente
EXEC dbo.sp_start_job
    @job_name = N'RecompilarStoredProcedures';



DECLARE @RecompileSQL NVARCHAR(MAX) = N'';

SELECT @RecompileSQL += 'EXEC sp_recompile ''' + SCHEMA_NAME(schema_id) + '.' + name + ''';' + CHAR(13)
FROM sys.procedures;

EXEC sp_executesql @RecompileSQL;


USE msdb;
GO

-- Create the job
EXEC dbo.sp_add_job
    @job_name = 'Recompile Stored Procedures',
    @enabled = 1;

-- Add a description to the job
EXEC dbo.sp_add_jobstep
    @job_name = 'Recompile Stored Procedures',
    @step_name = 'Recompile SPs',
    @subsystem = 'TSQL',
    @command = N'
        DECLARE @RecompileSQL NVARCHAR(MAX) = N'''';  
        
        SELECT @RecompileSQL += ''EXEC sp_recompile '''''' + SCHEMA_NAME(schema_id) + '''''' + ''.'' + '''''' + name + '''''' + '';'' + CHAR(13)
        FROM sys.procedures;
        
        EXEC sp_executesql @RecompileSQL;
    ',
    @retry_attempts = 0,
    @on_success_action = 1;

-- Schedule the job to run once a week (Sundays at 2:00 AM)
EXEC dbo.sp_add_schedule
    @schedule_name = 'Weekly Execution',
    @freq_type = 8,
    @freq_interval = 1,
    @active_start_time = 20000;

-- Associate the job with the schedule
EXEC dbo.sp_attach_schedule
    @job_name = 'Recompile Stored Procedures',
    @schedule_name = 'Weekly Execution';

-- Start the job
EXEC dbo.sp_start_job @job_name = 'Recompile Stored Procedures';



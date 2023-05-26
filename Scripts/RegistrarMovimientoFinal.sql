SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[RegistrarMovimientoFinal]
    @producerName varchar(50),
    @index int,
    @recipientesDando IdsRecipientesType READONLY,
    @recipientesRecibiendo IdsRecipientesType READONLY,
    @info InfoMovimientoType READONLY,
    @idrecolector int,
    @idcamion int, 
    @idresponsable int
AS
BEGIN
	SET NOCOUNT ON -- no retorne metadatos
	DECLARE @ErrorNumber INT, @ErrorSeverity INT, @ErrorState INT, @CustomError INT
	DECLARE @Message VARCHAR(200)
	DECLARE @InicieTransaccion BIT
    DECLARE @idMovimiento INT;
    SET @CustomError = 2001

    DECLARE @TotalRecipientesRecibiendo INT;
    DECLARE @TotalRecpientes INT;
    
    -- Retrieve the producer ID based on the name
    DECLARE @producerID int
    SELECT @producerID = idproductor FROM productor WHERE nombre = @producerName
    
    -- Retrieve the contract data for the specified index
    DECLARE @idcontrato int
    SELECT @idcontrato = T.idcontrato
    FROM (
        SELECT ROW_NUMBER() OVER (ORDER BY c.idcontrato) AS RowNumber, p.idproductor, ac.idcontrato
        FROM productor AS p
        INNER JOIN actores_contrato AS ac ON ac.idproductor = p.idproductor
        INNER JOIN contrato AS c ON c.idcontrato = ac.idcontrato
        WHERE p.idproductor = @producerID
    ) AS T
    WHERE T.RowNumber = @index

    

	SET @InicieTransaccion = 0
	IF @@TRANCOUNT=0 BEGIN
		SET @InicieTransaccion = 1
		BEGIN TRANSACTION		
        SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
	END
	
	BEGIN TRY
    

        IF EXISTS (SELECT rd.idRecipiente -- revisar si los recipientes recibiendo y dando repiten valores entre si
            FROM @recipientesDando rd
            INNER JOIN @recipientesRecibiendo rr
            ON rd.idRecipiente = rr.idRecipiente)
        BEGIN
            RAISERROR('SE ESTA RECIBIENDO Y DANDO UNO O MAS DE UNO DE LOS RECIPIENTES',16, 1)
            RETURN;
        END;

        SELECT @TotalRecpientes = SUM(recipientes) FROM @info;
        SELECT @TotalRecipientesRecibiendo = COUNT(*) FROM @recipientesRecibiendo;

        IF NOT EXISTS (SELECT @TotalRecipientesRecibiendo INTERSECT SELECT @TotalRecpientes)-- revisar que las cantidades de recipientes recibidos es acorde a los recipientes recibidos
        BEGIN
            RAISERROR('EL TOTAL DE RECIPIENTES INDICADOS Y RECIBIDOS ES DIFERENTE', 16, 1, @TotalRecpientes, @TotalRecipientesRecibiendo);
            RETURN; -- or you can use THROW to terminate the stored procedure
        END

        IF EXISTS ( -- revisar que los recipientes son aptos para los tipos de desechos dados
        SELECT im.tipoDesecho FROM @info im
        INNER JOIN recipiente_tipodesecho rt ON rt.idtipodesecho = (
            SELECT idtipodesecho FROM tipodesecho WHERE nombre = im.tipoDesecho)
        LEFT JOIN @recipientesRecibiendo ir ON ir.idRecipiente = rt.idrecipiente
        GROUP BY im.tipoDesecho, im.recipientes
        HAVING COUNT(ir.idrecipiente) < im.recipientes)
        BEGIN
            RAISERROR('RECIPIENTES INSUFICIENTES PARA ALGUNO DE LOS TIPOS DE DESECHO', 16, 1);
            RETURN; 
        END

        -- se registra el movimiento de recibimiento de los desechos
        INSERT INTO EsencialVerde.dbo.movimientos_recipiente
        (idresponsable, cantidad, fecha, idtipo_movimiento,idpunto_recoleccion,idproductor,idrecolector,idcamion,idadress,idcontrato)
        VALUES(@idresponsable,(SELECT SUM(recipientes) FROM @info),GETDATE(),1,(SELECT TOP 1 local.iddireccion FROM local WHERE local.idproductor = @producerID), @producerID, @idrecolector, @idcamion, (SELECT TOP 1 local.iddireccion FROM local WHERE local.idproductor = @producerID), @idcontrato);

        SET @idMovimiento = @@IDENTITY;

        INSERT INTO movimientos_recipiente_recipiente (idmovimientos_recipiente, idrecipiente)
        SELECT @idMovimiento, r.idrecipiente
        FROM @recipientesRecibiendo r

        -- se registra el movimiento de dar los recipientes
        INSERT INTO EsencialVerde.dbo.movimientos_recipiente
        (idresponsable, cantidad, fecha, idtipo_movimiento,idproductor,idrecolector,idcamion,idadress,idcontrato)
        VALUES(@idresponsable,(SELECT COUNT(*) FROM @recipientesDando),GETDATE(),2, @producerID, @idrecolector, @idcamion, (SELECT TOP 1 local.iddireccion FROM local WHERE local.idproductor = @producerID), @idcontrato);

        SET @idMovimiento = @@IDENTITY;

        INSERT INTO movimientos_recipiente_recipiente (idmovimientos_recipiente, idrecipiente)
        SELECT @idMovimiento, r.idrecipiente
        FROM @recipientesDando r

        -- se insertan los desechos del movimiento
        INSERT INTO desechos(idtipodesecho,idcontrato,peso,enabled)
        SELECT (SELECT TOP 1 idtipodesecho FROM tipodesecho WHERE tipodesecho.nombre = i.tipoDesecho), @idcontrato, i.peso, 1
        FROM @info i
        WAITFOR DELAY '00:00:6';


		IF @InicieTransaccion=1 BEGIN
            SELECT 1;
			COMMIT
		END
	END TRY
	BEGIN CATCH
		SET @ErrorNumber = ERROR_NUMBER()
		SET @ErrorSeverity = ERROR_SEVERITY()
		SET @ErrorState = ERROR_STATE()
		SET @Message = ERROR_MESSAGE()
		IF @InicieTransaccion=1 BEGIN
			ROLLBACK
		END
		RAISERROR('%s - Error Number: %i', @ErrorSeverity, @ErrorState, @Message, @CustomError)
	END CATCH	
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[ProcesarDesechosFinal]
    @idpuntorecoleccion int,
    @desechosaprocesar desechosaprocesar READONLY
AS
BEGIN
    SET NOCOUNT ON -- no retorne metadatos
    DECLARE @ErrorNumber INT, @ErrorSeverity INT, @ErrorState INT, @CustomError INT
    DECLARE @Message VARCHAR(200)
    DECLARE @InicieTransaccion BIT
    DECLARE @desechos desechostype;
    SET @CustomError = 2001


    SET @InicieTransaccion = 0
    IF @@TRANCOUNT=0 BEGIN
        SET @InicieTransaccion = 1
        SET TRANSACTION ISOLATION LEVEL REPEATABLE READ 
        BEGIN TRANSACTION     
    END
    
    BEGIN TRY
        -- select para obtener todos los desechos a procesar 
        INSERT INTO @desechos(iddesecho,idtipodesecho,peso)
        SELECT d.iddesecho,d.idtipodesecho,d.peso FROM desechos AS d
        INNER JOIN contrato AS c ON c.idcontrato = d.idcontrato
        INNER JOIN puntorecoleccion AS pr ON pr.idpuntorecoleccion = c.idpuntorecoleccion
        INNER JOIN tipodesecho AS td ON td.idtipodesecho = d.idtipodesecho
        WHERE pr.idpuntorecoleccion = @idpuntorecoleccion AND td.nombre IN (SELECT nombre FROM @desechosaprocesar) AND d.enabled = 1;
        

        -- se deshabilitan los desechos que hayan sido procesados 
        UPDATE desechos SET enabled = 0
        WHERE iddesecho IN (
            SELECT iddesecho
            FROM @desechos
        );

        -- se insertan los materiales que se esten leyendo de desechos 
        INSERT INTO inventario_materiales (idmaterial, peso, enabled, iddesecho)
        SELECT m.idmaterial,d.peso,1,d.iddesecho
        FROM @desechos AS d
        INNER JOIN materiales AS m ON m.idtipodesecho = d.idtipodesecho;

        IF EXISTS (SELECT * FROM desechos AS d INNER JOIN tipodesecho AS td ON td.idtipodesecho = d.idtipodesecho WHERE td.nombre IN (SELECT nombre FROM @desechosaprocesar) AND td.reciclable = 0)
        BEGIN
            RAISERROR('UNO DE LOS DESECHOS DADOS NO ES APTO PARA PROCESAR', 16, 1);
            RETURN; 
        END

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

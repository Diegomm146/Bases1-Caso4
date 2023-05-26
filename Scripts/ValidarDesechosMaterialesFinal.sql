SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[ValidarDesechosMaterialesFinal]
    @desechoacomparar VARCHAR(50)
AS
BEGIN
    DECLARE @pesoMateriales DECIMAL(8,2) ;
    DECLARE @pesoDesechos DECIMAL(8,2) ;

    SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    BEGIN TRANSACTION     

    SELECT @pesoDesechos = ISNULL(SUM(d.peso), 0) 
    FROM desechos d WITH (HOLDLOCK)
    INNER JOIN tipodesecho td ON td.idtipodesecho = d.idtipodesecho
    WHERE td.nombre = @desechoacomparar AND d.enabled = 0;

    WAITFOR DELAY '00:00:6';

    SELECT @pesoMateriales = ISNULL(SUM(im.peso), 0)
    FROM inventario_materiales im WITH (HOLDLOCK)
    INNER JOIN desechos d ON d.iddesecho = im.iddesecho
    INNER JOIN tipodesecho td ON td.idtipodesecho = d.idtipodesecho
    WHERE td.nombre = @desechoacomparar;

    COMMIT TRANSACTION

    DECLARE @diferencia DECIMAL(8,2);

    SET @diferencia = @pesoMateriales - @pesoDesechos;

    SELECT @diferencia AS 'materiales - desechos', @pesoMateriales AS pm, @pesoDesechos AS pd;
END
GO

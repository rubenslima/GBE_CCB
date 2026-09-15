SET
DATEFORMAT DMY;


WITH
    COMUNICADO_DEFERIDO AS (
        SELECT
            CF.Matricula,
            EE.NOME_ENTID AS Participante,
            EE.CPF_CGC AS CPF,
            CAST(CF.DataObito AS DATE) AS Obito,
            MAX(
                CASE
                    WHEN RH.SituacaoId = 1 THEN CAST(RH.DataSituacao AS DATE)
                END
            ) AS Incluido,
            MAX(
                CASE
                    WHEN RH.SituacaoId = 2 THEN CAST(RH.DataSituacao AS DATE)
                END
            ) AS Deferido
        FROM
            Requerimento.ComunicadoFalecimento CF
        WITH
            (NOLOCK)
            LEFT JOIN dbo.CS_FUNCIONARIO FUN ON FUN.NUM_MATRICULA = CF.Matricula
            LEFT JOIN dbo.EE_ENTIDADE EE
        WITH
            (NOLOCK) ON EE.COD_ENTID = FUN.COD_ENTID
            LEFT JOIN Requerimento.HistoricoSituacao RH ON RH.RequerimentoId = CF.RequerimentoId
        WHERE
            1 = 1
            AND RH.SituacaoId IN (1, 2)
        GROUP BY
            CF.Matricula,
            EE.NOME_ENTID,
            EE.CPF_CGC,
            CF.DataObito
    )
SELECT
    Matricula,
    Participante,
    CPF,
    FORMAT(Obito, 'dd/MM/yyyy') AS DATAOBITO,
    FORMAT(Incluido, 'dd/MM/yyyy') AS DATAINCLUSAO,
    FORMAT(Deferido, 'dd/MM/yyyy') AS DATADEFERIMENTO,
    DATEDIFF(dd, Obito, Incluido) AS dias_entre_obito_comunicado,
    DATEDIFF(dd, Incluido, Deferido) AS dias_entre_comunicado_deferimento
FROM
    COMUNICADO_DEFERIDO
WHERE
    Deferido IS NOT NULL
ORDER BY
    Participante;

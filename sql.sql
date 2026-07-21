IF OBJECT_ID('tempdb..#COMUNICADO_DEFERIDO') IS NOT NULL
DROP TABLE #COMUNICADO_DEFERIDO;


SET
NOCOUNT ON;


SET
DATEFORMAT DMY;


SELECT
    CF.Matricula,
    EE.NOME_ENTID,
    EE.CPF_CGC,
    CONVERT(VARCHAR(12), CF.DataObito, 103) AS DATAOBITO,
    CONVERT(VARCHAR(12), RH.DataSituacao, 103) AS DATAINCLUSAO,
    (
        CASE SituacaoRequerimento
            WHEN 'DEFERIDO' THEN CONVERT(VARCHAR(12), RH.DataSituacao, 103)
            ELSE CONVERT(VARCHAR(12), '', 103)
        END
    ) AS DATADEFERIMENTO,
    (
        CASE SituacaoRequerimento
            WHEN 'DEFERIDO' THEN RH.MatriculaAtendimento
            ELSE ''
        END
    ) AS ATENDENTEDEFERIMENTO,
    (
        CASE SituacaoRequerimento
            WHEN 'DEFERIDO' THEN EE2.NOME_ENTID
            ELSE ''
        END
    ) AS NOMEATENDENTEDEFERIMENTO,
    RH1.MatriculaAtendimento AS ATENDENTE,
    EE3.NOME_ENTID AS NOMEATENDENTE,
    SITUACAOREQUERIMENTO AS SituacaoPedido,
    CF.RequerimentoId
INTO
    #COMUNICADO_DEFERIDO
FROM
    Requerimento.ComunicadoFalecimento CF (NOLOCK)
    LEFT JOIN dbo.CS_FUNCIONARIO FUN ON FUN.NUM_MATRICULA = CF.Matricula
    LEFT JOIN dbo.EE_ENTIDADE EE (NOLOCK) ON EE.COD_ENTID = FUN.COD_ENTID
    LEFT JOIN Requerimento.HistoricoSituacao RH ON RH.RequerimentoId = CF.RequerimentoId
    LEFT JOIN Requerimento.HistoricoSituacao RH1 ON RH1.RequerimentoId = CF.RequerimentoId
    AND RH1.SituacaoId = 1
    LEFT JOIN dbo.CS_FUNCIONARIO FUN3 ON FUN3.NUM_MATRICULA = RH1.MatriculaAtendimento
    LEFT JOIN dbo.EE_ENTIDADE EE3 ON EE3.COD_ENTID = FUN3.COD_ENTID
    LEFT JOIN dbo.CS_FUNCIONARIO FUN2 ON FUN2.NUM_MATRICULA = RH.MatriculaAtendimento
    LEFT JOIN dbo.EE_ENTIDADE EE2 ON EE2.COD_ENTID = FUN2.COD_ENTID
    LEFT JOIN Requerimento.Situacao ON Situacao.SituacaoId = RH.SituacaoId
WHERE
    RH.DataSituacao = (
        SELECT
            MAX(HIS.DataSituacao)
        FROM
            Requerimento.HistoricoSituacao HIS
        WHERE
            HIS.RequerimentoId = RH.RequerimentoId
    )
    AND RH.SituacaoId = 2
ORDER BY
    RH.DataSituacao
SELECT
    *
FROM
    #COMUNICADO_DEFERIDO

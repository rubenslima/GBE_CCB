SELECT
    cf.Matricula,
    ee.NOME_ENTID AS NOME_ENTIDADE,
    ee.CPF_CGC,
    ISNULL(BD.Plano, 'Não') PlanoBD,
    ISNULL(BD.SitPlano, 'Não') TipoBeneficiarioBD,
    ISNULL(CV.Plano, 'Não') PlanoPostalprev,
    ISNULL(CV.SitPlano, 'Não') TipoBeneficiario,
    CONVERT(CHAR(10), cf.DataObito, 103) AS DT_OBITO,
    (
        SELECT
            CONVERT(CHAR(10), hs1.DataSituacao, 103)
        FROM
            Requerimento.HistoricoSituacao hs1
        WHERE
            hs1.RequerimentoId = hs.RequerimentoId
            AND hs1.SituacaoId = 1
    ) AS Data_inclusao,
    sr.SituacaoRequerimento AS SITUACAO_REQ,
    CONVERT(CHAR(10), hs.DataSituacao, 103) AS Data_situacao
FROM
    Requerimento.ComunicadoFalecimento cf
    INNER JOIN Requerimento.HistoricoSituacao hs ON hs.RequerimentoId = cf.RequerimentoId
    INNER JOIN Requerimento.Situacao sr ON sr.SituacaoId = hs.SituacaoId
    INNER JOIN dbo.CS_FUNCIONARIO fu ON fu.NUM_MATRICULA = cf.Matricula
    INNER JOIN dbo.EE_ENTIDADE ee ON ee.COD_ENTID = fu.COD_ENTID
    OUTER APPLY (
        SELECT
            'Sim' Plano,
            SP.DS_SIT_PLANO SitPlano
        FROM
            dbo.CS_PLANOS_VINC PV
            LEFT JOIN dbo.TB_SIT_PLANO SP ON SP.CD_SIT_PLANO = PV.CD_SIT_PLANO
        WHERE
            PV.NUM_INSCRICAO = FU.NUM_INSCRICAO
            AND PV.CD_PLANO = '0001'
    ) BD --BENEFICIO DEFINIDO
    OUTER APPLY (
        SELECT
            'Sim' Plano,
            SP.DS_SIT_PLANO SitPlano
        FROM
            dbo.CS_PLANOS_VINC PV
            LEFT JOIN dbo.TB_SIT_PLANO SP ON SP.CD_SIT_PLANO = PV.CD_SIT_PLANO
        WHERE
            PV.NUM_INSCRICAO = FU.NUM_INSCRICAO
            AND PV.CD_PLANO = '0002'
    ) CV --POSTALPREV
WHERE
    hs.SituacaoId = 2
    AND cf.Matricula NOT IN (
        SELECT
            pb.CD_MATRICULA
        FROM
            dbo.FI_GBE_PROCESSO_BENEFICIO pb
        WHERE
            pb.CD_ESPECIE IN (3, 4, 10, 11)
        UNION
        SELECT
            fn.NUM_MATRICULA
        FROM
            dbo.GB_PROCESSOS_BENEFICIO pb
            INNER JOIN dbo.CS_FUNCIONARIO fn ON fn.CD_FUNDACAO = pb.CD_FUNDACAO
            AND fn.NUM_INSCRICAO = pb.NUM_INSCRICAO
        WHERE
            pb.CD_ESPECIE IN ('21', '63')
    )
    AND DataObito IS NOT NULL
ORDER BY
    DT_OBITO,
    cf.Matricula;

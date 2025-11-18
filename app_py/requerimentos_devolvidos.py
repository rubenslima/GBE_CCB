import os
import re
import time
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv
from datetime import datetime


def ler_data(mensagem: str) -> str:
    """
    Lê uma data via input e converte para o formato mm-dd-aaaa,
    aceitando dd/mm/aaaa ou mm-dd-aaaa.
    """
    while True:
        entrada = input(mensagem).strip()
        formatos = ["%d/%m/%Y", "%m-%d-%Y"]

        for fmt in formatos:
            try:
                dt = datetime.strptime(entrada, fmt)
                # Converte sempre para mm-dd-aaaa
                return dt.strftime("%m-%d-%Y")
            except ValueError:
                pass

        print("Formato inválido. Use dd/mm/aaaa ou mm-dd-aaaa.")

data_inicio = ler_data("Informe a data inicial (dd/mm/aaaa ou mm-dd-aaaa): ")
data_fim = ler_data("Informe a data final (dd/mm/aaaa ou mm-dd-aaaa): ")


# ------------------------------
# Utilidades
# ------------------------------
def limpar_console() -> None:
    os.system("cls" if os.name == "nt" else "clear")


def carregar_cfg():
    load_dotenv()
    cfg = {
        "SERVER": (os.getenv("SERVER") or "").strip(),
        "USER": (os.getenv("USER") or "").strip(),
        "PASSWORD": (os.getenv("PASSWORD") or "").strip(),
        "DATABASE": (os.getenv("DATABASE") or "").strip(),
        "ODBC_DRIVER": (
            os.getenv("ODBC_DRIVER") or "ODBC Driver 17 for SQL Server"
        ).strip(),
        "ODBC_EXTRA": (os.getenv("ODBC_EXTRA") or "").strip(),  
    }
    faltando = [k for k in ("SERVER", "USER", "PASSWORD", "DATABASE") if not cfg[k]]
    if faltando:
        raise RuntimeError(f"Variáveis ausentes no .env: {', '.join(faltando)}")
    return cfg


def build_connection_url(cfg) -> str:
    """
    Usa DSN-less com odbc_connect e quote_plus para evitar problemas com caracteres especiais.
    """
    params = (
        f"DRIVER={{{cfg['ODBC_DRIVER']}}};"
        f"SERVER={cfg['SERVER']};"
        f"DATABASE={cfg['DATABASE']};"
        f"UID={cfg['USER']};"
        f"PWD={cfg['PASSWORD']}"
    )
    if cfg["ODBC_EXTRA"]:
        extra = cfg["ODBC_EXTRA"]
        if not extra.endswith(";"):
            extra += ";"
        params += ";" + extra
    return f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(params)}"


def get_engine(cfg) -> Engine:
    url = build_connection_url(cfg)
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
        fast_executemany=False,
    )


def garantir_pasta(caminho: str) -> None:
    os.makedirs(caminho, exist_ok=True)


def sanitize_filename(name: str) -> str:
    # Remove caracteres inválidos em nomes de arquivo (Windows-safe)
    return re.sub(r'[<>:"/\\|?*]+', "-", name).strip() or "export"


def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [re.sub(r"[\\/*?:\[\]]", "", str(col)).strip() for col in df.columns]
    return df


def autosize_columns(writer, sheet_name: str, df: pd.DataFrame) -> None:
    """
    Ajuste simples de largura por coluna usando xlsxwriter (sem reabrir o arquivo).
    """
    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns, start=0):
        if df.empty:
            max_len = len(str(col)) + 2
        else:
            serie = df[col].astype(str)
            max_len = max(serie.map(len).max(), len(str(col))) + 2
        ws.set_column(idx, idx, min(max_len, 60))


def main():
    limpar_console()
    try:
        cfg = carregar_cfg()
    except Exception as e:
        print(f"Erro nas variáveis de ambiente: {e}")
        return

    # --------------------------
    # Consultas SQL
    # --------------------------
    query_principal = f"""
    set nocount on;

    IF OBJECT_ID('tempdb..#Identificador') IS NOT NULL
        DROP TABLE #Identificador;

    SELECT rtrim(fun.NUM_MATRICULA)+'@'+rtrim(req.NU_BENEFICIO_INSS) identificador
    into #Identificador
    FROM dbo.WEB_GBE_REQUERIMENTO AS req
    INNER JOIN dbo.CS_FUNCIONARIO AS fun
        ON req.CD_FUNDACAO  = fun.CD_FUNDACAO
        AND req.CD_INSCRICAO = fun.NUM_INSCRICAO
    INNER JOIN dbo.CS_PLANOS_VINC AS plv
        ON fun.CD_FUNDACAO   = plv.CD_FUNDACAO
        AND fun.NUM_INSCRICAO = plv.NUM_INSCRICAO
    INNER JOIN web.HistoricoRequerimento AS his
        ON req.SQ_REQUERIMENTO = his.SequencialRequerimento
    INNER JOIN dbo.TB_PLANOS AS pln
        ON req.CD_PLANO    = pln.CD_PLANO
        AND req.CD_FUNDACAO = pln.CD_FUNDACAO
    INNER JOIN dbo.WEB_GPA_SIT_INSCRICOES AS sit
        ON req.NS_SIT_REQUERIMENTO = sit.NS_SIT_INSCRICAO
    INNER JOIN web.TipoRequerimentoBeneficio AS tip
        ON req.TP_PROCESSO = tip.Id
    WHERE 1=1
        AND req.CD_PLANO = plv.CD_PLANO
        AND his.Data = (        SELECT MAX(sub.Data)
        FROM web.HistoricoRequerimento AS sub
        WHERE sub.SequencialRequerimento = req.SQ_REQUERIMENTO     )
            AND req.DT_REQUERIMENTO >= '{data_inicio}' 
            AND req.DT_REQUERIMENTO <= '{data_fim}'
        AND sit.NS_SIT_INSCRICAO in(99,6)
        AND tip.Id = '1'
        and len(req.NU_BENEFICIO_INSS)>1
    group by fun.NUM_MATRICULA, req.NU_BENEFICIO_INSS 
    order by fun.NUM_MATRICULA, req.NU_BENEFICIO_INSS 
        



    SELECT 
        --req.SQ_REQUERIMENTO               AS CodigoRequerimento,
        fun.NUM_MATRICULA                 AS Matricula,
        ent.NOME_ENTID                    AS NomeParticipante,
        FORMAT(req.DT_REQUERIMENTO, 'dd/MM/yyyy HH:mm') AS DataRequerimento,
        FORMAT(req.DT_DEFERIMENTO, 'dd/MM/yyyy') AS DataDeferimento,
        req.NU_BENEFICIO_INSS             AS NumeroBeneficioINSS,
        esp.Especie                       AS Especie,
        tip.Descricao                     AS Tipo,
        sit.DS_SIT_INSCRICAO              AS [Status],
        funent.NOME_ENTID                 AS Responsavel,
            -- Flag indicando se foi autoatendimento (r.CD_MATRICULA_ATD_INCLUSAO = f.NUM_MATRICULA)
        case CAST(
            IIF(
                EXISTS (
                    SELECT 1
                    FROM WEB_GBE_REQUERIMENTO r
                    INNER JOIN CS_FUNCIONARIO f
                            ON r.CD_INSCRICAO = f.NUM_INSCRICAO
                    WHERE r.CD_MATRICULA_ATD_INCLUSAO = f.NUM_MATRICULA
                    AND r.SQ_REQUERIMENTO = req.SQ_REQUERIMENTO
                ),
            1, 0
            ) AS bit 
        ) WHEN 1
        THEN 'SIM' 
        ELSE '' end AS AutoAtendimento,

        CASE req.IN_DECISAO_JUDICIAL 
        WHEN '1' 
        THEN 'SIM' 
        ELSE '' END  AS DecisaoJudicial,
        pln.DS_PLANO                      AS Plano,
        format (dad.DT_OBITO, 'dd/MM/yyyy')         AS DataObito,
        format (req.DT_INI_BENEFICIO, 'dd/MM/yyyy') AS DataInicioBeneficio,
    --    format (req.DT_INCLUSAO, 'dd/MM/yyyy')     AS DataInclusao,
    --    tip.Id                            AS CodigoTipoRequerimento,
    --    sit.NS_SIT_INSCRICAO              AS NumeroSituacao,
    --    pln.CD_PLANO                      AS CodigoPlano,
    --    req.CD_ESPECIE                    AS CodigoEspecie,
        his.MatriculaAtendimento          AS MatriculaResponsavel,
    --    req.VL_SALARIO_CONTRIB            AS ValorSalarioContribuicao,
    --    req.TP_BAD_SITUACAO_INSS          AS TipoSituacaoINSS,
    --    fun.CD_EMPRESA                    AS CodigoEmpresa,
    --    req.DADOS_RPA                     AS IndicadorDadosRPA,
        CASE req.ANEXOU_LAUDO    
            WHEN '1' 
        THEN 'SIM' 
        ELSE '' END AS AnexouLaudo,
        CASE     req.ISENTO_IR
            WHEN '1' 
        THEN 'SIM' 
        ELSE '' END AS IsencaoIR
        

    FROM dbo.WEB_GBE_REQUERIMENTO AS req

    -- Espécie do benefício
    LEFT OUTER JOIN dbo.vwEspecieBeneficio AS esp
        ON req.CD_ESPECIE = esp.Codigo

    -- Participante (funcionário)
    INNER JOIN dbo.CS_FUNCIONARIO AS fun
        ON req.CD_FUNDACAO  = fun.CD_FUNDACAO
        AND req.CD_INSCRICAO = fun.NUM_INSCRICAO

    -- Entidade (dados pessoais)
    INNER JOIN dbo.EE_ENTIDADE AS ent
        ON fun.COD_ENTID = ent.COD_ENTID
    INNER JOIN dbo.CS_DADOS_PESSOAIS AS dad
        ON ent.COD_ENTID = dad.COD_ENTID

    -- Plano vinculado
    INNER JOIN dbo.CS_PLANOS_VINC AS plv
        ON fun.CD_FUNDACAO   = plv.CD_FUNDACAO
        AND fun.NUM_INSCRICAO = plv.NUM_INSCRICAO

    -- Histórico do requerimento
    INNER JOIN web.HistoricoRequerimento AS his
        ON req.SQ_REQUERIMENTO = his.SequencialRequerimento

    -- Funcionário e entidade responsável pelo atendimento
    LEFT OUTER JOIN dbo.CS_FUNCIONARIO AS hisfun
        ON his.MatriculaAtendimento = hisfun.NUM_MATRICULA
    LEFT OUTER JOIN dbo.EE_ENTIDADE AS funent
        ON hisfun.COD_ENTID = funent.COD_ENTID

    -- Plano (informações cadastrais)
    INNER JOIN dbo.TB_PLANOS AS pln
        ON req.CD_PLANO    = pln.CD_PLANO
        AND req.CD_FUNDACAO = pln.CD_FUNDACAO

    -- Situação e tipo de requerimento
    INNER JOIN dbo.WEB_GPA_SIT_INSCRICOES AS sit
        ON req.NS_SIT_REQUERIMENTO = sit.NS_SIT_INSCRICAO
    INNER JOIN web.TipoRequerimentoBeneficio AS tip
        ON req.TP_PROCESSO = tip.Id

    WHERE
        req.CD_PLANO = plv.CD_PLANO
        AND his.Data = (
            SELECT MAX(sub.Data)
            FROM web.HistoricoRequerimento AS sub
            WHERE sub.SequencialRequerimento = req.SQ_REQUERIMENTO
        )
        AND tip.Id = '1'-- CONCESSAO
        AND rtrim(fun.NUM_MATRICULA)+'@'+rtrim(req.NU_BENEFICIO_INSS) in(select identificador from #Identificador)

    ORDER BY  ent.NOME_ENTID 
    , req.NU_BENEFICIO_INSS 
    ,  req.DT_REQUERIMENTO 

	
    """.strip()

 
    query_sem_numero_beneficio = f"""
    set nocount on;

    IF OBJECT_ID('tempdb..#Identificador_matricula') IS NOT NULL
        DROP TABLE #Identificador_matricula;

    SELECT rtrim(fun.NUM_MATRICULA) matricula
    into #Identificador_matricula
    FROM dbo.WEB_GBE_REQUERIMENTO AS req
    INNER JOIN dbo.CS_FUNCIONARIO AS fun
        ON req.CD_FUNDACAO  = fun.CD_FUNDACAO
        AND req.CD_INSCRICAO = fun.NUM_INSCRICAO
    INNER JOIN dbo.CS_PLANOS_VINC AS plv
        ON fun.CD_FUNDACAO   = plv.CD_FUNDACAO
        AND fun.NUM_INSCRICAO = plv.NUM_INSCRICAO
    INNER JOIN web.HistoricoRequerimento AS his
        ON req.SQ_REQUERIMENTO = his.SequencialRequerimento
    INNER JOIN dbo.TB_PLANOS AS pln
        ON req.CD_PLANO    = pln.CD_PLANO
        AND req.CD_FUNDACAO = pln.CD_FUNDACAO
    INNER JOIN dbo.WEB_GPA_SIT_INSCRICOES AS sit
        ON req.NS_SIT_REQUERIMENTO = sit.NS_SIT_INSCRICAO
    INNER JOIN web.TipoRequerimentoBeneficio AS tip
        ON req.TP_PROCESSO = tip.Id
    WHERE 1=1
        AND req.CD_PLANO = plv.CD_PLANO
        AND his.Data = (        SELECT MAX(sub.Data)
        FROM web.HistoricoRequerimento AS sub
        WHERE sub.SequencialRequerimento = req.SQ_REQUERIMENTO     )
            AND req.DT_REQUERIMENTO >= '{data_inicio}' 
            AND req.DT_REQUERIMENTO <= '{data_fim}'
        AND sit.NS_SIT_INSCRICAO in(99,6)
        AND tip.Id = '1'
        and len(isnull(req.NU_BENEFICIO_INSS,'0'))<1
    group by fun.NUM_MATRICULA
    order by fun.NUM_MATRICULA

    SELECT 
        --req.SQ_REQUERIMENTO               AS CodigoRequerimento,
        fun.NUM_MATRICULA                 AS Matricula,
        ent.NOME_ENTID                    AS NomeParticipante,
        FORMAT(req.DT_REQUERIMENTO, 'dd/MM/yyyy HH:mm') AS DataRequerimento,
        FORMAT(req.DT_DEFERIMENTO, 'dd/MM/yyyy') AS DataDeferimento,
        req.NU_BENEFICIO_INSS             AS NumeroBeneficioINSS,
        esp.Especie                       AS Especie,
        tip.Descricao                     AS Tipo,
        sit.DS_SIT_INSCRICAO              AS [Status],
        funent.NOME_ENTID                 AS Responsavel,
            -- Flag indicando se foi autoatendimento (r.CD_MATRICULA_ATD_INCLUSAO = f.NUM_MATRICULA)
        case CAST(
            IIF(
                EXISTS (
                    SELECT 1
                    FROM WEB_GBE_REQUERIMENTO r
                    INNER JOIN CS_FUNCIONARIO f
                            ON r.CD_INSCRICAO = f.NUM_INSCRICAO
                    WHERE r.CD_MATRICULA_ATD_INCLUSAO = f.NUM_MATRICULA
                    AND r.SQ_REQUERIMENTO = req.SQ_REQUERIMENTO
                ),
            1, 0
            ) AS bit 
        ) WHEN 1
        THEN 'SIM' 
        ELSE '' end AS AutoAtendimento,

        CASE req.IN_DECISAO_JUDICIAL 
        WHEN '1' 
        THEN 'SIM' 
        ELSE '' END  AS DecisaoJudicial,
        pln.DS_PLANO                      AS Plano,
        format (dad.DT_OBITO, 'dd/MM/yyyy')         AS DataObito,
        format (req.DT_INI_BENEFICIO, 'dd/MM/yyyy') AS DataInicioBeneficio,
        format (req.DT_INCLUSAO, 'dd/MM/yyyy')     AS DataInclusao,
    --    tip.Id                            AS CodigoTipoRequerimento,
    --    sit.NS_SIT_INSCRICAO              AS NumeroSituacao,
    --    pln.CD_PLANO                      AS CodigoPlano,
    --    req.CD_ESPECIE                    AS CodigoEspecie,
    --    his.MatriculaAtendimento          AS MatriculaResponsavel,
    --    req.VL_SALARIO_CONTRIB            AS ValorSalarioContribuicao,
    --    req.TP_BAD_SITUACAO_INSS          AS TipoSituacaoINSS,
    --    fun.CD_EMPRESA                    AS CodigoEmpresa,
    --    req.DADOS_RPA                     AS IndicadorDadosRPA,
        CASE req.ANEXOU_LAUDO    
            WHEN '1' 
        THEN 'SIM' 
        ELSE '' END AS AnexouLaudo,
        CASE     req.ISENTO_IR
            WHEN '1' 
        THEN 'SIM' 
        ELSE '' END AS IsencaoIR
        

    FROM dbo.WEB_GBE_REQUERIMENTO AS req

    -- Espécie do benefício
    LEFT OUTER JOIN dbo.vwEspecieBeneficio AS esp
        ON req.CD_ESPECIE = esp.Codigo

    -- Participante (funcionário)
    INNER JOIN dbo.CS_FUNCIONARIO AS fun
        ON req.CD_FUNDACAO  = fun.CD_FUNDACAO
        AND req.CD_INSCRICAO = fun.NUM_INSCRICAO

    -- Entidade (dados pessoais)
    INNER JOIN dbo.EE_ENTIDADE AS ent
        ON fun.COD_ENTID = ent.COD_ENTID
    INNER JOIN dbo.CS_DADOS_PESSOAIS AS dad
        ON ent.COD_ENTID = dad.COD_ENTID

    -- Plano vinculado
    INNER JOIN dbo.CS_PLANOS_VINC AS plv
        ON fun.CD_FUNDACAO   = plv.CD_FUNDACAO
        AND fun.NUM_INSCRICAO = plv.NUM_INSCRICAO

    -- Histórico do requerimento
    INNER JOIN web.HistoricoRequerimento AS his
        ON req.SQ_REQUERIMENTO = his.SequencialRequerimento

    -- Funcionário e entidade responsável pelo atendimento
    LEFT OUTER JOIN dbo.CS_FUNCIONARIO AS hisfun
        ON his.MatriculaAtendimento = hisfun.NUM_MATRICULA
    LEFT OUTER JOIN dbo.EE_ENTIDADE AS funent
        ON hisfun.COD_ENTID = funent.COD_ENTID

    -- Plano (informações cadastrais)
    INNER JOIN dbo.TB_PLANOS AS pln
        ON req.CD_PLANO    = pln.CD_PLANO
        AND req.CD_FUNDACAO = pln.CD_FUNDACAO

    -- Situação e tipo de requerimento
    INNER JOIN dbo.WEB_GPA_SIT_INSCRICOES AS sit
        ON req.NS_SIT_REQUERIMENTO = sit.NS_SIT_INSCRICAO
    INNER JOIN web.TipoRequerimentoBeneficio AS tip
        ON req.TP_PROCESSO = tip.Id

    WHERE
        req.CD_PLANO = plv.CD_PLANO
        AND his.Data = (
            SELECT MAX(sub.Data)
            FROM web.HistoricoRequerimento AS sub
            WHERE sub.SequencialRequerimento = req.SQ_REQUERIMENTO
        )
        AND tip.Id = '1'-- CONCESSAO
        AND rtrim(fun.NUM_MATRICULA)  in (select matricula from #Identificador_matricula)
        AND req.DT_REQUERIMENTO >= '{data_inicio}' 
        AND req.DT_REQUERIMENTO <= '{data_fim}'

    ORDER BY  ent.NOME_ENTID 
    , req.NU_BENEFICIO_INSS 
    ,  req.DT_REQUERIMENTO 
    """.strip()


    print("Conectando ao DATABASE...")
    try:
        engine = get_engine(cfg)
        # Teste rápido
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Conexão bem-sucedida.")
    except Exception as e:
        print(f"Erro ao conectar ao DATABASE de dados: {e}")
        return

    print("Executando consultas...")
    try:
        # IMPORTANTE: manter a mesma conexão para temp tables, se houver
        with engine.connect() as conn:
            # Consulta principal
            df = pd.read_sql(text(query_principal), conn)

            try:
                df_sem_numero_beneficio = pd.read_sql(text(query_sem_numero_beneficio), conn)
            except Exception as e:
                print(f"Erro ao executar a query_sem_numero_beneficio: {e}")
                df_sem_numero_beneficio = pd.DataFrame()
 

    except Exception as e:
        print(f"Erro ao executar as queries: {e}")
        return

    out_dir = "Arquivos"
    garantir_pasta(out_dir)

    # Tratar colunas
    df = sanitize_columns(df)

    df_sem_numero_beneficio = sanitize_columns(df_sem_numero_beneficio) if not df_sem_numero_beneficio.empty else df_sem_numero_beneficio

        # <<< NOVO: gerar DataFrame de estatística por Status >>>
    df_estat = pd.DataFrame()
    if not df.empty and "Status" in df.columns:
        df_estat = (
            df["Status"]
            .value_counts()
            .reset_index()
        )
        df_estat.columns = ["Status", "Total"]
    else:
        print("Não foi possível gerar a aba 'Estatistica' (coluna 'Status' ausente ou sem dados).")
    # <<< FIM NOVO >>>

    ts = time.strftime("%Y%m%d")
    nome_arquivo = os.path.join(out_dir, f"Requerimentos_devolvidos_{ts}.xlsx")

    print("Gerando Excel...")
    try:
        with pd.ExcelWriter(nome_arquivo, engine="xlsxwriter") as writer:
            sheet = "Dados"
            df.to_excel(writer, sheet_name=sheet, index=False)
            autosize_columns(writer, sheet, df)

            if df_sem_numero_beneficio is not None and not df_sem_numero_beneficio.empty:
                sheet_teste = "sem_numero_beneficio"
                df_sem_numero_beneficio.to_excel(writer, sheet_name=sheet_teste, index=False)
                autosize_columns(writer, sheet_teste, df_sem_numero_beneficio)
            else:
                print("Não há registros sem numero de beneficio (segunda consulta não retornou linhas).")
                        # <<< NOVO: Aba 'Estatistica' >>>
            if df_estat is not None and not df_estat.empty:
                sheet_est = "Estatistica"
                df_estat.to_excel(writer, sheet_name=sheet_est, index=False)
                autosize_columns(writer, sheet_est, df_estat)
            # <<< FIM NOVO >>>    

        print(f"Arquivo salvo com sucesso: {nome_arquivo}")
        if df.empty:
            print("Aviso: a consulta principal retornou 0 linhas (planilha criada vazia na aba 'Dados').")
        else:
            print(f"Aba 'Dados' - Linhas: {len(df)}  |  Colunas: {len(df.columns)}")

        if df_sem_numero_beneficio is not None and not df_sem_numero_beneficio.empty:
            print(f"Aba 'sem_numero_beneficio' - Linhas: {len(df_sem_numero_beneficio)}  |  Colunas: {len(df_sem_numero_beneficio.columns)}")

    except Exception as e:
        print(f"Erro ao salvar o arquivo Excel: {e}")


if __name__ == "__main__":
    main()

"""
Projeto         : Comunicado falecimento
Autor           : Rubens Lima
Solicitante     : Silvia
Criado em       : 2026-06-10
Última alteração: 2026-06-10
Versão          : 1.0.0.a
Descrição       : Geração de planilha para conferência dos comunicados de falecidos deferidos
Tipo            : ETL
Módulo          : Conferência CCB
Tags            : falecidos
ID              : GBE.CCB.20260610.001
"""

import os
import re
import time
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv


# ------------------------------
# Utilidades
# ------------------------------
def limpar_console() -> None:
    os.system("cls" if os.name == "nt" else "clear")


nome_arquivo = os.path.splitext(os.path.basename(__file__))[0]


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
        "ODBC_EXTRA": (
            os.getenv("ODBC_EXTRA") or ""
        ).strip(),  # ex.: Encrypt=yes;TrustServerCertificate=yes
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
        url, pool_pre_ping=True, pool_recycle=1800, pool_size=5, max_overflow=5
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

    query = """
    SET NOCOUNT ON
    SET DATEFORMAT DMY

    SELECT
    cf.Matricula,
    ee.NOME_ENTID AS NOME_ENTIDADE,
    ee.CPF_CGC,
    ISNULL(BD.Plano, 'Não') PlanoBD,
    ISNULL(BD.SitPlano, 'Não') TipoBeneficiarioBD,
    ISNULL(CV.Plano, 'Não') PlanoPostalprev,
    ISNULL(CV.SitPlano, 'Não') TipoBeneficiario,
    CONVERT(CHAR(10), cf.DataObito, 103) AS DT_OBITO,
    (  SELECT
            CONVERT(CHAR(10), hs1.DataSituacao, 103)
        FROM  Requerimento.HistoricoSituacao hs1
        WHERE  hs1.RequerimentoId = hs.RequerimentoId
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
    OUTER APPLY (SELECT 'Sim' Plano,
            SP.DS_SIT_PLANO SitPlano
        FROM dbo.CS_PLANOS_VINC PV
            LEFT JOIN dbo.TB_SIT_PLANO SP ON SP.CD_SIT_PLANO = PV.CD_SIT_PLANO
        WHERE PV.NUM_INSCRICAO = FU.NUM_INSCRICAO
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
        SELECT pb.CD_MATRICULA
        FROM dbo.FI_GBE_PROCESSO_BENEFICIO pb
        WHERE  pb.CD_ESPECIE IN (3, 4, 10, 11)
        UNION
        SELECT fn.NUM_MATRICULA
        FROM dbo.GB_PROCESSOS_BENEFICIO pb
        INNER JOIN dbo.CS_FUNCIONARIO fn ON fn.CD_FUNDACAO = pb.CD_FUNDACAO
            AND fn.NUM_INSCRICAO = pb.NUM_INSCRICAO
        WHERE pb.CD_ESPECIE IN ('21', '63')
    )
    AND DataObito IS NOT NULL
ORDER BY
    DT_OBITO,
    cf.Matricula;

   """.strip()

    print("Conectando ao DATABASE...")
    try:
        engine = get_engine(cfg)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Conexão bem-sucedida.")
    except Exception as e:
        print(f"Erro ao conectar ao DATABASE de dados: {e}")
        return

    print("Executando consulta...")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
    except Exception as e:
        print(f"Erro ao executar a query: {e}")
        return

    # Garantir pasta de saída
    out_dir = "Arquivos"
    garantir_pasta(out_dir)

    # Tratar colunas
    df = sanitize_columns(df)

    # Montar nome de arquivo com timestamp para evitar sobrescrita
    base = sanitize_filename(nome_arquivo)
    ts = time.strftime("%Y%m%d_%H%M%S")
    nome_arquivo_completo = os.path.join(out_dir, f"{base}_{ts}.xlsx")

    print("Gerando Excel...")
    try:
        with pd.ExcelWriter(nome_arquivo_completo, engine="xlsxwriter") as writer:
            sheet = "Dados"
            df.to_excel(writer, sheet_name=sheet, index=False)
            autosize_columns(writer, sheet, df)
        print(f"Arquivo salvo com sucesso: {nome_arquivo_completo}")
        if df.empty:
            print("Aviso: a consulta retornou 0 linhas (planilha criada vazia).")
        else:
            print(f"Linhas: {len(df)}  |  Colunas: {len(df.columns)}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo Excel: {e}")


if __name__ == "__main__":
    main()

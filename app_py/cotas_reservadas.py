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


def carregar_cfg():
    load_dotenv()
    cfg = {
        "SERVER": (os.getenv("SERVER") or "").strip(),
        "USER": (os.getenv("USER") or "").strip(),
        "PASSWORD": (os.getenv("PASSWORD") or "").strip(),
        "DATABASE": (os.getenv("DATABASE") or "").strip(),
        "ODBC_DRIVER": (os.getenv("ODBC_DRIVER") or "ODBC Driver 17 for SQL Server").strip(),
        "ODBC_EXTRA": (os.getenv("ODBC_EXTRA") or "").strip(),  # ex.: Encrypt=yes;TrustServerCertificate=yes
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
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800, pool_size=5, max_overflow=5)


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

    # Personalize conforme sua necessidade 
    solicitacao = "cotas_reservadas"

    # Sua consulta aqui (exemplo simples; substitua por sua SQL)
    query = """
        SET NOCOUNT ON

        SELECT DISTINCT
        RTRIM(pb.NR_PROCESSO)+'/'+RTRIM(PB.ANO_PROCESSO) PROCESSO,
        PB.CD_MATRICULA [MATRICULA] ,
        ee1.NOME_ENTID [PARTICIPANTE],
        CONVERT(CHAR(10), DP.DT_OBITO, 103) OBITO,
        CONVERT(CHAR(10), vp.DT_INIC_BENEFICIO, 103) [INICIO_BENEFICIO],
        ee.NOME_ENTID [RECEBEDOR],
        gp.DS_GRAU_PARENTESCO PARENTESCO,
        ST.DS_SIT_PROCESSO SITUACAO_PROCESSO,
        CONVERT(CHAR(10), DP.DT_NASCIMENTO, 103) [DT_NASCIMENTO],       
        DATEDIFF(YEAR, vp.DT_INIC_BENEFICIO, GETDATE()) [QTD ANOS DIB] ,
        TP.DS_TIPO_BLOQUEIO BLOQUEIO,
        BL.MOTIVO_BLOQUEIO
        FROM    dbo.FI_GBE_BENEFICIARIO_RECEBEDOR re
        INNER JOIN dbo.EE_ENTIDADE ee ON ee.COD_ENTID = re.CD_PESSOA_RECEB
        INNER JOIN dbo.FI_GBE_PROCESSO_BENEFICIO PB ON PB.SQ_PROCESSO = re.SQ_PROCESSO
        INNER JOIN dbo.FI_GBE_ESPECIE_BENEFICIO eb ON eb.CD_ESPECIE = PB.CD_ESPECIE
              
        LEFT JOIN POS_GBE_BENEFICIARIO GB ON gb.SQ_BENEFICIARIO = RE.SQ_BENEFICIARIO AND PB.CD_MATRICULA = GB.CD_INSCRICAO AND  PB.CD_PLANO = gb.CD_PLANO AND PB.CD_FUNDACAO = GB.CD_FUNDACAO AND EE.NOME_ENTID = GB.NO_BENEFICIARIO ---VER A FORMA CORRETA
        LEFT jOIN TB_GRAU_PARENTESCO GP on GP.CD_GRAU_PARENTESCO = gb.CD_GRAU_PARENTESCO

        INNER JOIN dbo.CS_FUNCIONARIO fu ON fu.CD_FUNDACAO = PB.CD_FUNDACAO
                                            AND fu.CD_EMPRESA = PB.CD_EMPRESA
                                            AND fu.NUM_MATRICULA = PB.CD_MATRICULA
        INNER JOIN dbo.EE_ENTIDADE ee1 ON ee1.COD_ENTID = fu.COD_ENTID
        INNER JOIN dbo.CS_DADOS_PESSOAIS DP ON DP.COD_ENTID = ee1.COD_ENTID
        INNER JOIN dbo.FI_GBE_HIST_VERSAO_PROCESSO vp ON vp.SQ_PROCESSO = PB.SQ_PROCESSO
                                                         AND vp.SQ_VERSAO = PB.SQ_VERSAO

        INNER JOIN FI_GBE_SIT_PROCESSO st ON ST.CD_SIT_PROCESSO = PB.CD_SIT_PROCESSO


        LEFT JOIN CS_BLOQUEIO bl
          ON BL. COD_ENTID   = EE1.COD_ENTID
          AND  BL.CD_PLANO  = PB.CD_PLANO
         -- AND BL.CD_INSCRICAO  = PB.CD_MATRICULA
        LEFT JOIN TB_TIPO_BLOQUEIO TP ON  BL.CD_TIPO_BLOQUEIO = TP.CD_TIPO_BLOQUEIO
 

        WHERE   eb.CD_TIPO_ESPECIE IN ( 2, 4, 7, 6 )
                AND EXISTS ( SELECT *
                            FROM   dbo.FI_GBE_FICHA_FINANC_ASSISTIDO ff1
                            WHERE  ff1.SQ_PROCESSO = re.SQ_PROCESSO )
                AND NOT EXISTS ( SELECT *
                                FROM   dbo.FI_GBE_FICHA_FINANC_ASSISTIDO ff2
                                WHERE  ff2.SQ_PROCESSO = re.SQ_PROCESSO
                                        AND ff2.CD_PESSOA_RECEB = re.CD_PESSOA_RECEB )


ORDER BY PB.CD_MATRICULA


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
    base = sanitize_filename(solicitacao)
    ts = time.strftime("%Y%m%d_%H%M%S")
    nome_arquivo = os.path.join(out_dir, f"{base}_{ts}.xlsx")

    print("Gerando Excel...")
    try:
        with pd.ExcelWriter(nome_arquivo, engine="xlsxwriter") as writer:
            sheet = "Dados"
            df.to_excel(writer, sheet_name=sheet, index=False)
            autosize_columns(writer, sheet, df)
        print(f"Arquivo salvo com sucesso: {nome_arquivo}")
        if df.empty:
            print("Aviso: a consulta retornou 0 linhas (planilha criada vazia).")
        else:
            print(f"Linhas: {len(df)}  |  Colunas: {len(df.columns)}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo Excel: {e}")


if __name__ == "__main__":
    main()

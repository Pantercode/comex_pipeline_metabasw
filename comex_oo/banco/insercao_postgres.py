
import os, re, io, math
from getpass import getpass
import pandas as pd
import psycopg2
from psycopg2 import sql
from pandas.api.types import (
    is_integer_dtype, is_float_dtype, is_bool_dtype,
    is_datetime64_any_dtype
)

class InsercaoPostgresComex:
    """
    Insere todos os Parquets da pasta em tabelas no PostgreSQL (schema public).
    """

    def __init__(self, pasta_parquet: str, usuario: str = "postgres",
                 host: str = "localhost", porta: int = 5432, banco: str = "comex",
                 senha: str | None = None):
        self.pasta_parquet = pasta_parquet
        self.usuario = usuario
        self.host = host
        self.porta = porta
        self.banco = banco
        self.senha = senha or getpass("Digite a senha do banco: ")
        self.conn = None

    # ---------- Conexão ----------
    def _conectar(self):
        self.conn = psycopg2.connect(
            host=self.host,
            port=self.porta,
            dbname=self.banco,
            user=self.usuario,
            password=self.senha
        )
        with self.conn.cursor() as cur:
            cur.execute("SET client_encoding TO 'UTF8';")
        self.conn.commit()

    # ---------- Helpers ----------
    @staticmethod
    def _sanitize_col(nome: str) -> str:
        nome = re.sub(r"[^a-zA-Z0-9_]", "_", nome.strip().lower())
        nome = re.sub(r"_+", "_", nome).strip("_")
        if not nome:
            nome = "coluna"
        if nome[0].isdigit():
            nome = f"c_{nome}"
        return nome

    @staticmethod
    def _map_dtype(dtype, serie: pd.Series) -> str:
        if is_integer_dtype(serie):
            return "BIGINT"
        if is_float_dtype(serie):
            return "DOUBLE PRECISION"
        if is_bool_dtype(serie):
            return "BOOLEAN"
        if is_datetime64_any_dtype(serie):
            return "TIMESTAMP"
        return "TEXT"

    def _create_table(self, tabela: str, df: pd.DataFrame):
        cols_sql = []
        for col in df.columns:
            pg_type = self._map_dtype(df[col].dtype, df[col])
            cols_sql.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type)))

        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}.{}")
                        .format(sql.Identifier("public"), sql.Identifier(tabela)))
            cur.execute(sql.SQL("CREATE TABLE {}.{} ({})")
                        .format(sql.Identifier("public"), sql.Identifier(tabela), sql.SQL(", ").join(cols_sql)))
        self.conn.commit()

    def _copy_chunk(self, tabela: str, df_chunk: pd.DataFrame):
        buffer = io.StringIO()
        df_chunk.to_csv(buffer, index=False, header=False, na_rep="\N")
        buffer.seek(0)

        col_list = [sql.Identifier(c) for c in df_chunk.columns]
        copy_sql = sql.SQL(
            "COPY {}.{} ({}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', NULL '\N')"
        ).format(
            sql.Identifier("public"),
            sql.Identifier(tabela),
            sql.SQL(", ").join(col_list)
        )

        with self.conn.cursor() as cur:
            cur.copy_expert(copy_sql.as_string(self.conn), buffer)

    def _count_rows(self, tabela: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}.{}")
                        .format(sql.Identifier("public"), sql.Identifier(tabela)))
            (cnt,) = cur.fetchone()
            return int(cnt)

    # ---------- Processo principal ----------
    def processar_arquivos(self):
        self._conectar()
        arquivos = [f for f in os.listdir(self.pasta_parquet) if f.lower().endswith(".parquet")]
        if not arquivos:
            print("Nenhum arquivo .parquet encontrado em:", self.pasta_parquet)
            return

        for arquivo in arquivos:
            caminho = os.path.join(self.pasta_parquet, arquivo)
            tabela = os.path.splitext(arquivo)[0].lower().replace("-", "_")
            print(f"\n🔄 Processando: {tabela}")
            try:
                df = pd.read_parquet(caminho)
                df = df.rename(columns={c: self._sanitize_col(c) for c in df.columns})
                linhas_parquet = len(df)
                print(f"➡️  Linhas no parquet: {linhas_parquet}")

                self._create_table(tabela, df)

                if linhas_parquet > 0:
                    chunk_size = 300
                    for ini in range(0, linhas_parquet, chunk_size):
                        fim = ini + chunk_size
                        chunk = df.iloc[ini:fim].copy()
                        self._copy_chunk(tabela, chunk)
                    self.conn.commit()

                linhas_pg = self._count_rows(tabela)
                print(f"Linhas na tabela: {linhas_pg}")

                if linhas_pg == linhas_parquet:
                    os.remove(caminho)
                    print(f"✅ Validação OK. Arquivo '{arquivo}' removido.")
                else:
                    print(f"⚠️ Divergência: parquet={linhas_parquet} x postgres={linhas_pg}.")

            except Exception as e:
                print(f"❌ Erro ao processar '{arquivo}': {e}")

        self.conn.close()
        print("\n Processo finalizado.")

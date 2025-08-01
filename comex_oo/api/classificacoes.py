
import pandas as pd
from .requisicao_base import RequisicaoBase

class ClassificacoesComex(RequisicaoBase):
    """
    Baixa as classificações comerciais e salva em Parquet.
    """

    def executar(self):
        response = self.cliente.get("/tables/classifications?language=pt&page=1&perPage=1000")
        classificacoes = response.get("data", [])
        if not classificacoes:
            print(" Nenhuma classificação encontrada.")
            return

        df_raw = pd.DataFrame(classificacoes)

        if 'list' in df_raw.columns:
            df_expandido = pd.json_normalize(df_raw['list'])
            df_final = pd.concat([df_expandido, df_raw['count']], axis=1)
            df_final.to_parquet(self.caminho_saida, index=False)
            print(f"Arquivo salvo em: {self.caminho_saida}")
        else:
            print("Coluna 'list' não encontrada nos dados.")

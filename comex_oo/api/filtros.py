
import pandas as pd
from .requisicao_base import RequisicaoBase

class BlocosEconomicosCidades(RequisicaoBase):
    """
    Baixa blocos econômicos (filtro de cidades) e salva em Parquet.
    """

    def executar(self):
        response = self.cliente.get("/cities/filters/economicBlock?language=pt")
        blocos_raw = response.get("data", [[]])[0]
        df_blocos = pd.DataFrame(blocos_raw)
        df_blocos.rename(columns={"text": "bloco_economico"}, inplace=True)
        df_blocos.to_parquet(self.caminho_saida, index=False)
        print(f"Arquivo salvo em: {self.caminho_saida}")

class MetricasComex(RequisicaoBase):
    """
    Baixa métricas de comércio exterior para cidades e salva em Parquet.
    """

    def executar(self):
        response = self.cliente.get("/cities/metrics?language=pt")
        lista = response.get("data", {}).get("list", [])
        idioma = response.get("data", {}).get("0", "pt")
        df = pd.DataFrame(lista)
        df["idioma"] = idioma
        df.rename(columns={"id": "id_metrica", "text": "metrica"}, inplace=True)
        df.to_parquet(self.caminho_saida, index=False)
        print(f"Métricas salvas em: {self.caminho_saida}")

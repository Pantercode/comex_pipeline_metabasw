
import pandas as pd
from .requisicao_base import RequisicaoBase

class LocalidadesEUA(RequisicaoBase):
    """
    Baixa localidades (UFs) do Brasil na API e salva em Parquet.
    """

    def executar(self):
        response = self.cliente.get("/tables/uf?language=pt")
        ufs = response.get("data", [])
        if not ufs:
            print("Nenhuma UF retornada.")
            return

        df = pd.DataFrame(ufs)
        df.to_parquet(self.caminho_saida, index=False)
        print(f" Arquivo salvo em: {self.caminho_saida}")

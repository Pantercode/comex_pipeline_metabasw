
import pandas as pd
from .requisicao_base import RequisicaoBase

class ModosTransporte(RequisicaoBase):
    """
    Baixa modos de transporte e salva em Parquet.
    """

    def executar(self):
        response = self.cliente.get("/tables/ways?language=pt")
        modos = response.get("data", [])
        if not modos:
            print("Nenhum dado retornado.")
            return

        df = pd.DataFrame(modos)
        df.to_parquet(self.caminho_saida, index=False)
        print(f"Arquivo salvo em: {self.caminho_saida}")

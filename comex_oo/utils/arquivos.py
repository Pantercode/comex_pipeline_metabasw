
from pathlib import Path

def listar_parquets(diretorio: str):
    """
    Retorna lista de caminhos .parquet em `diretorio`.
    """
    return list(Path(diretorio).glob("*.parquet"))

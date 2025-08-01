
from abc import ABC, abstractmethod
from .cliente_api import ClienteAPI

class RequisicaoBase(ABC):
    """
    Classe abstrata base para requisições.
    Cada classe filha deve implementar o método `executar`.
    """

    def __init__(self, cliente: ClienteAPI, caminho_saida: str):
        self.cliente = cliente
        self.caminho_saida = caminho_saida

    @abstractmethod
    def executar(self):
        """Implementar a lógica específica de cada requisição."""
        pass

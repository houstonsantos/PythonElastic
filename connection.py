import requests
from elasticsearch import Elasticsearch


class ConnectionVectra():

    def __init__(self, api_key_id: str, api_key_secret: str, server: str) -> None:
        self.__api_key_id = api_key_id
        self.__api_key_secret = api_key_secret
        self.__server = server
        #self.__es = ''
        pass

    def connection(self) -> None:
        '''
        Annotation
        '''
        res = requests.get(self.__server)

        if res.status_code != 200:
            if res.status_code != 401:
            #print('Usuário sem permissão, conexão com limitações status code').format(res.status_code)
                raise Exception('Erro do request da URL fornecida: {}'.format(res.status_code))
       
        es = Elasticsearch(
            hosts = self.__server,
            api_key = (self.__api_key_id, self.__api_key_secret)
        )

        if not es.ping():
            raise Exception('Conexão não realizada com servidor informado:')
                
        return es


#teste = ConnectionVectra('vhOG-HoBNHcdUFNsTjHD', 'wrJ3OeMBSf6qhpc1aVHdqQ', 'https://faaa243eb75a4fc188857a42510fbae6.vectracs.com.br:9243/')
#teste.connection()
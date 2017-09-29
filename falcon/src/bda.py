#!/usr/bin/env python
# -*- coding: utf-8 -*-
import falcon
import json
from falconjsonio.schema import request_schema
import falconjsonio.middleware
from kafka import KafkaProducer
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

# NOTE: Cluster hace referencia al cluster de cassandra que escuchara
# en el puerto 9042
cluster = Cluster(['cassandra'], port=9042)

# NOTE: Formato que tendran los datos recibidos por el sistema
post_request_schema = {
    'type': 'object',
    'properties': {
        'product': {'type': 'integer'},
        'product_generic_category': {'type': 'integer'},
        'product_specific_category': {'type': 'integer'},
        'site_name': {'type': 'integer'},
        'user_location_country': {'type': 'integer'},
        'user_location_region': {'type': 'integer'},
        'user_location_city': {'type': 'integer'},
        'user_id': {'type': 'integer'},
        'date_time': {'type': 'string'},
        'is_mobile': {'type': 'integer'},
        'channel': {'type': 'integer'},
        'is_purchase': {'type': 'integer'}
    },
    'required': [
        'product',
        'product_generic_category',
        'product_specific_category',
        'site_name',
        'user_location_country',
        'user_location_region',
        'user_location_city',
        'user_id',
        'date_time',
        'is_mobile',
        'channel',
        'is_purchase'
    ],
}


class BdaResource:
    def on_get(self, req, resp):
        """
        NOTE: on_get recibe un query en el cual se especifica el id del producto o del
        usuario 
        - En caso de que se pase el id del producto, el sistema devolvera otros
        productos que fueron comprados por los que compraron el mismo producto
        - En caso de pasarle como query el id del usuario, el sistema devolvera los productos
        recomendados para ese usuario
        """
        session = cluster.connect('bda')
        # Se crea una sesion conectada al cluster cassandra
        # Se parsea el request 
        input = falcon.uri.parse_query_string(req.query_string)
        # Se imprime un log de la entrada
        print('input = {}'.format(input))
        if 'product-lambda' in input:
            # si el parametro product-lambda es contenido en el query
            # se obtiene el id del producto
            product = int(input['product-lambda'])
            # NOTE: se prepara la consulta cql, en este caso para la columna que guarda el 
            # stream de datos (real-time view)
            stream_lookup = session.prepare("SELECT other_products FROM top_other_products_stream WHERE product=?")
            stream_lookup.consistency_level = ConsistencyLevel.ONE
            # se obtienen las filas que cumplan con la condicion product=?, en este caso la id del producto es ?
            stream_rows = session.execute(stream_lookup, [product])
            # NOTE: se prepara la consulta cql, en este caso para la columna que guarda el 
            # batch de datos (batch view)
            batch_lookup = session.prepare("SELECT other_products FROM top_other_products_batch WHERE product=?")
            batch_lookup.consistency_level = ConsistencyLevel.ONE
            # se obtienen las filas que cumplan con la condicion product=?, en este caso la id del producto es ?
            batch_rows = session.execute(batch_lookup, [product])
            resp.status = falcon.HTTP_200
            # result sera el arreglo que contendra los productos sugeridos de la forma
            # los usuarios que compraron [product] tambien compraron los siguientes productos
            result = []
            if batch_rows and stream_rows:
                # si el batch view y stream view no son vacios
                merged = []
                for x in (batch_rows[0].other_products + stream_rows[0].other_products):
                    # Se toman todos los elementos de ambas listas y se verifica
                    if x not in merged:
                        # si aun no se encuentran el la lista de merge(batch-view, realtime-view)
                        # se agregan a la lista de merge
                        merged.append(x)
                # una vez obtenida la lista, se retornan solo los primeros 5 productos
                result = merged[:5]
            elif batch_rows:
                # si solo el batch-view tiene elementos, la lista de productos 
                # en la misma es la retornada como resultado
                result = batch_rows[0].other_products
            elif stream_rows:
                # si solo el batch-view tiene elementos, la lista de productos 
                # en la misma es la retornada como resultado
                result = stream_rows[0].other_products
            # NOTE: se genera el json de respuesta a la consulta
            resp.body = json.dumps({"product": product, "recommendedProducts": result}, encoding='utf-8')
        elif 'user' in input:
            # si user es pasado como parametro en la query
            # se toma el id del usuario pasado
            user = int(input['user'])
            # NOTE: se prepara la consulta cql, en este caso para la tabla que guarda el 
            # los productos recomendados para dicho usuario
            user_lookup = session.prepare("SELECT recommended_products FROM cf WHERE user_id=?")
            user_lookup.consistency_level = ConsistencyLevel.ONE
            # se obtienen las filas que cumplan con la condicion user_id=?, en este caso la id del producto es ?
            rows = session.execute(user_lookup, [user])
            resp.status = falcon.HTTP_200
            result = []
            if rows:
                # si la consulta retorno filas
                # se agrega esta lista a los resultados
                result = rows[0].recommended_products
            # NOTE: se genera el json de respuestas a la consulta, en este caso los productos 
            # recomendados para el usuario consultado
            resp.body = json.dumps({"user": user, "recommendedProducts": result}, encoding='utf-8')
        else:
            resp.status = falcon.HTTP_400
            resp.body = '{"error": "Parametro *product-lambda* o *user* debe ser proveido en la query"}'

    @request_schema(post_request_schema)
    def on_post(self, req, resp):
        """
        NOTE: on_post permite recibir el stream de datos generados por
        el sitio de e-commerce ficticio
        """
        msg = json.dumps(req.context['doc'], encoding='utf-8')
        ''' 
        NOTE: se especifica el servidor en el cual se enviara el stream de datos 
        https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
        - KafkaProducer es el cliente que publica registros al cluster de Kafka
        - producer consiste en un pool de espacio de buffer que contiene los registros
        que todavia no han sido transmitidos al servidor y un thread en background de I/O
        responsable de convertir estos registros en requests y transmitirlos al servidor
        '''
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        '''
        NOTE: el metodo send() de KafkaProducer es asincrono
        Cuando es llamado agrega un registro al buffer de registros con envio pendientes 
        y retorna automaticamente. para este caso send toma los siguientes parametros:
        - topic: un str que indica donde el mensaje será publicado, en este caso 'bda'
        - value: el valor que se desea publicar, en este caso msg
        '''
        producer.send('bda', msg)
        ''' 
        NOTE: flush() hace que todos los metodos en el buffer esten disponibles
        para enviar
        '''
        producer.flush()
        # se crea la respuesta de exito del envio del dato
        resp.status = falcon.HTTP_201
        resp.body = msg

# NOTE: Clase para determinar el estado de la arquitectura
class HealthCheckResource:
    def on_get(self, req, resp):
        """
        NOTE: on_get permite saber el estado de la aplicacion falcon
        """
        resp.status = falcon.HTTP_200
        resp.body = json.dumps({"healthcheck": "ok"}, encoding='utf-8')

# NOTE: Configuracion de la app falcon la app define los middlewares que se 
# usaran para enviar y recibir los registros, en este caso en formato JSON
app = falcon.API(
    middleware=[
        falconjsonio.middleware.RequireJSON(),
        falconjsonio.middleware.JSONTranslator(),
    ],
)

bda = BdaResource()
healthcheck = HealthCheckResource()

# NOTE: Se agrega a la aplicacion la ruta a la cual se realizaran los POSTS y GETS
app.add_route('/bda', bda)
# Ruta en la cual se puede checkear el estado de la arquitectura
app.add_route('/healthcheck', healthcheck)

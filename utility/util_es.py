from elasticsearch import Elasticsearch, helpers, OrjsonSerializer
from elasticsearch import BadRequestError
import os

es_host = os.getenv("ES_SERVER")
es_api_key = os.getenv("ES_API_KEY")
# es_username = os.getenv("ES_USERNAME")
# es_password = os.getenv("ES_PASSWORD")

es = Elasticsearch(
    hosts=[f"{es_host}"],
    # basic_auth=(es_username, es_password),
    api_key=es_api_key,
    serializer=OrjsonSerializer(),
    http_compress=True,
    max_retries=10,
    connections_per_node=100,
    request_timeout=120,
    retry_on_timeout=True,
)


def get_es() -> Elasticsearch:
    return es


def batchify(docs, batch_size):
    for i in range(0, len(docs), batch_size):
        yield docs[i:i + batch_size]


def bulkLoadIndex( es, json_docs, index_name, id_param, batch_size=10):
    # doc_type = "_doc"

    # Create the index with the mapping if it doesn't exist
    if not es.indices.exists(index=index_name):
        raise BadRequestError(f"Index [{index_name}] needs to exist before bulk loading")

    batches = list(batchify(json_docs, batch_size))

    for batch in batches:
        # Convert the JSON documents to the format required for bulk insertion
        bulk_docs = [
            {
                "_op_type": "index",
                "_index": index_name,
                "_source": doc,
                "_id": doc[id_param]
            }
            for doc in batch
        ]

        # Perform bulk insertion
        success, errors =  helpers.bulk(es, bulk_docs, raise_on_error=False)
        if errors:
            for error in errors:
                print(error)



def search_to_context(es: Elasticsearch, index_name: str, body: dict, rag_context: str, trim_context_len: int) -> list:
    results = es.search(index=index_name, body=body)

    context = []
    # results['hits']['hits'] is the list of hits returned by Elasticsearch
    for hit in results['hits']['hits'][:trim_context_len]:
        # Safely get the value in case `rag_context` is missing
        context_value = hit["_source"].get(rag_context, "")
        context.append(str(context_value))

    return context
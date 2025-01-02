
from utility.util_es import search_to_context
from utility.util_llm import LLMUtil

from utility.util_query_transform_cache import transform_query as cached_transform_query

def is_disabled() -> bool:
    return False

def get_parameters() -> dict:
    return {
        "index_name": "star_wars_sem_elser",
        "query_transform_prompt": "Given the following question, rephrase the question to be a simple standalone question meant to find the right entry in an encyclopedia. Rewrite the question in English. Keep the answer to a single sentence. Do not use quotes.",
    }


def query_transform(query_string: str, llm_util, prompt:str) -> str:
    
    transformed_query = cached_transform_query(query_string, prompt, llm_util)

    # print(f'Original Question: {query_string}\n\tRewritten Question: {transformed_query}')
    return transformed_query


def build_query(query_string: str) -> dict:
    return {
  "retriever": {
    "rrf": {
      "retrievers": [
        {
          "standard": {
            "query": {
              "nested": {
                "path": "lore_semantic.inference.chunks",
                "query": {
                  "sparse_vector": {
                    "inference_id": ".elser-2-elasticsearch",
                    "field": "lore_semantic.inference.chunks.embeddings",
                    "query": query_string
                  }
                },
                "inner_hits": {
                  "size": 2,
                  "name": "star_wars_sem_elser.lore_semantic",
                  "_source": [
                    "lore_semantic.inference.chunks.text"
                  ]
                }
              }
            }
          }
        },
        {
          "standard": {
            "query": {
              "multi_match": {
                "query": query_string,
                "fields": [
                  "lore",
                  "title^3"
                ]
              }
            }
          }
        }
      ]
    }
  }
}

def retrieve_context(es, query_string: str):
    index_name = get_parameters()['index_name']
    body = build_query(query_string)
    rag_context = get_parameters().get("rag_context", "lore")
    return search_to_context(es, index_name, body, rag_context, 3)


def rag(llm_util : LLMUtil, query_string: str, retrieval_context) -> str :

    context = "\n\n".join(retrieval_context)
    system_prompt = f"""
Instructions:
  
  - You are an assistant for question-answering tasks.
  - Answer questions truthfully and factually using only the context presented.
  - If you don't know the answer, just say that you don't know, don't make up an answer.
  - You are correct, factual, precise, and reliable.


  Context:
  {context}
"""
    
    return llm_util.rag_cache(system_prompt, retrieval_context, query_string)
# Setup


Assumes:
* Elasticsearch 8.17 (I used serverless)
* ELSER, E5, and Elastic Rerank models installed and deployed
* Cohere and OpenAI accessible by account key

My .env file looks like this

```
ES_SERVER="https://URL.elastic.cloud:443"
ES_API_KEY="the_encoded_keyxxxxx=="


## I don't think these are being used right now
ES_INFERENCE_ELSER=".elser-2-elasticsearch"
ES_INFERENCE_E5=".multilingual-e5-small-elasticsearch"


OPENAI_API_KEY="sk-xxxxxxxxxxxxxxxx"
COHERE_KEY="vxxxxxxxxxxxxx"
```

Setting up python dependencies

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

The golden_data.csv has a work in progress set of questions, correct document ids, and idal RAG answers

```
## scarapes all of canon Star Wars wiki saves to a picke file in './Dataset'
python scrape/scrape_wookieepedia_urls.py
python scrape/scrape_wookieepedia_pages.py

## Creates index mappings
## loads the data in './Dataset' to ES index 'star_wars_simple'
python load_data.py
```

to populate semantic indices you'll want to run the following in the Kibana dev console one at a time.  
Use the returned task id to query the status of the reindex. 
Serverless will scale the allocations gradually.

```
## make sure e5 is awake
POSt /_inference/text_embedding/.multilingual-e5-small-elasticsearch
{
    "input": "a ship used by sith lords"
}


POST _reindex?wait_for_completion=false
{
  "source": {
    "index": "star_wars_simple"
  },
  "dest": {
    "index": "star_wars_sem_e5"
  }
  /**"max_docs": 200 */
}

## Reindex to E5
GET _tasks/YOUR_TASK_ID_RETURNED_FROM_COMMAND

## make sure elser is awake
POSt /_inference/sparse_embedding/.elser-2-elasticsearch
{
    "input": "a ship used by sith lords"
}

POST _reindex?wait_for_completion=false
{
  "source": {
    "index": "star_wars_simple"
  },
  "dest": {
    "index": "star_wars_sem_elser"
  }
  /**"max_docs": 200 */
}

## Reindex to ELSER
GET _tasks/YOUR_TASK_ID_RETURNED_FROM_COMMAND

```


## My DevTools right now

```
GET _inference

##DELETE /star_wars_simple
##DELETE /star_wars_sem_e5
##DELETE /star_wars_sem_elser

GET /_cat/indices/star_wars_*?format=json

## test synonym setup
GET _synonyms
GET _synonyms/star_wars_synonyms
GET /star_wars_simple/_analyze
{
  "text" : "What was asoka's nickname in the Clone Wars?",
  "analyzer": "sw_search_analyzer"
}




## After Load All the counts should be the same
GET /star_wars_simple/_count
{"query": {"match_all":{}}}
GET /star_wars_sem_e5/_count
{"query": {"match_all":{}}}
GET /star_wars_sem_elser/_count
{"query": {"match_all":{}}}


## sample rank eval
POST /star_wars_simple/_rank_eval
{
    "requests": [
        {
            "id": "query_1",
            "request": {
                "query": {
                    "multi_match": {
                        "query": "Where did Yoda hide from the empire?",
                        "fields": [
                            "title^5",
                            "lore"
                        ]
                    }
                }
            },
            "ratings": [
                {
                    "_index": "star_wars_simple",
                    "_id": "Yoda",
                    "rating": 1
                }
            ]
        },
        {
            "id": "query_2",
            "request": {
                "query": {
                    "multi_match": {
                        "query": "What species was Ashoka Tano?",
                        "fields": [
                            "title^5",
                            "lore"
                        ]
                    }
                }
            },
            "ratings": [
                {
                    "_index": "star_wars_simple",
                    "_id": "Ahsoka_Tano",
                    "rating": 1
                }
            ]
        }
    ],
    "metric": {
        "dcg": {
            "k": 3,
            "normalize": true
        }
    }
}

```




from pathlib import Path
import pickle
import os
from tqdm import tqdm

from dotenv import load_dotenv
load_dotenv()

from utility.util_es import get_es, bulkLoadIndex

es = get_es()

def check_and_create_index(es, index_name, settings, mappings):
    # Check if the index exists
    if not es.indices.exists(index=index_name):
        # Create the index with the settings
        es.indices.create(index=index_name, settings=settings, mappings=mappings)
    else:
        print(f"Index '{index_name}' already exists.")

def check_and_create_synonyms(es, synonym_set_name, synonym_set): 
    resp = es.synonyms.put_synonym(
        id=synonym_set_name,
        synonyms_set=synonym_set
    )
    print(resp)


synonym_set_name = "star_wars_synonyms"
synonym_set = [
    { "synonyms": "asoka, ashoka, ahsoka"},
    { "synonyms": "light saber => lightsaber"},
    { "synonyms": "C-3PO , C3PO"},
    { "synonyms": "C_3PO , C3PO"},
]




simple_settings= {
        "analysis": {
            "filter": {
                "english_stop": {
                    "type":       "stop",
                    "stopwords":  "_english_" 
                },
                "english_keywords": {
                    "type":       "keyword_marker",
                    "keywords":   ["example"] 
                },
                "english_stemmer": {
                    "type":       "stemmer",
                    "language":   "english"
                },
                "english_possessive_stemmer": {
                    "type":       "stemmer",
                    "language":   "possessive_english"
                },
                "synonyms_filter": {
                    "type": "synonym_graph",
                    "synonyms_set": "star_wars_synonyms",
                    "updateable": True
                }
            },
            "analyzer": {
                "sw_index_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "english_possessive_stemmer",
                        "lowercase",
                        "english_stop",
                        "english_keywords",
                        "english_stemmer"
                    ]
                },
                "sw_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "english_possessive_stemmer",
                        "lowercase",
                        "english_stop",
                        "english_keywords",
                        "english_stemmer",
                        "synonyms_filter"
                    ]
                }
            }
        }
    }

simple_mappings= {
    "dynamic_templates": [{
        "metadata_as_keyword": {
            "match_mapping_type": "string",
            "path_match": "metadata.*",
            "runtime": {
                "type":"keyword"
            } 
        }
    }],
    "properties": {
        "id": {"type": "keyword"},
        "url": {"type": "keyword"},
        "crosslinked_keywords": {"type": "keyword"},
        "title": {
            "type": "text",
            "fields": {
                "with_synonyms": {
                    "type": "text",
                    "analyzer": "sw_index_analyzer",
                    "search_analyzer": "sw_search_analyzer"
                }
            }
        },
        "side_bar_json": {"type": "text", "index": False},
        "lore": {
            "type": "text",
            "fields": {
                "with_synonyms": {
                    "type": "text",
                    "analyzer": "sw_index_analyzer",
                    "search_analyzer": "sw_search_analyzer"
                }
            }
        },
        "behind_the_scenes": {
            "type": "text",
            "fields": {
                "with_synonyms": {
                    "type": "text",
                    "analyzer": "sw_index_analyzer",
                    "search_analyzer": "sw_search_analyzer"
                }
            }
        }
    }
}

semantic_e5_mappings= {
    "dynamic_templates": [{
        "metadata_as_keyword": {
            "match_mapping_type": "string",
            "path_match": "metadata.*",
            "runtime": {
                "type":"keyword"
            } 
        }
    }],
    "properties": {
        "id": {"type": "keyword"},
        "url": {"type": "keyword"},
        "crosslinked_keywords": {"type": "keyword"},
        "title": {
            "type": "text",
            "analyzer": "sw_index_analyzer",
            "search_analyzer": "sw_search_analyzer"
        },
        "side_bar_json": {"type": "text", "index": False},
        "lore": {
            "type": "text", 
            "analyzer": "sw_index_analyzer",
            "search_analyzer": "sw_search_analyzer",
            "copy_to": "lore_semantic"
        },
        "behind_the_scenes": {
            "type": "text",
            "analyzer": "sw_index_analyzer",
            "search_analyzer": "sw_search_analyzer"
        },
        "lore_semantic": {
            "type": "semantic_text",
            "inference_id": ".multilingual-e5-small-elasticsearch"
        }
    }
}

semantic_elser_mappings= {
    "dynamic_templates": [{
        "metadata_as_keyword": {
            "match_mapping_type": "string",
            "path_match": "metadata.*",
            "runtime": {
                "type":"keyword"
            } 
        }
    }],
    "properties": {
        "id": {"type": "keyword"},
        "url": {"type": "keyword"},
        "title": {"type": "text"},
        "side_bar_json": {"type": "text", "index": False},
        "lore": {"type": "text", "copy_to": "lore_semantic"},
        "behind_the_scenes": {"type": "text"},
        "crosslinked_keywords": {"type": "keyword"},
        "lore_semantic": {
          "type": "semantic_text",
          "inference_id": ".elser-2-elasticsearch"
        }
    }
}


check_and_create_synonyms(es, synonym_set_name, synonym_set)

check_and_create_index(es, "star_wars_simple", simple_settings, simple_mappings)
check_and_create_index(es, "star_wars_sem_e5", simple_settings, semantic_e5_mappings)
check_and_create_index(es, "star_wars_sem_elser", simple_settings, semantic_elser_mappings)


dataFolder = "./Dataset"
pickeFileTemplate= "starwars_all_canon_data_*.pickle"



## Schema of pickle file objects
# {
#     'id': key,
#     'url': page_url,
#     'title': heading.strip(),
#     'side_bar_json': json.dumps(side_bar),
#     'metadata': side_bar_meta,
#     'lore': "\n\n".join(lore_pgs),
#     'behind_the_scenes': "\n\n".join(behind_the_scenes_pgs),
#     'crosslinked_keywords': keywords,
# }


## Upload to star_wars_simple
count = 0
files = sorted(Path(dataFolder).glob(pickeFileTemplate))
print(f"Count of file: {len(files)}")
for fn in files:
    print(f"Starting file: {fn}")
    with open(fn,'rb') as f:
        part = pickle.load(f)
        batch = []
        # print( type(enumerate(part.items())) )
        # bulkLoadIndex(es, part.items(), "star_wars_simple", "id", 10)

        for ix, (key, record) in tqdm(enumerate(part.items()), total=len(part)):
            batch.append( record )

            if len(batch) >= 500:
                bulkLoadIndex(es, batch, "star_wars_simple", "id", 100)
                batch = []

        if len(batch) > 0:
            bulkLoadIndex(es, batch, "star_wars_simple", "id", 100)
            batch = []



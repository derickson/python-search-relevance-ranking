import csv
import json
import glob
import os
import importlib.util
import traceback

from dotenv import load_dotenv
load_dotenv()

from utility.util_es import get_es
from utility.util_llm import LLMUtil
from utility.util_deep_eval import generateLLMTestCase, evaluateTestCases

from deepeval.evaluate import TestResult

openai_api_key = os.getenv("OPENAI_API_KEY")
llm_util = LLMUtil(openai_api_key)


OUTPUT_CSV = "search_evaluation_results.csv"
STRATEGIES_FOLDER = "strategies"       # Folder containing *.py strategy files
GOLDEN_DATA_CSV = "golden_data.csv"    # CSV with columns: query, best_ids, natural_answer (or similar)


def load_strategies(folder_path):
    """
    Dynamically load each .py file in folder_path as a strategy module.
    We assume each file has a function `build_query(query_string: str) -> dict`.
    
    Returns a dict: { strategy_name: module_object }
    """
    strategies = {}
    for file_path in glob.glob(os.path.join(folder_path, "*.py")):
        strategy_name = os.path.splitext(os.path.basename(file_path))[0]
        
        spec = importlib.util.spec_from_file_location(strategy_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Store the module in the dictionary
        strategies[strategy_name] = module
    return strategies

def load_golden_data(csv_path):
    """
    Load the golden data CSV.
    Expects columns:
      query, best_ids, [natural_answer, ...] 
    or something similar.
    
    Returns a list of dicts, for example:
    [
      {
        "query": "What is Python used for?",
        "best_ids": ["doc123", "doc129"],
        "natural_answer": "..."
      },
      ...
    ]
    """
    data = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ## Parse best IDs into a list if comma-separated
            best_ids_list = row["best_ids"].split(",") if "best_ids" in row else []
            best_ids_list = [x.strip() for x in best_ids_list]
            
            data.append({
                "query": row["query"],
                "best_ids": best_ids_list,
                "natural_answer": row.get("natural_answer", "")
            })
    return data


def build_rank_eval_request(golden_data, strategy_module):
    """
    Build the request body for the _rank_eval API.
    This function prepares 'requests' for each query, 
    assigning rating=1 for each doc in the 'best_ids' list.
    
    The DSL 'request' is taken from `strategy_module.build_query(...)`.
    """
    index_name = strategy_module.get_parameters()['index_name']
    requests = []
    
    for i, item in enumerate(golden_data):
        qid = f"query_{i+1}"

        query_string = strategy_module.query_transform(item["query"], llm_util,  strategy_module.get_parameters()["query_transform_prompt"]) if hasattr(strategy_module, "query_transform") else item["query"]

        query_dsl = strategy_module.build_query(query_string)
        
        ## Build ratings
        ratings = []
        for doc_id in item["best_ids"]:
            ratings.append({"_id": doc_id, "rating": 1, "_index": index_name})
        
        ## Append each request as a dict with the correct structure
        requests.append({
            "id": qid,
            "request": query_dsl,
            "ratings": ratings
        })

    
    ## Rank eval body
    rank_eval_body = {
        "requests": requests,
        "metric": {
            "dcg": {
                "k": 10,
                "normalize": True
            }
            # "recall": {
            #     "k": 10,
            #     "relevant_rating_threshold": 1
            # }
        }
    }
    
    return rank_eval_body



def main():
    # 1. Connect to Elasticsearch
    es = get_es()
    
    
    # 2. Load the golden data set
    golden_data = load_golden_data(GOLDEN_DATA_CSV)
    # print(json.dumps(golden_data, indent=4))

    # 3. Load strategies from the strategies folder
    strategy_modules = load_strategies(STRATEGIES_FOLDER)  # {name: module}

    ## We will store results in a structure like:
    ## {
    ##   query_text1: { "bm25": 0.88, "semantic": 0.79, ... },
    ##   query_text2: { "bm25": 0.92, ... },
    ##   ...
    ## }
    results = {}

    ## Search rank Evaluation
    print("\b### SEARCH RANK EVAL")
    for strategy_name, module in strategy_modules.items():

        if hasattr(module, "is_disabled") and module.is_disabled(): ## or strategy_name != "1a_bm25" :
            print(f"Skipping strategy: {strategy_name}")
            continue

        print(f"Starting strategy: {strategy_name}")
        rank_eval_body = build_rank_eval_request(golden_data, module)
        # print(json.dumps(rank_eval_body, indent=4))

        index_name = module.get_parameters()['index_name']
        
        # 4. Call the _rank_eval API
        try:
            response = es.rank_eval(body=rank_eval_body, index=index_name)
            # print(response)
            ## response structure reference:
            ## {
            ##   "metric_score": 0.85,   # overall metric
            ##   "details": {
            ##       "query_1": { "metric_score": 1.0, "unrated_docs": [], ... },
            ##       "query_2": { "metric_score": 0.5, ... },
            ##       ...
            ##   }
            ## }

            ## Update results
            for i, item in enumerate(golden_data):
                qid = f"query_{i+1}"
                query_text = item["query"]
                
                ndcg_for_this_query = response["details"][qid]["metric_score"]
                
                if query_text not in results:
                    results[query_text] = {}
                results[query_text][strategy_name] = ndcg_for_this_query

            # print(json.dumps(response, indent=4))
        except Exception as e:
            print(f"Error running rank_eval for strategy {strategy_name}: {e}")
            traceback.print_exc()  
            print(json.dumps(rank_eval_body, indent=4))

            # Optionally fill with None or 0
            for item in golden_data:
                query_text = item["query"]
                if query_text not in results:
                    results[query_text] = {}
                results[query_text][strategy_name] = None


    ## Deep Eval Evaluation
    print("### DEEP EVAL")
    deepEvalScores = {}
    for strategy_name, module in strategy_modules.items():
        if hasattr(module, "is_disabled") and module.is_disabled(): ## or strategy_name != "1a_bm25" :
            print(f"Skipping strategy: {strategy_name}")
            continue

        print(f"Starting strategy: {strategy_name}")
        testCases = []
        for i, item in enumerate(golden_data):
                qid = f"query_{i+1}"
                query = item["query"]


                ## correct answer from the golden data
                correct_answer = item["natural_answer"]

                ## pre-process the query string
                query_string = module.query_transform(query, llm_util,  module.get_parameters()["query_transform_prompt"]) if hasattr(module, "query_transform") else query

                ## do the RAG
                retrieval_context = module.retrieve_context(es, query_string)
                actual_output = module.rag(llm_util, query_string, retrieval_context)


                ## fill in query and strategy responses in score sheet
                stratResult = {"actual_output": actual_output}
                if qid not in deepEvalScores:
                    deepEvalScores[qid] = { 
                        "query" : query, 
                        "correct_answer": correct_answer,
                        "strategies": { strategy_name: stratResult} }
                else:
                    deepEvalScores[qid]["strategies"][strategy_name] = stratResult

                ## prep deel eval test case for later batch evaluation
                testCase = generateLLMTestCase(qid, query, actual_output, retrieval_context, correct_answer)
                testCases.append(testCase)

        ## Run evaluations for this strategy      
        rag_evaluation = evaluateTestCases(testCases)

        for test_result in  rag_evaluation.test_results:
            quid = test_result.name

            success = test_result.success
            scores = {"success": success}
            # print(f"name: {quid} | success: {success}")
            for metric in  test_result.metrics_data:
                # print(f"{metric.name} : score {metric.score} | {metric.reason}")
                scores[metric.name] = {"score": metric.score, "reason": metric.reason }
            
            deepEvalScores[quid]["strategies"][strategy_name]["scores"] = scores
            
            

    ## save the scores to disk
    # print(json.dumps(deepEvalScores, indent=2))
    with open("deepeval_results.json", "w") as f:
        json.dump(deepEvalScores, f, indent=2)



    strategy_names = list(strategy_modules.keys())
    strategy_names.sort()  # Sort the list in ascending order


    # Write two CSVs for the deep eval results
    with open("deepeval_results_relevancy.csv", "w", newline="", encoding="utf-8") as f_rel:
        with open("deepeval_results_correctness.csv", "w", newline="", encoding="utf-8") as f_cor:

            relevancy_writer = csv.writer(f_rel)
            correctness_writer = csv.writer(f_cor)

            # Header row: "query" + all strategy names
            header = ["query"] + strategy_names
            relevancy_writer.writerow(header)
            correctness_writer.writerow(header)
        
            # For each query in the dictionary
            for qid, query_data in deepEvalScores.items():
                query_text = query_data["query"]
                strategies_data = query_data.get("strategies", {})

                # Start the row with the query text
                relevancy_row = [query_text]
                correctness_row = [query_text]
            
                # For each strategy, grab its "Answer Relevancy" score if it exists
                for strategy_name in strategy_names:
                    strategy = strategies_data.get(strategy_name)
                    if strategy is not None:
                        # Safely get the relevancy score
                        relevancy_score = strategy["scores"]["Answer Relevancy"]["score"]
                        relevancy_row.append(relevancy_score)
                        correctness_score = strategy["scores"]["Correctness (GEval)"]["score"]
                        correctness_row.append(correctness_score)
                    else:
                        relevancy_row.append(None)  # Or "", if you prefer
                        correctness_row.append(None)  # Or "", if you prefer
                relevancy_writer.writerow(relevancy_row)
                correctness_writer.writerow(correctness_row)



    with open(OUTPUT_CSV, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Header: query plus one column per strategy
        header_row = ["query"] + strategy_names
        writer.writerow(header_row)
        
        # Write each query row
        per_strategy_sums = {s: 0.0 for s in strategy_names}
        per_strategy_counts = {s: 0 for s in strategy_names}
        
        for query_text, row_scores in results.items():
            row_to_write = [query_text]
            for s in strategy_names:
                score = row_scores.get(s, None)
                row_to_write.append(score if score is not None else "")
                if score is not None:
                    per_strategy_sums[s] += score
                    per_strategy_counts[s] += 1
            writer.writerow(row_to_write)

        # Write total (avg) row
        total_row = ["TOTAL"]
        for s in strategy_names:
            if per_strategy_counts[s] > 0:
                avg_score = per_strategy_sums[s] / per_strategy_counts[s]
                total_row.append(avg_score)
            else:
                total_row.append("")
        writer.writerow(total_row)

    print(f"Evaluation complete. Results written to {OUTPUT_CSV}")



if __name__ == "__main__":
    main()


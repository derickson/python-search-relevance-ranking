from pathlib import Path
import pickle
import os
from tqdm import tqdm

from typing import Optional
import re
import requests
from bs4 import BeautifulSoup
import json


## Go through already parsed files
dataFolder = "./Dataset"
pickeFileTemplate= "starwars_all_canon_data_*.pickle"

newDataFolder = "./RepairDataset"


files = sorted(Path(dataFolder).glob(pickeFileTemplate))
print(f"Count of file: {len(files)}")


failCounter = 0
## Open the repaired file
repairedFn = dataFolder + f'/repair_starwars_all_canon_data.pickle'
with open(repairedFn,'rb') as repairedF:
    repairedDict = pickle.load(repairedF)
    
    ## merge data strcuture
    integrated_records = {}
    
    for fn in files:
        print(f"Starting read on file: {fn}")
        with open(fn,'rb') as f:
            part = pickle.load(f)
            for ix, (key, record) in tqdm(enumerate(part.items()), total=len(part)):
                lore = record['lore']
                is_short_lore = lore.count('\n') == 0
                if(is_short_lore):
                    if(key in repairedDict):
                        integrated_records[key] = repairedDict[key]
                    else:
                        failCounter = failCounter + 1
                        print(f"Failed to find repaired record for {key}")
                else:
                    integrated_records[key] = record
        writeFn = str(fn).replace('Dataset', 'RepairDataset')
        print(f"Persisting new file: {writeFn}")
        with open(writeFn, 'wb') as wf:
            pickle.dump(integrated_records, wf, protocol=pickle.HIGHEST_PROTOCOL)
        
        ## clear merged data strcuture for next file
        integrated_records = {}
print(f"records with issues: {failCounter}")
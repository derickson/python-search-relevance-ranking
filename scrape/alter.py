from pathlib import Path
import pickle
import os
from tqdm import tqdm

from typing import Optional
import re
import requests
from bs4 import BeautifulSoup
import json

import copy

## Go through already parsed files
dataFolder = "./Dataset"
pickeFileTemplate= "starwars_all_canon_data_*.pickle"

newDataFolder = "./DatasetNew"


files = sorted(Path(dataFolder).glob(pickeFileTemplate))
print(f"Count of file: {len(files)}")


failCounter = 0
    
## merge data strcuture
integrated_records = {}

for fn in files:
    print(f"Starting read on file: {fn}")
    with open(fn,'rb') as f:
        part = pickle.load(f)
        for ix, (key, record) in tqdm(enumerate(part.items()), total=len(part)):

            copyRecord = copy.deepcopy(record)

            if 'lore' in copyRecord and copyRecord['lore'] == "":
                del copyRecord['lore']
            
            if 'behind_the_scenes' in copyRecord and copyRecord['behind_the_scenes'] == "":
                del copyRecord['behind_the_scenes']
            
            integrated_records[key] = copyRecord

    writeFn = str(fn).replace('Dataset', 'DatasetNew')
    print(f"Persisting new file: {writeFn}")
    with open(writeFn, 'wb') as wf:
        pickle.dump(integrated_records, wf, protocol=pickle.HIGHEST_PROTOCOL)
    
    ## clear merged data strcuture for next file
    integrated_records = {}

print(f"records with issues: {failCounter}")
import pandas as pd 
import sys

# get the command line arguments
args = sys.argv
File_path = args[1]
# read the data
chunksize = 10000 # set chunksize as needed
dfs = []
for chunk in pd.read_csv(File_path,sep='\t', on_bad_lines='skip',chunksize=1000):
    dfs.append(chunk)
df = pd.concat(dfs,ignore_index=True)
new_df = df.sort_values(by = "total_votes", ascending = False).iloc[:200]
new_df.drop(columns=["review_headline","review_body"],axis = 1, inplace = True)
file_name = File_path.replace(".","").replace("/","")
new_df.to_csv("Processed_"+file_name+".csv", index = False)#



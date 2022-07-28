import requests
from tqdm import tqdm
import pandas as pd
import random
import time
# all_urls = pd.DataFrame()
path = '/Users/kiansheik/Downloads/urls_to_check.csv'
print(f"Loading urls from {path}")
all_urls = {"CA": ['https://www.realestateagents.com/agents/profile/michaela-hellman-1','https://www.realestateagents.com/agents/profile/michaela-hellman-1000']}
urls_to_check = pd.read_csv(path)      
all_urls = { k:[x[0] for x in dd.values] for (k,dd) in dict(tuple(urls_to_check.groupby('state'))).items() }
print("URLs loaded...testing states...")

# Given a list, x, sample_list returns
# a list, sample, of n random samples from x
# and also the remainder of x without
# the entries in sample
# We can use this repeatedly to exhaust a list
# randomly
def sample_list(x, n=5):
	random.shuffle(x)
	sample = x[:n]
	x = x[n:]
	return sample, x

while sum([len(x) for x in all_urls.items()]) > 0:
	for i, state in enumerate(all_urls.keys()):
		print(f"\nTesting {state}...({i+1}/{len(all_urls.keys())})")
		# Get sample of urls to test with and write new list
		sample, new_list = sample_list(all_urls[state])
		all_urls[state] = new_list
		for url in tqdm(sample):
			url = f"https://rea.staging.referralexchange.com{url}"
			resp = requests.get(url)
			# breakpoint()
			if resp.status_code != 200:
				with open("errors.txt", "a") as file_object:
					err = f"{resp.status_code}\t{state}\t{url}"
					print(f"{err}")
					file_object.write(f"\n{err}")
			time.sleep(0.1)
	print("Finished")
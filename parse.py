import pandas as pd
import os

path = os.path.join("docs")

def main(path):
	columns = ["code", "company_name", "title", "timestamp", "filename"]
	df = pd.DataFrame(columns=columns)
	for file in os.listdir(path):
		if file[-3:] == "txt":
			elements = file.rstrip('.txt').split('_')
			elements.append(file)
			df = df.append({key:val for key, val in zip(columns, elements)}, ignore_index=True)

	df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y%m%d")
	df.to_csv("df.csv", index=False)
	print(df.head())
main(path)
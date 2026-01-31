import pandas as pd

# CAC 40
url = "https://finance.yahoo.com/quote/%5EFCHI/components?p=%5EFCHI"
tables = pd.read_html(url)
df_cac40 = tables[0]
print(df_cac40[["Symbol", "Name"]])
import pandas as pd

df = pd.read_csv('input2.csv')
contagem = df['nome'].value_counts().reset_index()
contagem.columns = ['nome', 'quantidade']
contagem.to_csv('stdout.txt', index=False)

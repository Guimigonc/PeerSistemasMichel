import pandas as pd

df = pd.read_csv("input.csv")
primos = []

for num in df["numeros"]:
    if num < 2:
        continue
    if all(num % i != 0 for i in range(2, int(num**0.5) + 1)):
        primos.append(num)

print("Números primos:", primos)

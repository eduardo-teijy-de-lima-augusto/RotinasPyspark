# Projeto Estudo Databricks com Pyspark.



> Após a criação de um notebook no databricks, importamos 3 arquivos de exemplo para o repositório padrão do databricks (VER mais sobre DBFS)
```py
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/duaugusto@gmail.com/modelo_carro.csv")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/duaugusto@gmail.com/marcas_duplicadas.csv")
df3 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/duaugusto@gmail.com/marca_carro.csv")

```
---

> Vamos dar um display para verificar os dataframes carregados a partir dos arquivos.

```py
display(df1)
display(df2)
display(df3)

```
---

> Agora vamos escrever esses dataframes em outro local (pasta) usando o write.

```py
df1.write.format("csv").save("/aprendizado/modelo_carro")
df2.write.format("csv").save("/aprendizado/marcas_duplicadas")
df3.write.format("csv").save("/aprendizado/marca_carro")

```
---

> Uma vez criado os dataframes no local indicado podemos usar o comando append ou overwrite, onde o primeiro vai inserir mais dados ao dataframe e o segundo substituirá o conteúdo do dataframe.

```py
# Insere novos registro ou duplica os mesmos.
df1.write.format("csv").mode("append").save("/aprendizado/modelo_carro")

# Substitui o conteúdo.
df2.write.format("csv").mode("overwrite").save("/aprendizado/marcas_duplicadas")
df3.write.format("csv").mode("overwrite").save("/aprendizado/marca_carro")

```
---

> Agora vamos contar as linhas dos dataframes.

```py
# Nesse caso estamos lendo o primeiro df que foi gravado no diretorio padrão DBFS, não confundir.
print(df1.count())

# nesse caso estamos lendo o diretorio aprendizado que criamos, ou seja, locais diferentes.
df_carros = spark.read.format("csv").load("/aprendizado/modelo_carro")
print(df_carros.count())

# Visualizando os outros dfs gravados no DBFS.
print(df2.count())
print(df3.count())

```
---

```py
# Passando a option "header" para que possamos ler o cabeçalho que esta na primeira linha do arquivo.
> df_carros = spark.read.format("csv").option("header", True).load("/aprendizado/modelo_carro")

```


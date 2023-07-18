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

> DELETANDO PASTAS NO DATABRICKS ****TOMAR CUIDADO.

```PY
# Funciona
dbutils.fs.rm("/aprendizado/modelo_carro", True)

# Funciona também.
%fs rm -r /aprendizado/modelo_carro

```
---

> Agora vamos contar as linhas dos dataframes.

```py
# Nesse caso estamos lendo o primeiro df que foi gravado no diretorio padrão DBFS, não confundir.
print(df1.count())

# nesse caso estamos lendo o diretorio aprendizado que criamos, ou seja, locais diferentes.
# a opção .option("header", True) possibilita colocar a primeira linha como cabeçalho.
df_carros = spark.read.format("csv").option("header", True).load("/aprendizado/modelo_carro")
print(df_carros.count())

# Visualizando os outros dfs gravados no DBFS.
print(df2.count())
print(df3.count())

```
---

```py
# Passando a option "header" para que possamos ler o cabeçalho que esta na primeira linha do arquivo.
df_carros = spark.read.format("csv").option("header", True).load("/aprendizado/modelo_carro")

# Podemos ainda colocar a option delimiter para dizer qual é o delimitador usado no csv
df_carros = spark.read.format("csv").option("header", True).option("delimiter", ",").load("/aprendizado/modelo_carro")


# Para concatenar melhor o codigo se usa a \
# Podemos ainda colocar a option delimiter para dizer qual é o delimitador usado no csv
df_carros = spark.read.format("csv").option("header", True).option("delimiter", ",").load("/aprendizado/modelo_carro")
display(df_carros)

# Outra forma de passar o delimitador é:
df_carros = spark.read \
            .format("csv") \
            .option("header", True) \
            .load("/aprendizado/modelo_carro", sep = ",")  #outra forma com sep
display(df_carros)

# Nesse codigo usamos a option enconding utf-8 para caracteres e acentuações ja concatenado.
df_carros = spark.read \
            .format("csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("encoding", "utf-8") \
            .load("/aprendizado/modelo_carro") 

```
---

> Podemos escrever os dados que estão em csv para outros formatos como parquet, json ou avro.

```py
# Escrevendo os arquivos em formato parquet, json e avro
# Nao esquecendo que também podemos usar o option append ou overwrite
df_carros.write.format("parquet").save("/aprendizado/modelo_carro_parquet")
df_carros.write.format("json").save("/aprendizado/modelo_carro_json")
df_carros.write.format("avro").save("/aprendizado/modelo_carro_avro")

```
---
> Agora vamos ler o arquivo json como exemplo

```py
# Sempre vamos precisar de outro dataframe para fazer a leitura.

df_carros_json = spark.read \
                .format("json") \
                .option("header", True) \
                .load("/aprendizado/modelo_carro_json")
display(df_carros_json)

```
---

> SELECT no pyspark. Vamos ler a tabela e as opções.

```py
#Defina o DF (tome cuidado ao definir dataframes pois ele vai substituir algum já usado)

# Select basico 
df_carros = spark.read.format("csv").option("header", True).load("/aprendizado/modelo_carro")
display(
        df_carros.where("id_carro = '1'")     #Sempre com display para mostrar o resultado 
       )

```
---

> VAMOS USAR O Transaction SQL para fazer um exemplo

```py
# Criando uma tabela temporária que irá receber o SELECT do SQL.
df_carros.createOrReplaceTempView("carros")      # note que chamamos a tabela de carros

```
> Agora vamos rodar um script em SQL usando o databricks

```sql

%sql
--Percebi que para cada query precisa colocar o ;
--Então não faz muito sentido pois nao vai rodar tudo, vai rodar uma e mostrar so a ultima.
SELECT *
FROM CARROS 
WHERE id_carro='1';

SELECT DISTINCT * 
FROM CARROS; 

SELECT
     REPLACE(PRECO,'$','') AS PRECO
FROM CARROS;

```
> Se quisermos gravar o resultado em um dataframe para uso posterior podemos fazer o seguinte.

```py

df_carros_sql = spark.sql("""

SELECT
     REPLACE(PRECO,'$','') AS PRECO
FROM CARROS;

""")
display(df_carros_sql)

```
---
> Agora vamos fazer esses selects no pyspark com distinct e dropDuplicates (lembrando que os dois fazem a mesma coisa)

```py
# Vamos definir um datatrame que vai receber o resultado de outro dataframe.

df_carros_pyspark = df_carros.distinct()   # Lembrando que df_carros tem a tabela duplicada.
df_carros_pyspark = df_carros.dropDuplicates()   # Lembrando que df_carros tem a tabela duplicada.

print(df_carros.count())           # verificando quantos registros em em df_carros
print(df_carros_pyspark.count())   # verificando o resultado do dataframe com o distinct.

```
---

> Usando o replace do pyspark

```py

#Lembrando que a função regex_replace é nativa do sql, sendo assim temos que importar a biblioteca do sql

from pyspark.sql.function import regex_replace      #Poderiamos colocar o * que importaria tudo.

df_carros_spark_2 = df_carros     # definindo que o novo dataframe receberá os dados de df_carros
df_carros_spark_2 = df_carros_spark_2.withColumn("preco", regex_replace("preco","$",""))

```




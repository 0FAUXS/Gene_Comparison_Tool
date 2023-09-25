#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 24 19:57:42 2023

@author: Lumin
"""

# Databricks notebook source order:

#SparkML pipeline data to classify/cluster/etc. 

#Transform train fit test stream

#Testing set of data streamed to the Gene Comparison Tool program, 1 file per trigger

#ML pipeline model to transform your streaming data

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as f
from pyspark.sql.functions import regexp_extract, col, when, concat_ws, lit,collect_list, udf
import re
import os
import shutil
from difflib import SequenceMatcher

from pyspark.sql.functions import levenshtein, length, expr
from pyspark.ml import Pipeline
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import broadcast
from pyspark.sql.types import DoubleType



# COMMAND ----------

#load gene sequence file as destDir:

#dbutils.fs.rm("dbfs:/FileStore/sequenceFiles/parquetOUTdirectory", True)

#dbutils.fs.mkdirs("/FileStore/sequenceFiles/unkGenesFiles")
#dbutils.fs.mkdirs("/FileStore/sequenceFiles/knowngenes")


# COMMAND ----------

unkGeneDir = "/FileStore/sequenceFiles/unkGenesFiles"
knownGeneDir = "/FileStore/sequenceFiles/knowngenes"

# COMMAND ----------

def multiGenes(directory):
    fp = [fInfo.path for fInfo in dbutils.fs.ls(directory)]
    dfDict = {}
    for p in fp:
        content = spark.read.text(p)
        h = content.first()[0]
        data = content.filter(col("value") != h)
        eachDF = data.withColumnRenamed('value', 'sequence')
        name = os.path.basename(p)
        cleanName = os.path.splitext(name)[0]
        dfDict[cleanName] = eachDF

    return dfDict




# COMMAND ----------

#reference: unkGeneFiles = "/FileStore/sequenceFiles/genes"

def multiGenesToParquet(directory):
    fp = [fInfo.path for fInfo in dbutils.fs.ls(directory)]
    dfDict = {}
    for p in fp:
        content = spark.read.text(p)
        h = content.first()[0]
        data = content.filter(col("value") != h)
        eachDF = data.withColumnRenamed('value', 'sequence')
        name = os.path.basename(p)
        cleanName = os.path.splitext(name)[0]
        dfDict[cleanName] = eachDF

        parquetDir = os.path.join(parquetOUTdirectory, f"{cleanName}Parquet")


        os.makedirs(parquetDir, exist_ok = True)
        parquetPath = os.path.join(parquetDir, cleanName)
        eachDF.write.parquet(parquetPath)
        print(parquetDir)
        
    return parquetDir

# COMMAND ----------

#this works for multiple parquet files so that unknown files as "test" can partition and stream quickly: 
#unkGeneFiles = "/FileStore/sequenceFiles/genes"
#unkGeneFilesParquet = multiGenesToParquet(unkGeneDir, unkGeneFilesParquet)

# COMMAND ----------

#streaming GeneLevenshtein, using the idea of broadcast in the processBatch function for the two know gene sequences to prevent comparison shuffle:

def geneLevenshtein(xy, unknown):

    levTransformer = SQLTransformer(statement="SELECT *, levenshtein(sequence, 'sequence') AS distance, LENGTH(sequence) AS maxLen FROM __THIS__")
    spark.udf.register("similarityPercent", lambda distance, maxLen: distance / maxLen * 100)
    simTransformer = SQLTransformer(statement=f"SELECT *, similarityPercent(distance, maxLen) AS similarityPercent FROM __THIS__")
    pipeline = Pipeline(stages=[levTransformer,simTransformer])
    knownXY = multiGenes(xy) 
    schema = list(knownXY.values())[0].schema
    print(schema)
    #below will write parquet files for better distribution, making it possible to run with speed, even with just 5 unknow files now, it is slow
    unkGenes = multiGenesToParquet(unknown)
    closestTo = []
    #closestTO will be a list of tuples (known g ene, closest unknown gene, percent diff) 
    #train and test data are represented by knownG and unkG
    #fit knownG on knownG and get 100%, the streaming test data will deliver similarity for new "test" data
    
    def processBatch(unk, epoch):
         
        for g, knownG in knownXY.items():
            print(g, "g for each g")
            levModel = pipeline.fit(knownG)
            knownGTransformed = levModel.transform(knownG) 
            cloIdx = None
            cloDistPercent = float("inf")

            parFiles = os.listdir(unkGenes) 
            for fName in parFiles:
                print(fName, "using fName in parFiles")
                unkFileName = os.path.join(unkGenes, fName)
                unkG = spark.read.parquet(unkFileName)

                unkTransformed = levModel.transform(unkG)
                simPercent = unkTransformed.select("similarityPercent").collect()[0]["similarityPercent"]
                simPercent = float(similarityPercent)
                print(simPercent, "simPercent")

                #The Levenshtein calculation is a measure of dissimilarity since we are looking for the most similarity simPercent is recorded when it greater than the last dist.
                if simPercent > cloDistPercent:
                    cloIdx = fName 
                    print(cloIdx, "cloIdx")
                    cloDistPercent = simPercent
                    print(cloDistPercent, "cloDistPercent")
            closestTo.append((g, cloIdx, cloDistPercent))
        return closestTo    
    readParStream =  spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).format("parquet").load(unknown)
    query = readParStream.writeStream.foreachBatch(processBatch).start()
    query.awaitTermination()


# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/sequenceFiles/parquetOUTdirectory", True)
unkGeneFiles = "/FileStore/sequenceFiles/unkGenesFiles"
knownGeneDir = "/FileStore/sequenceFiles/knowngenes"

geneLevenshtein(knownGeneDir, unkGeneFiles)

# COMMAND ----------


#This is the model training section of this assignment. Self-train/self-test should = 100% 

def geneLevenshtein(xy, unknown):

    levTransformer = SQLTransformer(statement="SELECT *, levenshtein(sequence, 'sequence') AS distance, LENGTH(sequence) AS maxLen FROM __THIS__")
    spark.udf.register("similarityPercent", lambda distance, maxLen: distance / maxLen * 100)
    simTransformer = SQLTransformer(statement=f"SELECT *, similarityPercent(distance, maxLen) AS similarityPercent FROM __THIS__")
    pipeline = Pipeline(stages=[levTransformer,simTransformer])
    knownXY = multiGenes(xy) 
    #knownXY = broadcast(knownXY)
    #unkGenes = multiGenes(unknown)
    closestTo = []
    #closestTO will be a list of tuples (known g ene, closest unknown gene, percent diff)
    #train and test data are represented by knownG and unkG
    #fit knownG on knownG and get 100%, the streaming test data will deliver similarity for new "test" data
    #self-test should return 100% similarity 
    firstKnown = list(knownXY.values())[0]
    secKnown = list(knownXY.values())[1]
    firstModel = pipeline.fit(firstKnown)
    firstTransformed = firstModel.transform(firstKnown)
    print("first gene on self-test, should be 100%")
    firstTransformed.show()
    #second test tell us how far away the two base genes form early 2020 to late 2023 are away from each other:
    secondTransformed = firstModel.transform(secKnown)
    print("second known gene test to first known gene")
    secondTransformed.show()
    #for g, knownG in knownXY.items():
    #    print(g, "g")
    #    levModel = pipeline.fit(knownG)
    #    knownGTransformed = levModel.transform(knownG) 
    #    cloIdx = None
    #    cloDistPercent = float("inf")

    #    for u, unkG in unkGenes.items():
    #        unkTransformed = levModel.transform(unkG)
    #        simPercent = unkTransformed.select("similarityPercent").collect()[0]["similarityPercent"]
    #        simPercent = float(simPercent)
    #        print(simPercent, "simPercent")
    #        if simPercent < cloDistPercent:
    #            cloIdx = u 
    #            print(cloIdx, "cloIdx if simPercent < cloDistPercent)
    #            cloDistPercent = simPercent
    #            print(cloDistPercent, "cloDistPercent second encounter")
    #    closestTo.append((g, cloIdx, cloDistPercent))

    #return closestTo

        
firstANDsecond = geneLevenshtein(knownGeneDir, unkGeneFiles)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC def geneLevenshtein(xy, unknown):
# MAGIC
# MAGIC     levTransformer = SQLTransformer(statement="SELECT *, levenshtein(sequence, 'sequence') AS distance, LENGTH(sequence) AS maxLen FROM __THIS__")
# MAGIC     spark.udf.register("similarityPercent", lambda distance, maxLen: distance / maxLen * 100)
# MAGIC     simTransformer = SQLTransformer(statement=f"SELECT *, similarityPercent(distance, maxLen) AS similarityPercent FROM __THIS__")
# MAGIC     pipeline = Pipeline(stages=[levTransformer,simTransformer])
# MAGIC     knownXY = multiGenes(xy) 
# MAGIC     schema = list(knownXY.values())[0].schema
# MAGIC     print(schema)
# MAGIC     #below will write parquet files for better distribution, making it possible to run with speed, even with just 5 unknow files now, it is slow
# MAGIC     unkGenes = multiGenesToParquet(unknown)
# MAGIC     closestTo = []
# MAGIC     #closestTO will be a list of tuples (known g ene, closest unknown gene, percent diff) 
# MAGIC     #train and test data are represented by knownG and unkG
# MAGIC     #fit knownG on knownG and get 100%, the streaming test data will deliver similarity for new "test" data
# MAGIC     
# MAGIC     def processBatch(unk, epoch):
# MAGIC          
# MAGIC         for g, knownG in knownXY.items():
# MAGIC             print(g, "g for each g")
# MAGIC             levModel = pipeline.fit(knownG)
# MAGIC             knownGTransformed = levModel.transform(knownG) 
# MAGIC             cloIdx = None
# MAGIC             cloDistPercent = float("inf")
# MAGIC
# MAGIC             parFiles = os.listdir(unkGenes) 
# MAGIC             for fName in parFiles:
# MAGIC                 print(fName, "using fName in parFiles")
# MAGIC                 unkFileName = os.path.join(unkGenes, fName)
# MAGIC                 unkG = spark.read.parquet(unkFileName)
# MAGIC
# MAGIC                 unkTransformed = levModel.transform(unkG)
# MAGIC                 simPercent = unkTransformed.select("similarityPercent").collect()[0]["similarityPercent"]
# MAGIC                 simPercent = float(similarityPercent)
# MAGIC                 print(simPercent, "simPercent")
# MAGIC
# MAGIC                 #The Levenshtein calculation is a measure of dissimilarity since we are looking for the most similarity simPercent is recorded when it greater than the last dist.
# MAGIC                 if simPercent > cloDistPercent:
# MAGIC                     cloIdx = fName 
# MAGIC                     print(cloIdx, "cloIdx")
# MAGIC                     cloDistPercent = simPercent
# MAGIC                     print(cloDistPercent, "cloDistPercent")
# MAGIC             closestTo.append((g, cloIdx, cloDistPercent))
# MAGIC         return closestTo    
# MAGIC     readParStream =  spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).format("parquet").load(unknown)
# MAGIC     query = readParStream.writeStream.foreachBatch(processBatch).start()
# MAGIC     query.awaitTermination()
# MAGIC
# MAGIC     produced : StructType([StructField('sequence', StringType(), True)])
# MAGIC dbfs:/FileStore/sequenceFiles/parquetOUTdirectory/group_1Parquet
# MAGIC dbfs:/FileStore/sequenceFiles/parquetOUTdirectory/group_33Parquet
# MAGIC dbfs:/FileStore/sequenceFiles/parquetOUTdirectory/group_56Parquet
# MAGIC dbfs:/FileStore/sequenceFiles/parquetOUTdirectory/group_65Parquet
# MAGIC dbfs:/FileStore/sequenceFiles/parquetOUTdirectory/group_90Parquet
# MAGIC EarlyKnown_OM065354 g for each g
# MAGIC OY594635_1KnownGene g for each g
# MAGIC EarlyKnown_OM065354 g for each g
# MAGIC OY594635_1KnownGene g for each g
# MAGIC EarlyKnown_OM065354 g for each g
# MAGIC OY594635_1KnownGene g for each g
# MAGIC EarlyKnown_OM065354 g for each g
# MAGIC OY594635_1KnownGene g for each g
# MAGIC EarlyKnown_OM065354 g for each g
# MAGIC OY594635_1KnownGene g for each g

# COMMAND ----------

# MAGIC %md
# MAGIC #check type
# MAGIC
# MAGIC for key, value in a.items():
# MAGIC     print(f"Type for key '{key}': {type(value)}")
# MAGIC for key, value in b.items():
# MAGIC     print(f"Type for key '{key}': {type(value)}")

# COMMAND ----------

# MAGIC %md
# MAGIC geneLevenshtein(knownGeneDir, unkGeneFiles)
# MAGIC output from an earlier de-bugging geneLevenshtein
# MAGIC
# MAGIC OUT:
# MAGIC EarlyKnown_OM065354
# MAGIC DataFrame[sequence: string]
# MAGIC cloIdx established:  None
# MAGIC cloDist first:  inf
# MAGIC -99.0 simPercent
# MAGIC group_1 cloIdx second encounter
# MAGIC -99.0 cloDist second encounter
# MAGIC -99.0 simPercent
# MAGIC -99.0 simPercent
# MAGIC -99.0 simPercent
# MAGIC -99.0 simPercent
# MAGIC OY594635_1KnownGene
# MAGIC DataFrame[sequence: string]
# MAGIC cloIdx established:  None
# MAGIC cloDist first:  inf
# MAGIC -99.0 simPercent
# MAGIC group_1 cloIdx second encounter
# MAGIC -99.0 cloDist second encounter
# MAGIC -99.0 simPercent
# MAGIC -99.0 simPercent
# MAGIC -99.0 simPercent
# MAGIC -99.0 simPercent
# MAGIC Out[46]: [('EarlyKnown_OM065354', 'group_1', -99.0),
# MAGIC  ('OY594635_1KnownGene', 'group_1', -99.0)]

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ncbi_nlm_nihFromKnownGene = "dbfs:/FileStore/sequenceFiles/EarlyKnown_OM065354.txt"
# MAGIC
# MAGIC def knownGeneNCBI(file_path, patterns):
# MAGIC     content = spark.read.text(file_path)
# MAGIC     
# MAGIC     tokenizer = RegexTokenizer(pattern=patterns, inputCol="value", outputCol="translation")
# MAGIC     sectionsDF = tokenizer.transform(content)
# MAGIC     sectionsDF
# MAGIC    
# MAGIC     
# MAGIC     return sectionsDF
# MAGIC
# MAGIC patternsNCBI = r"\/translation=\"([\s\S]+?)\""
# MAGIC
# MAGIC knownGeneContent = knownGeneNCBI(ncbi_nlm_nihFromKnownGene, patternsNCBI)
# MAGIC
# MAGIC knownGeneContent.show(2,truncate=False)
# MAGIC

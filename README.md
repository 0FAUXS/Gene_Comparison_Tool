# Gene_Comparison_Tool
PySpark genome comparison tool


Purpose:


  After the corona virus pandemic there are many studies on DNA, or RNA sequences which await classification. This is a program which classifies string-based sequence files into similarity groups based on their dominant genomic patterning.
  
  Comparing unknown, or newly produced, gene sequences to known variants selected from a well cataloged source like the www.ncbi.nlm.nih.gov/nuccore repository, will allow the fast processing of new gena data, reducing the backlog of sequencing work on the virus. This becomes quite important when crucial information is need for public safety.
  
  This application is written in such a way that a gene sequencer machine can produce streaming files that are loaded into a DBFS directory, that directory can be passed in to the function call as well as the directory of genes that are known and the genes that are unknown and run the comparison returning the most similar known gene, which unknown gene it was compared to and the similarity in percent between the two.

  This program will indicate if the new samples are closer to the earlier COVID-19 strands, using sample collected on 2020-02-01 or a later sequence collected on 2023-02-15. This can say a lot about the new samples considering their location and sample date.
  
  The Levenshtein Distance is way to calculate the edit-distance by considering the single character edits that it would take to transform one string into the comparison string. The pyspark Levenshtein distance is calculated as: from string_a to string_b, an edit-matrix is populated iteratively from (i,j), where i is the character of string_a corresponding with the location of j in string_b, and the minimum number of insertions or deletions that are needed to change one string to another is recorded. The Percentage Difference between knownXY and unkGenes is: PD = (LevDist/MaxLength)*100.


Methods:


  This code is still a work in progress.
  I used a modified version of a user defined function called “geneLevenshtein” and passed in the first known gene data and a second known gene data. I did a self-test comparison from the first gene to the first gene, which yielded 100% similarity. I compared the early known gene to the later known gene and unfortunately also had 100% similarity returned. This indicates to me that the parameters may not be tuned to the sensitivity required for minute differences and do need to return fractional values for the similarityPercent. The parameter that can be tuned with the levenshtein function are insertion_cost, deletion_cost and substitution_cost. I need to consult a bioinformatics expert to better tune my results.

  For the “test” streaming section of this assignment, I simulated the file input as a machine would produce, naming the files in order of processing. I wrote into the comparison function a way to maintain that file name to directly associate it with the file name produced. Each dataframe created from a gene file takes on the name of that gene file. The return is the name of the dataframe used. This is to avoid as much human error as possible.


Results:


  Running geneLevenshtein(knownGeneDir, unkGeneFiles) without streaming did yield a tuple during the debugging stage, and printing out various stages of the function showed a great amount of potential in the process. The percentage calculation is off, or as stated before the parameters need to be adjusted for sensitivity, but I did get results:
  
    Out[46]: [('EarlyKnown_OM065354', 'group_1', -99.0), 
              ('OY594635_1KnownGene', 'group_1', -99.0)]
  This output is showing a -99 percent similarity. For this I used a similarityPercent calculation of lambda distance, maxLen: 1- distance / maxLen * 100). Since the Levenshtein calculation is a measurement of dissimilarity, I opted to not subtract the Levenshtein numbers from 1 and take the percent from the Levenshtein distance and maxLen. Still the results were disappointing after that, as the percents returned were always 100.
  The streaming/simulated streaming did run without crashing but did not produce the tuple desired. However, it did get to 5 sets of comparisons for each dataframe in knownXY; so I had 10 lines of each known gene (2) to each of the 5 unknowns by looking at the print out:
    EarlyKnown_OM065354 g for each g
    OY594635_1KnownGene g for each g... (x5)
  The parquet files kept the file naming consistent, and by feeding the stream simulation ("maxFilesPerTrigger", 1) the pyspark automatic partitioning is much needed to process this calculation faster than letting the system waste efficiency by one calculation at a time.
When gene directories are smaller, the multiGenes function can be called which does not write the dataframes to parquet files, but puts them in dictionaries with the dataframe as the value and the key is the naming scheme corresponding to the original gene file, like the parquet names.


Conclusion:


  This has the potential to be a very powerful tool. The workflow is streamlined (nearly correctly) and if the actual gene sequencer were to be pushing files to my repository, DataBricks and pyspark ML is surly able to efficiently handle the task. The program written for this assignment is easily adaptable from 5 unknown gene and 2 known comparisons to as many as is needed. The partitioning and processing just by a little automatic grouping of large directories for speed.

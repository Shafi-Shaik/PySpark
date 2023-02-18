# PySpark

I'll be uploading my PySpark learning in the form of jupyter notebooks.

Below Topics will be covered in this Repo

1) RDD - Resilient Distributed Datasets
   * Different Ways of Creating RDDs
     * on the fly 
     * External (Importing)
   * Transformations on RDDs
   * Actions on RDDs
 
2) DataFrames 
   * Different Ways of creating DataFrames
     * on the fly
     * from existing RDDs
     * External Files (Importing
  
  
  
<br> <br> <hr>
#### <center> Content in Notebooks:
    
 1. RDD.ipynb 
     * Create Spark Session
     * Creating RDDs Using
        - Parallelize ( )
        - textFile ( )
        - wholeTextFiles ( )
        - sc.emptyRDD ( )
     * Repartition and Coalesce
     * RDD Transformations
        - Map, flatMap ( )
        - reduceByKey
        - sortByKey
        - filter
     * RDD Actions
        - collect, take, takeOrdered, takeSample
        - count, countApprox,countApproxDistinct
        - max, min, first, top
        - aggregate
        - fold, reduce, treeReduce
  
    <br> <br>
  
 2. Transformations.ipynb 
    - Map, flatMap ( )
    - filter
    - MapPartitions, mapPartitionsWithIndex
    - sample
    - union, intersection, distinct 
    - groupByKey
    
      
    <br> <br>
  
3. Actions.ipynb
    - collect, take,takeSample,takeOrdered top
    - count, max, min, sum, mean, stdev, first, top
    - fold, reduce, treeReduce
    - countByKey, countByValue
    - variance, sampleVariance
    - stats
    - forEach
    - saveAsObjectFile, saveAsTextFile 

    <br> <br>   
  
 4. RDD Joins.ipynb
    - Iinner join
    - Right Outer Join
    - Left Outer Join
    - Full Outer Join
    - Cartesian Join (cross join)
    
   <br> <br>  
    
 5. DataFrames.ipynb
     * Create Empty DF with schema
     * Create Empty DF without schema
     * DF from RDD
         - using toDF ( )
         - createDataFrame (rdd,schema)
         - createDataFrame with StructType
     * DF on the fly (adhoc data)
     * DF from external Data source
    
    
     <br> <br> 
 6.  DF methods.ipynb 
     * show(), select(), collect()
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of ALS for learning how to use Spark. Please refer to
pyspark.ml.recommendation.ALS for more conventional use.

This example requires numpy (http://www.numpy.org/)
"""
from __future__ import print_function

import sys

import numpy as np
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession

LAMBDA = 0.01   # regularization
"np.random.seed(42)"


def rmse(tR, tms, tus,tnonEmptyLocation,tM,tU):
    
    diff = tR - tms * tus.T
    return np.sqrt(np.sum(np.power(diff, 2)) / (tM * tU))
    
    """

    
    temp=tms * tus.T
    temp=np.array(temp)
    tR=np.array(tR)
    diff=0

    for nonEmLoc in tnonEmptyLocation:
        
        aI=nonEmLoc[0]
        bI=nonEmLoc[1]
        diffTemp=tR[aI][bI]-temp[aI][bI]
        
        
        diff+=np.power(diffTemp,2)


    return np.sqrt((float(diff)/len(tnonEmptyLocation)))
    """

def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]

    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T

    for j in range(ff):
        XtX[j, j] += LAMBDA * uu

    return np.linalg.solve(XtX, Xty)


if __name__ == "__main__":

    """
    Usage: als [M] [U] [F] [iterations] [partitions]"
    """

    print("""WARN: This is a naive implementation of ALS and is given as an
      example. Please use pyspark.ml.recommendation.ALS for more
      conventional use.""", file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("PythonALS")\
        .getOrCreate()

    sc = spark.sparkContext


    "Should change!!!!!!!!!!!!!!!"
    fileName=sys.argv[1]

    M = int(sys.argv[2]) 
    U = int(sys.argv[3]) 
    F = int(sys.argv[4]) 
    ITERATIONS = int(sys.argv[5]) 
    partitions = int(sys.argv[6])
    
    outFileName = sys.argv[7]

    "outputFile=sys.argv[7]"
    
    print("Running ALS with M=%d, U=%d, F=%d, iters=%d, partitions=%d\n" %
          (M, U, F, ITERATIONS, partitions))

    

    RFile=sc.textFile(fileName)
    tagsheader = RFile.first()
    
    header=sc.parallelize([tagsheader])
    RFile = RFile.subtract(header)


    "R = matrix(rand(M, F)) * matrix(rand(U, F).T)"
    


    '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    R = matrix(np.zeros(shape=(M,F))) * matrix(np.zeros(shape=(U,F)).T)
    mirR=np.zeros(shape=(M,U))

    "R=np.array(R)"
    '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'

    

    
    rawR=[]
    onlyUid=[]
    onlyMovieID=[]

    RFile = RFile.subtract(header)
    
    csv=RFile.collect()

    for row in csv:
        eachR=row.split(',')
    
        eachR.pop(3)
        onlyUid.append(int(eachR[0]))
        onlyMovieID.append(int(eachR[1]))
        
        rawR.append(map(float,eachR))
    
    onlyUid.sort()
    onlyMovieID.sort()
    
    onlyUid=list(set(onlyUid))
    onlyUid.sort()

    onlyMovieID=list(set(onlyMovieID))
    onlyMovieID.sort()

    nonEmptyLocation=[]

    
    
    for line in rawR:

        uID=(int(line[0]))
        mID=(int(line[1]))
        
        indexOfUid=onlyUid.index(uID)
        indexOfMid=onlyMovieID.index(mID)
        
         
        if indexOfUid<=M-1 and indexOfMid<=U-1:
            if line[2]!=0:
                
                mirR[indexOfUid][indexOfMid]=line[2]
                    
                "print('Line[2]!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1')"
                "print(line[2])"
                tempTuple=[indexOfUid,indexOfMid]
                nonEmptyLocation.append(tempTuple)
        
    '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'
    R=matrix(mirR)
    '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!'

    #Initialize matrix with random number
    # ms = matrix(rand(M, F))
    # us = matrix(rand(U, F))


    #Initialize matrix with ones
    a=(M,F)
    b=(U,F)

    ms = matrix(np.ones(a))
    #ms = matrix(np.zeros(a))
    us = matrix(np.ones(b))
    #us = matrix(np.zeros(b))
    #######################

    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)



    outputFile=open(outFileName,'a')


    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), partitions) \
               .map(lambda x: update(x, usb.value, Rb.value)) \
               .collect()
        # collect() returns a list, so array ends up being
        # a 3-d array, we take the first 2 dims for the matrix
        ms = matrix(np.array(ms)[:, :, 0])
        msb = sc.broadcast(ms)

        us = sc.parallelize(range(U), partitions) \
               .map(lambda x: update(x, msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)


        """
        for row in ms:
            print(row)
    
        

        for row in us:
            print(row)

        """
    

        error = rmse(R, ms, us,nonEmptyLocation,M,U)
    
        ch='%.4f' % error   
        outputFile.write(ch+'\r\n')
    
        #print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
        #print("Iteration %d:" % i)
        #print("\nRMSE: %.4f\n" % error)

    outputFile.close()
    spark.stop()

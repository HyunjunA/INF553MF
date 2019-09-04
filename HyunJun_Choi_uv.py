'''
Created on Mar 4, 2018
 
@author: Hyun Jun Choi
 
INF 553 Task 1 UV decomposition In HW3
'''



import sys
import timeit

import csv
import numpy as np
from numpy.random import rand
from numpy import matrix

from multiprocessing import Pool


LAMBDA = 0.01   # regularization
# np.random.seed(42)

def rmse(tR,tNonEmptyLocation, tU, tV):
    UV=np.dot(tU , tV)
    diff = tR - UV
    diffValue=0
    
    
        
    diffValue=[np.power(diff[chLoca[0]][chLoca[1]], 2) for chLoca in tNonEmptyLocation]
                
    return np.sqrt(sum(diffValue)/len(tNonEmptyLocation))




def updateU2(fU,fV,fR,frowU,fcolU):
    denom=0
    sumOFexceptR=0
    nom=0
    
    if sum(fR)!=0.0:
        unZeroRlocation=[i for i,inR in enumerate(fR) if inR !=0]
        
        nom=0
        denom=0
         
        for unZeroRlocationIndex in unZeroRlocation:
            firtTerm=fU.dot(fV[:,unZeroRlocationIndex])-fU[fcolU]*fV[colU][unZeroRlocationIndex]
            secondTerm = fR[unZeroRlocationIndex]-(firtTerm)
            nom+=fV[fcolU,unZeroRlocationIndex]*secondTerm
            denom+=np.power(fV[fcolU,unZeroRlocationIndex],2)
    
        return nom/denom
    
    if sum(fR)==0.0:
        return float('inf')
 
def updateV2(sU,sV,sR,srowV,scolV):
    
    denom=0
    sumOFexceptR=0
    nom=0
    
    if sum(sR)!=0.0:
        unZeroRlocation=[i for i,inR in enumerate(sR) if inR !=0]
        
        nom=0
        denom=0
         
        for unZeroRlocationIndex in unZeroRlocation:
            firtTerm=sV.dot(sU[unZeroRlocationIndex,:])-sU[unZeroRlocationIndex][srowV]*sV[srowV]
            secondTerm = sR[unZeroRlocationIndex]-(firtTerm)
            nom+=sU[unZeroRlocationIndex,srowV]*secondTerm
            denom+=np.power(sU[unZeroRlocationIndex,srowV],2)
        return nom/denom
    if sum(sR)==0.0: 
        return float('inf')



    

if __name__ == "__main__":
    
    start = timeit.default_timer()
#     pool = Pool()
    
    fileName=sys.argv[1]
             
    MN = int(sys.argv[2])
    UN = int(sys.argv[3])
    F = int(sys.argv[4])
    ITERATIONS = int(sys.argv[5])



 
 


    rawR=[]
    onlyUid=[]
    onlyMovieID=[]
    
    csv=open(fileName,'r')
    csv.next()
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

    R=np.zeros(shape=(MN,UN))

    nonEmptyLocation=[]
    
    for line in rawR:

        uID=(int(line[0]))
        mID=(int(line[1]))
        
        indexOfUid=onlyUid.index(uID)
        indexOfMid=onlyMovieID.index(mID)
        
        if indexOfUid<=MN-1 and indexOfMid<=UN-1:
            if line[2]!=0:
                R[indexOfUid][indexOfMid]=line[2]
                tempTuple=[indexOfUid,indexOfMid]
                nonEmptyLocation.append(tempTuple)
            
    U=np.ones(shape=(MN, F))

    V=np.ones(shape=(F, UN))


    Utemp=[]



    
    "UV decomposition"
    for iterT in range(ITERATIONS):
        
        
        denom=0
        sumOFexceptS=0
        nomi=0
        
   
        "U"

        
        for rowU in range(U.shape[0]):
            for colU in range(U.shape[1]):
                

                U[rowU][colU]=updateU2(U[rowU,:],V,R[rowU,:],rowU,colU)
                

        
        
        sdenom=0
        ssumOFexceptR=0
        snomi=0

        
        "V"
        for colV in range(V.shape[1]):
            for rowV in range(V.shape[0]):
 

                V[rowV][colV]=updateV2(U,V[:,colV],R[:,colV],rowV,colV)

        error=rmse(R,nonEmptyLocation, U, V) 

        print("%.4f" % error)


    stop = timeit.default_timer()
    "print stop - start" 


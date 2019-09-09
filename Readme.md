# Matrix Factorization

I implemented Matrix Factorization for recommendation system.




## Reference
1 [Recommendation systems](http://infolab.stanford.edu/~ullman/mmds/ch9.pdf)

2 [Matrix Factorization Techniques for Recommender systems](https://datajobs.com/data-science-repo/Recommender-Systems-[Netflix].pdf)

3 [The BellKor solution to the Netflix grand prize](https://www.netflixprize.com/assets/GrandPrize2009_BPC_BellKor.pdf
)




## How to run
Python version: 2.7

* How to run the code named, HyunJun_Choi_uv.py
1) python HyunJun_Choi_uv.py ratings_task1.csv 100 4382 40 10

Execution format: python HyunJun_Choi_uv.py ratings_task1.csv n m f k

n is the number of rows (users) of the matrix, while m is the number of columns (movies).
f is the number of dimensions/factorsin the factor model. That is, U is n-by-f matrix, while V is f-by-m matrix.
k is the number of iterations.





Spark version: 2.2.0

* How to run the code named, HyunJun_Choi_als.py
1) bin/spark-submit HyunJun_Choi_als.py ratings_task2.csv 671 9066 60 10 2 out_task2.txt

Execution format: bin/spark-submit HyunJun_Choi_als.py ratings_task2.csv n m f k out_task2.txt

n is the number of rows (users) of the matrix, while m is the number of columns (movies).
f is the number of dimensions/factorsin the factor model. That is, U is n-by-f matrix, while V is f-by-m matrix.
k is the number of iterations.
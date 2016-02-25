**Author:**
This hadoop package has being written by Nikzad Babaii Rizvandi when was working as research engineer at National ICT Australia (currently honorary research associate at Centre for Distributed and High Performance Computing at University of Sydney). <a href='Hidden comment: employee of National ICT Australia, since Oct.2012.
It is no longer actively developed or supported.'></a>

**Licence:**
This package and all source code has been released under the Apache License 2.0.

**Dependencies:**
The Java implementation of this package depends on:
  * JDK/JRE 1.6
  * Hadoop 2.0.1


**Objectives:**
The package focuses on hadoop implementation of some of machine learning algorithms (in Java and Python). <a href='Hidden comment: Note that this package was tested on Hadoop pseudo-distributed platform and a 6-node Amazon elastic MapReduce cluster.'></a> Some of the algorithms are:

<a href='Hidden comment: 
==== ============== _Statistics/Linear Algebra_ =============== ====
* Covariance matrix (cov(X,Y)) [_under develop in Python/Java_]
* P-value
* t-statistics
* correlation
* confidence intervals/hypothesis testing
* Expectation Maximization (EM) (under implementation in Java)
'></a>

#### ============== _Statistical models_ =============== ####
  * Markov model [_under develop in Python/Java_]

#### ===================== _Regression_ ==================== ####
  * Multiple linear regression by gradient descent [_in Java_]
  * Multiple linear regression-closed form [_under develop in Python/Java_]

<a href='Hidden comment: 
* Bayesian linear regression
* Gaussian Process regression
'></a>
#### ===================== _Classification_ =================== ####
<a href='Hidden comment: 
* Logistic regression [_under develop in Java_]
'></a>
  * Naive Bayes Classifier for continuous input [_in Java_]
<a href='Hidden comment: 
* Support vector machine (SVM)
* Decision Tree
'></a>

<a href='Hidden comment: 
==== =============== _Bayesian network_ ================= ====
* Bayesian Network Parameter Learning using EM
* Markov random filed
'></a>
#### =========== _Clustering & Dimension Reduction_ ============= ####
  * K-mean clustering [_in Java_]
<a href='Hidden comment: 
* CURE Algorithm
* Principle component analysis (PCA)

==== ================= _Applications_ =================== ====
* Text processing (Spam detection with Naive Bayes Classifier)
* Anomaly detection in Network
'></a>
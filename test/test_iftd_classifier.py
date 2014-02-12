#!/usr/bin/python

# ensure that the classifier works like the original

import sys
import random

sys.path.append("../")

from classifiers.naivebayes import NaiveBayesClassifier
from iftstats import iftNaiveBayesClassifier


def print_prob( distrib ):
   out = ""
   for sample in distrib.samples():
      out += sample + ": " + str(distrib.prob(sample)) + ", "

   print out

features = []
for i in xrange(0, 100):
   features.append([ {"foo": random.choice([1,1,1,2]), "bar": random.choice([2,2,3,3]), "baz": random.choice([1,2,3,4]), "goo": random.choice([3,4,3,4])}, random.choice(["mars", "jupiter", "saturn", "uranus"]) ])


classifier_1 = NaiveBayesClassifier.train( features[:50] )
classifier_2 = NaiveBayesClassifier.train( features )
classifier_3 = NaiveBayesClassifier.train( features )

iftclassifier_1 = iftNaiveBayesClassifier()
iftclassifier_2 = iftNaiveBayesClassifier()
iftclassifier_3 = iftNaiveBayesClassifier()

iftclassifier_1.retrain( features[:50] )
iftclassifier_2.retrain( features )

iftclassifier_3.retrain( features[:50] )
iftclassifier_3.retrain( features[50:] )

test = {"foo": 2, "bar": 2, "baz": 3, "goo": 4}

print_prob( classifier_1.prob_classify( test ) )
print_prob( iftclassifier_1.prob_classify( test ) )

print ""

print_prob( classifier_2.prob_classify( test ) )
#print_prob( classifier_3.prob_classify( test ) )

print_prob( iftclassifier_2.prob_classify( test ) )
print_prob( iftclassifier_3.prob_classify( test ) )


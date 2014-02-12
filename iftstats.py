#!/usr/bin/env python

"""
iftstats.py
Copyright (c) 2009 Jude Nelson

This package defines statistics collection and analysis
methods for choosing file protocols.  The idea is that,
given a feature set (a.k.a. feature vector) containing
information relevant to the nature of the file transmission,
this module should identify protocols that are most likely
to succeed in transmitting the file.  The system will learn
over time what works and what doesn't.
"""

import sys
import os

import classifiers.naivebayes
import classifiers.decisiontree

from classifiers.compat import defaultdict
from classifiers.probability import *

from classifiers.api import *

import time
import threading
import iftlog
import iftfile

# log stats?
IFTSTATS_DO_STATS       = "IFTSTATS_DO_STATS"

# transfer start time
IFTSTATS_BEGIN_TIME     = "IFTSTATS_BEGIN_TIME"

# transfer end time
IFTSTATS_END_TIME       = "IFTSTATS_END_TIME"

# transfer status
IFTSTATS_STATUS         = "IFTSTATS_STATUS"

IFTSTATS_PROTO_SENDER   = "IFTSTATS_PROTO_SENDER"
IFTSTATS_PROTO_RECEIVER = "IFTSTATS_PROTO_RECEIVER"

# list of (protocol, status, start, end, size) tuples for each chunk sent
IFTSTATS_PROTO_LIST     = "IFTSTATS_PROTO_LIST"
IFTSTATS_PROTO_LIST_LOCK= "IFTSTATS_PROTO_LIST_LOCK"
PROTO_LIST_NAME   = 0      # protocol name
PROTO_LIST_STATUS = 1      # success or failure
PROTO_LIST_START  = 2      # start time
PROTO_LIST_END    = 3      # end time
PROTO_LIST_SIZE   = 4      # amount of data sent

# size estimatation step
FSIZE_STEP = 65536

# how many bins for classifying file size?
FSIZE_BINS = 8

# file classification tree, where the children of the ith node is at 2i and 2i+1.  Indexing starts at 1.
FSIZE_TREE = []

# feature set fields
FSET_FILETYPE           = "FSET_FILETYPE"
FSET_FILESIZE           = "FSET_FILESIZE"    # not the exact size, but an integer in [0, FSIZE_BINS)
FSET_STATUS             = "FSET_STATUS"      # T/F
FSET_DAY_OCTANT         = "FSET_DAY_OCTANT"  # 0 for 0:01-3:00, 1 for 3:01-6:00, 2 for 6:01-9:00, 3 for 9:01-12:00, 4 for 12:01-15:00, etc.

# how often (number of transfers) do we retrain the classifier?
RETRAIN_FREQ = 1

# the classifier itself
CLASSIFIER = None
CLASSIFIER_TYPE = None

# how many best protos to classify?
NUM_BEST_PROTOS = 1

# features that we buffer up until it is time to retrain
PENDING_FEATURES = []

# global information about this iftd instance to report to owld
OWLD_LOCK = threading.BoundedSemaphore(1)
TIME_INIT = 0     # when we started up
PROTO_TRANSMIT = {}   # amount of data sent by each protocol (in bytes)
PROTO_TIMES = {}      # amount of time spent on each protocol (in seconds)
PROTO_ATTEMPTS = {}   # amount of attempts for each protocol (per chunk)
PROTO_SUCCESS = {}    # amount of successes for each protocol (per chunk)
PROTOS = None         # names of known protocols



class iftNaiveBayesClassifier( classifiers.naivebayes.NaiveBayesClassifier ):
   """
   This is an improvement on the NLTK NaiveBayes classifier that allows
   for continuous training by adding additional feature vectors and
   labels to recalculate the feature frequency distributions and feature values
   over subsequent calls to the new refine() method, as opposed to
   creating a whole new classifier each time we have a new feature.
   """
   
   def __init__(self, label_probdist=None, feature_probdist={}):
      
      self._label_probdist = label_probdist
      self._feature_probdist = feature_probdist
      
      if label_probdist != None:
         self._labels = label_probdist.samples()
      else:
         self._labels = None
      
      # keep track of label frequency distribution, feature names, feature frequency distribution, and feature values
      # as they get updated every time retrain() is called.
      self.label_freqdist = FreqDist()
      self.feature_freqdist = defaultdict(FreqDist)
      self.feature_values = defaultdict(set)
      self.fnames = set()
      self.untrained = True
   
   @staticmethod
   def train( labeled_featuresets ):
      nbc = classifiers.naivebayes.NaiveBayesClassifier.train( labeled_featuresets )
      return iftNaiveBayesClassifier( nbc._label_probdist, nbc._feature_probdist )
   
   def refine(self, labeled_featuresets, estimator=ELEProbDist):
      """
      Modified from train() method, this method will take the given featuresets and re-calculate
      the classifier's known labels, label probability distribution, and feature probability distribution
      based on data given to it upon initialization, data given to it from previous calls to retrain(), and
      data given to it in this call.
      
      @param label_featurests
          A list of featuresets, where each featureset is a tuple with (feature set, label)
      """
      
      #self.label_freqdist = FreqDist()
      #self.feature_freqdist = defaultdict(FreqDist)
      #self.feature_values = defaultdict(set)
      #self.fnames = set()
      
      # Count up how many times each feature value occured, given
      # the label and featurename.
      for featureset, label in labeled_featuresets:
          self.label_freqdist.inc(label)
          for fname, fval in featureset.items():
              # Increment freq(fval|label, fname)
              self.feature_freqdist[label, fname].inc(fval)
              # Record that fname can take the value fval.
              self.feature_values[fname].add(fval)
              # Keep a list of all feature names.
              self.fnames.add(fname)

      # If a feature didn't have a value given for an instance, then
      # we assume that it gets the implicit value 'None.'  This loop
      # counts up the number of 'missing' feature values for each
      # (label,fname) pair, and increments the count of the fval
      # 'None' by that amount.
      for label in self.label_freqdist:
         num_samples = self.label_freqdist[label]
         for fname in self.fnames:
            count = self.feature_freqdist[label, fname].N()
            self.feature_freqdist[label, fname].inc(None, num_samples-count)
            self.feature_values[fname].add(None)
            
         
      
      # Create the P(label) distribution
      label_probdist = estimator(self.label_freqdist)

      # Create the P(fval|label, fname) distribution
      feature_probdist = {}
      for ((label, fname), freqdist) in self.feature_freqdist.items():
          probdist = estimator(freqdist, bins=len(self.feature_values[fname]))
          feature_probdist[label,fname] = probdist


      self._label_probdist = label_probdist
      self._feature_probdist = feature_probdist
      # store label names
      self._labels = self._label_probdist.samples()
      
   


def startup(proto_list, retrain_freq, classifier_type, num_best_protos):
   """
   Start up stats
   """
   global TIME_INIT
   global PROTO_TRAMSMIT
   global PROTO_TIMES
   global PROTO_ATTEMPTS
   global PROTO_SUCCESS
   global PROTOS
   global CLASSIFIER
   global RETRAIN_FREQ
   global FSIZE_AVGS
   global CLASSIFIER_TYPE
   global NUM_BEST_PROTOS
   
   TIME_INIT = time.time()
   for proto_name in proto_list:
      PROTO_TRANSMIT[proto_name] = 0
      PROTO_TIMES[proto_name] = 0
      PROTO_SUCCESS[proto_name] = 0
      PROTO_ATTEMPTS[proto_name] = 0
   
   proto_list.sort()
   PROTOS = proto_list
   RETRAIN_FREQ=retrain_freq
   
   FSIZE_TREE = [(0,0)] * (FSIZE_BINS+1)
   
   CLASSIFIER_TYPE = classifier_type
   NUM_BEST_PROTOS = num_best_protos


def restore_data(data_filename):
   """
   Restore the saved classification data
   
   @param data_filename:
      Name of the filename from which to restore
   """
   
   
   return None



def save_data(data_filename, data):
   """
   Save classification data
   
   @param data_filename:
     Name of the file to save the data to
     
   @param data:
     The actual data itself
   """
   
   return None




def begin_transfer( job, sender=False, receiver=False ):
   """
   Log the beginning of a transfer.
   
   @param job:
      iftjob instance that describes the transfer
   """
   
   job.set_stat( IFTSTATS_BEGIN_TIME, time.time() )
   job.set_stat( IFTSTATS_PROTO_LIST, [] )
   job.set_stat( IFTSTATS_PROTO_LIST_LOCK, threading.BoundedSemaphore(1))  # need to be thread-safe for appending data
   job.set_stat( IFTSTATS_PROTO_SENDER, sender )
   job.set_stat( IFTSTATS_PROTO_RECEIVER, receiver )
   job.set_stat( IFTSTATS_DO_STATS, True )
   return 0



def log_chunk( job, proto, status, start, end, size ):
   """
   Log the transmission (sending or receiving) of a chunk
   
   @param job
      iftjob instance that describes the transfer
   
   @param proto
      name of the protocol used to make the transfer
      
   @param status
      True if the protocol sent the chunk, false if not
   
   @param start
      Start time
   
   @param stop
      Stop time
   
   @param size
      Amount of data sent
   """
   
   
   if job.get_stat( IFTSTATS_DO_STATS ) == True:
      job.get_stat( IFTSTATS_PROTO_LIST_LOCK ).acquire(True)
      job.get_stat( IFTSTATS_PROTO_LIST ).append( (proto, status, start, end, size) )
      job.get_stat( IFTSTATS_PROTO_LIST_LOCK ).release()
   
   OWLD_LOCK.acquire(True)
   
   if PROTO_TRANSMIT.has_key( proto ):
      PROTO_TRANSMIT[ proto ] += size
   else:
      PROTO_TRANSMIT[ proto ] = size
   
   if PROTO_TIMES.has_key( proto ):
      PROTO_TIMES[ proto ] += end - start
   else:
      PROTO_TIMES[ proto ] = end - start
   
   if PROTO_ATTEMPTS.has_key( proto ):
      PROTO_ATTEMPTS[ proto ] += 1
   else:
      PROTO_ATTEMPTS[ proto ] = 1
      
   if status:
      if PROTO_SUCCESS.has_key( proto ):
         PROTO_SUCCESS[ proto ] += 1
      else:
         PROTO_SUCCESS[ proto ] = 1;
      
   OWLD_LOCK.release()
   
   return 0



def proto_performance( job, proto ):
   """
   Calculate the data sent and time taken sending it
   """
   stats = job.get_stat( IFTSTATS_PROTO_LIST )
   if not stats:
      return (None, None)
   
   stat_len = len(stats)
   data_cnt = 0
   time_cnt = 0.0
   for i in xrange(0, stat_len):
      if proto != stats[i][0]:
         continue    # wrong proto
      
      time_cnt += stats[i][3] - stats[i][2]     # end time - start time
      if stats[i][1]:      # successful transmit?
         data_cnt += stats[i][4]    # transmission len
   
   if time_cnt == 0.0:  # no usage?
      return (None, None)
   
   return (data_cnt, time_cnt);




def extract_features( job_attrs, file_success = True ):
   """
   Extract a feature vector from a job.
   This may be done before the transfer has started by the receiver
   to calculate protocol applicability.
   """
   fset = {}
   fset[FSET_FILESIZE] = fset_filesize( job_attrs )
   
   if job_attrs.get( iftfile.JOB_ATTR_FILE_TYPE ) == None:
      fset[FSET_FILETYPE] = fset_filetype( job_attrs )
   else:
      fset[FSET_FILETYPE] = job_attrs.get( iftfile.JOB_ATTR_FILE_TYPE )
   
   fset[FSET_STATUS] = file_success      # at first, assume that the transfer will be successful, since we favor successful protocols
   fset[FSET_DAY_OCTANT] = time.localtime()[3] / 3    # the hour field is the 3rd field
   
   return fset



def best_protocol( feature_vector ):
   """
   Given the feature vector, calculate the protocol that is most likely to map to it.
   """
   if CLASSIFIER:
      return CLASSIFIER.classify( feature_vector )[0]
   else:
      return None



def end_transfer( job, file_success=True):
   """
   Log the end of a transfer, extracting useful information from the job to create a feature vector.
   
   @param job:
     iftjob instance that describes the transfer
   
   @param file_success
      True if the file was sent successfully; false if not
   
   @param sender
      True if the file was sent
   
   @param receiver
      True if the file was received
   """
   global PENDING_FEATURES
      
   if job.get_stat( IFTSTATS_DO_STATS ) == False:
      # can't do anything
      iftlog.log(1, "Will not do statistics processing, since it is disabled")
      return 0

   job.set_stat( IFTSTATS_END_TIME, time.time() )
   job.set_stat( IFTSTATS_STATUS, file_success )
   
   # if we never retrain the classifier, don't bother with these calculations
   if RETRAIN_FREQ <= 0:
      return 0
   
   fset = extract_features( job.attrs, file_success )
   
   # identify the fastest and most reliable protocols
   
   proto_data = job.get_stat( IFTSTATS_PROTO_LIST )
   proto_data_time = {}    # map protocols to (total data sent, total time spent)
   proto_successes = {}    # map protocols to success counts
   for (proto, success, start, end, size) in proto_data:
      if proto_successes.has_key( proto ):
         proto_successes[proto][1] += 1      # one transmission
      else:
         proto_successes[proto] = [0, 1]       # one transmission
      
      if not success:
         continue    # don't bother if the chunk was a failure
      
      # count successes
      proto_successes[proto][0] += 1
      
      # count up data sent and time taken
      if proto_data_time.has_key( proto ):
         proto_data_time[proto][0] += size
         proto_data_time[proto][1] += end - start
      else:
         proto_data_time[proto] = [size, end - start]
   
   # now find the bandwidths
   proto_bandwidths = {}
   for (proto, data) in proto_data_time.items():
      size = float(data[0])
      t = data[1]
      if t == 0:
         t = 0.0000000001    # should never happen, but here nevertheless
         
      proto_bandwidths[proto] = size / t
   
   # rank protocols by speed
   fastest_protos = [(bandwidth, proto) for (proto, bandwidth) in proto_bandwidths.items()]
   fastest_protos.sort()
   fastest_protos.reverse()

   iftlog.log(3, "iftstats: Begin Transfer Analysis")  
   for (bandwidth, proto) in fastest_protos:
      iftlog.log(3, "iftstats:     bandwidth of " + proto + " is " + str(bandwidth))
   iftlog.log(3, "iftstats: End Transfer Analysis")
 
   """
   # find the most successful proto
   successful_protos = []
   for (proto, data) in proto_successes.items():
      success_count = data[0]
      xmit_count = data[1]
      if xmit_count == 0:
         continue    # something's really wrong
      
      r = float(success_count) / float(xmit_count)
      succssful_protos.append( (r, proto) )
   
   successful_protos.sort()
   successful_protos.reverse()
   """
   
   # get the file type
   file_type = filetype(job.get_attr( iftfile.JOB_ATTR_DEST_NAME ))
   if file_type == "ERROR":
      return E_NO_DATA     # cannot classify if we can't get the file type
   
   if len(fastest_protos) < NUM_BEST_PROTOS:
      fastest_protos += [(0,"unknown")] * (NUM_BEST_PROTOS + 1 - len(fastest_protos))
   
   # map the feature vector to the fastest protocol
   # XXX: make it an option to use most reliable?
   feature = [dat[1] for dat in fastest_protos[ 0 : NUM_BEST_PROTOS ]]
   
   PENDING_FEATURES.append( [fset, tuple(feature)] )
   
   # do we need to reclassify?
   if len(PENDING_FEATURES) >= RETRAIN_FREQ:
      return refine_classifier()
   
   return 0



def get_proto_rankings( job_attrs, success=True ):
   """
   If the classifier is a Naive Bayes classifier, then get back the probabilities of each protocol's success
   given a dictionary of job attributes.
   
   @arg job_attrs
      Job attribute dictionary to feed into the classifier
   
   @arg success
      Set to True to measure the probabilities of successful transmission.
      Set to False to measure the probabilities of failed transmissions.
      
   @return
      A list of protocol rankings as tuples.
   """
   if not CLASSIFIER:
      print "no classifier available :("
      return []

   if CLASSIFIER_TYPE == "NaiveBayes":
      ret = []
      features = extract_features( job_attrs, success )
      distrib = CLASSIFIER.prob_classify( features )
      for sample in distrib.samples():
         ret.append( (sample, distrib.prob(sample)) )

      return ret

   else:
      iftlog.log(1, "get_proto_rankings: cannot yet handle " + str(CLASSIFIER_NAME))
      return []



def clear_classifier():
   """
   Have IFTD forget all of its previous knowledge of file transfers.
   """
   global CLASSIFIER
   del CLASSIFIER
   CLASSIFIER = None



def refine_classifier():
   """
   Train the classifier using the data collected
   """
   global PENDING_FEATURES
   global CLASSIFIER
   if RETRAIN_FREQ <= 0:
      return 0    # don't bother if the classifier will never exist anyway
  
   iftlog.log(1, "Retraining classifier...")

   if CLASSIFIER == None:
      if CLASSIFIER_TYPE == "NaiveBayes":
         # haven't begun training yet, so make it happen
         CLASSIFIER = iftNaiveBayesClassifier.train( PENDING_FEATURES )
      elif CLASSIFIER_TYPE == "DecisionTree":
         CLASSIFIER = DecisionTreeClassifier.train( PENDING_FEATURES )
   
   else:
      if CLASSIFIER_TYPE == "NaiveBayes":
         CLASSIFIER.refine( PENDING_FEATURES )
      elif CLASSIFIER_TYPE == "DecisionTree":
         CLASSIFIER.refine( PENDING_FEATURES, 0.05, 100, 10 )   # use default train() values
 
   PENDING_FEATURES = []
 
   return 0



def get_owl_data():
   """
   Get information needed by owld (raven-specific)
   """
   ret = "[iftd]\n"
   
   dd, hh, mm, ss = __seconds_to_uptime( time.time() - TIME_INIT )
   ret += "uptime = " + str(dd) + ":" + str(hh) + ":" + str(mm) + ":" + str(ss) + "\n"
   
   for proto in PROTOS:
      ret += proto + " = (Xmit=" + str(PROTO_TRANSMIT[proto]) + ", Time=" + str(PROTO_TIMES[proto]) + ", Try=" + str(PROTO_ATTEMPTS[proto]) + ", Succ=" + str(PROTO_SUCCESS[proto]) + ")\n"
   
   return ret


def __seconds_to_uptime( t ):
   """
   Convert seconds to dd:hh:mm:ss
   """
   dd = int(t / 3600 * 24)
   t -= dd * 3600 * 24
   
   hh = int(t / 3600)
   t -= hh * 3600
   
   mm = int(t / 60)
   t -= 60 * mm
   
   ss = int(t)
   
   return (dd, hh, mm, ss)


def fset_filesize( job_attrs ):
   """
   Identify the size of the file from the transfer job
   
   @param job_attrs:
      dictionary of job attributes
   """
   
   if job_attrs.get( iftfile.JOB_ATTR_FILE_SIZE ):
      return __filesize_estimate( job_attrs.get( iftfile.JOB_ATTR_FILE_SIZE ) )
   
   # don't have given file size...
   
   # calculate size from proto list.
   # need to do this since there could be variable-length chunks.
   proto_list = job_attrs.get( IFTSTATS_PROTO_LIST )
   if proto_list == None:
      return -1

   size = 0
   for elem in proto_list:
      if elem[PROTO_LIST_STATUS]:
         size += elem[PROTO_LIST_SIZE]
      
   return __filesize_estimate( size )
   
   


def __filesize_estimate( size ):
   """
   Convert the exact file size into an estimation, so as to have
   a small number of different file sizes while maintaining a meaningful
   filesize feature.
   
   @param size
      The exact filesize
   """
   
   return (size / FSIZE_STEP) * FSIZE_STEP


def filetype( path ):
   """
   Deduce what type of file was sent.
   Executes "file -b --mime-type" on the file to get the type.
   
   @param job_attrs
      dictionary of job attributes
   """
   if path == None:
      return None
   
   pfd = os.popen("file -b --mime-type " + path)
   output = pfd.readlines()
   rc = pfd.close()
   
   if rc != None:
      return None
   
   # if "regular file, no read permission" then just return "unknown"
   if output[0].find("regular file") != -1:
      return "unknown"
   
   return output[0].strip('\n')


def fset_filetype( job_attrs ):
   """
   Given a job attribute dictionary, get the file MIME type
   """
   t = filetype( job_attrs.get( iftfile.JOB_ATTR_SRC_NAME ) )
   if t == None:
      return "ERROR"
   return t

   

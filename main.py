# Student Name: Volkan Öztürk
# Student Number: 2019400033
# Compile Status: Compiling
# Program Status: Working
# Notes: Probability values are rounded to 5 decimal places with '%.5f'.

import sys, getopt
from mpi4py import MPI

comm = MPI.COMM_WORLD
world_size = comm.Get_size()
rank = comm.Get_rank()


def main(argv):  # Main function that processor 0 executes.
   input_file = ''  # Name of the input file.
   merge_method = ''  # Type of the merge method.
   test_file = ''  # Name of the test file.
   opts, args = getopt.getopt(argv,"",["input_file=","merge_method=","test_file="])  # To differentiate argument flags.
   for opt, arg in opts:
      if opt == "--input_file":
         input_file = arg
      elif opt == "--merge_method":
         merge_method = arg
      elif opt == "--test_file":
         test_file = arg  
   method = merge_method
   for q in range(1, world_size):
      comm.send(method, dest=q)  # Processor 0 sends the method type to worker processors.
   
   input_open = open(input_file, 'r', encoding="utf-8")  # Opens the input file with utf-8 encoding to avoid errors of using characters that is not in the English alphabet.
   distribution_dict = dict()  # Dictionary that keeps the lines to be distributed. Key is the rank (or id) of the processor, value is the list of lines that is assigned to the regarding processor.
   for v in range(1, world_size):
      distribution_dict[v] = list()  # Initialize the lists of all processors.
   counter = 0  # Counts the number of lines, facilitates the equal distribution with the modulo operator.
   for line in input_open:  # For loop equally distributes lines to the worker processors.
      l = distribution_dict[(counter % (world_size - 1)) + 1]
      l.append(line)
      distribution_dict[(counter % (world_size - 1)) + 1] = l
      counter += 1
   for i in range(1, world_size):  # For loop that sends the data to the worker processors. Data is a list consisting of the lines to be examined.
      comm.send(distribution_dict[i], dest=i)
   
   data_dict = dict()  # Dictionary for keeping the frequency of unigrams and bigrams.
   if method == 'MASTER':  # If the method is MASTER, then processor 0 seperately receives the data from all other worker processors and adds the frequencies to main dictionary, named data_dict.
      for i in range(1, world_size):  # For loop to add seperate data that are sent by the workers to the processor 0.
         data = comm.recv(source=i)
         for unit in data.keys():  # For loop to add the given frequencies from the data that is sent by the regarding worker, to the main dictionary. Finally we have the dictionary that has the cumulative frequencies.
            data_dict[unit] = data_dict.get(unit, 0) + data[unit]
   else:  # If the method is WORKERS, then processor 0 only receives data from processor world_size - 1, and the contents are loaded to the main dictionary, named data_dict.
      data = comm.recv(source=(world_size-1))
      for unit in data.keys():
            data_dict[unit] = data_dict.get(unit, 0) + data[unit]
   
   input_test_open = open(test_file, 'r', encoding="utf-8")  # Opens the test file with utf-8 encoding to avoid errors of using characters that is not in the English alphabet.
   biglist = list()  # List that keeps the bigrams from the test file.
   for line in input_test_open:  # For loop that adds the bigrams to the biglist.
      biglist.append(line.strip())
   calculate_bigram_probability(biglist, data_dict)  # Processor finally calls this function to calculate the probabilities of the given bigrams.

def calculate_freq(dist_list):  # Function to calculate the frequencies of the unigrams and bigrams from all the lines that the regarding processor is assigned to, by processor 0.
   return_dict = dict()  # Dictionary that keeps the frequencies of the unigrams and bigrams. Key is the unigram/bigram, value is the count.
   for line in dist_list:  # For loop that iterates through the lines that the regarding processor is assigned to.
      token_list = line.split()  # Splits all the tokens of the line.
      for k in range(len(token_list) - 1):  # For loop to iterate the tokens. Note that final token is seperately handled to avoid 'index out of bound error' that would happen if we try to reach the index len(token_list).
         bigram = token_list[k] + ' ' + token_list[k + 1]  # Creates the bigram with the given index.
         return_dict[token_list[k]] = return_dict.get(token_list[k], 0) + 1  # Increments 1, the count of the regarding unigram.
         return_dict[bigram] = return_dict.get(bigram, 0) + 1 # Increments 1, the count of the regarding bigram.
      return_dict[token_list[-1]] = return_dict.get(token_list[-1], 0)  # Increments 1, the count of the final token.
   return return_dict

def method_master():  # Function to use when the method is MASTER.
   data = comm.recv(source=0)  # All workers receives data from the processor 0.
   print("Worker rank is:", rank, "Number of sentences:", len(data))  # Print requirement in Requirement 2.
   data_dict = calculate_freq(data)  # All workers calls this function to calculate the frequencies of bigrams and the unigrams from the data that is assigned by processor 0.
   comm.send(data_dict, dest=0)  # All workers send their respective data to the processor 0, with the dictionary type. Key is the unigram/bigram, value is the frequency / count.
   return

def method_workers(caller_rank):  # Function to use when the method is WORKERS.
   data = comm.recv(source=0)   # All workers receives data from the processor 0.
   print("Worker rank is:", rank, "Number of sentences:", len(data))  # Print requirement in Requirement 2.
   data_dict = calculate_freq(data)  # All workers calls this function to calculate the frequencies of bigrams and the unigrams from the data that is assigned by processor 0.
   if caller_rank == 1:  # Worker that has the rank 1 receives no data since it is the first worker, So it just sends its respective data to the next processor, namely 2.
      comm.send(data_dict, dest=(caller_rank + 1))
   elif caller_rank != (world_size - 1):  # Workers that have the rank (1 < rank < world_size - 1) receives the data from their preceding processor in the form of the dictionary.
      data_dict2 = comm.recv(source=(caller_rank -1))  # Key is the unigram/bigram, value is the count.
      for key in data_dict2.keys():  # Workers adds the data from its preceding worker to its own data in order to have the data cumulatively.
         data_dict[key] = data_dict.get(key, 0) + data_dict2[key]
      comm.send(data_dict, dest=(caller_rank +1))  # Worker send the cumulative data to the next worker.
   else:  # Worker that has the rank world_size - 1 receives the data from its preceding processor in the form of the dictionary, and sends the final cumulative data to the processor 0.
      data_dict2 = comm.recv(source=(caller_rank -1))   # Key is the unigram/bigram, value is the count.
      for key in data_dict2.keys():
         data_dict[key] = data_dict.get(key, 0) + data_dict2[key]
      comm.send(data_dict, 0)  # Worker that has the rank world_size - 1 sends the final data in the form of the dictionary to the processor 0.
   return

def calculate_bigram_probability(biglist, data_dict):  # Function to calculate the probabilities of the bigrams in the biglist from the frequencies in the data_dict.
   for big in biglist:  # For loop to iterate the bigrams in the biglist.
      bigw = big.split()  # Seperates the bigram tokens in order to find first token.
      firstw = bigw[0]
      probability = data_dict.get(big, 0) / data_dict[firstw]  # Probability of the given bigram is = (Freq(<bigram>) / Freq(<first token of the bigram>)).
      print("Bigram:", big, "Probability: %.5f" % probability)  # The probability is printed with 5 digit precision after point.
   return

if rank == 0:  # Processor 0, MASTER, calls the main function.
   if __name__ == "__main__":
      main(sys.argv[1:])
else:
   method = comm.recv(source=0)  # Workers wait until the merge_method is determined by the processor 0.
   if method == 'MASTER':  # If the method is MASTER, workers call the method_master function, otherwise they call method_workers function.
      method_master()
   else:
      method_workers(rank)




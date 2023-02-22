# MPI_Project_on_Bigram_Language_Model
This project was my 2nd Homework for the course CMPE 300 (Analysis of Algorithms) at Bogazici University. This project uses MPI framework on Python language with mpi4py package.
## About the Project
In this project we used MPI framework to calculate the probabilies of the given bigrams from an input file. Necessary background about the language models as well as the project requirements can be found in the project description file in this repository. Our design approach, example input files & results, detailed explanations about the functions in the code, and many useful information can be found in the documentation file in this repository. This project aimed to be a introductory project for learning MIMD type parallel algorithms. (Refer to Flynn's Taxonomy for the MIMD type parallel algorithms.)
## To Run the Code
We used Open MPI library for running MPI based projects. There is an installation guide for Open MPI in this repository. After installing Open MPI, please install 'mpi4py' package to your Python if you do not already have. One can easily install with pip via following command:
```pip3 install mpi4py```
Now the project is ready to be tested. One can use the example files in this repository directly or by modifying them to test the project. (example_input.txt, example_test.txt)
One can run the project via command:
```mpiexec -n <n> python3 main.py --input_file <input_file> --merge_method <MASTER||WORKERS> --test_file <test_file>```
## Final Remarks
Since this is an introductory project about MPI framework we are advised to just use 'send' and 'recv' functions. Of course, they are enough for this project's purposes but a better design would include 'scatter' and 'gather' functions to avoid some for loops.

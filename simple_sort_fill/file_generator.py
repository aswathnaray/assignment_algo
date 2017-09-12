import random, string
import pandas as pd


def random_word(length):
   """Random file names creation"""
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))


def files_gen(num_files):
    """Generate num_files number of random file names with file sizes within a given range specified by rf_size"""
    rf_name = []
    rf_size = []
    for i in range(num_files):
        rf_name.append(random_word(6)+'.dat')
        rf_size.append(random.randint(10, 500))

    rf_mx = {'# filename': rf_name, 'size': rf_size}
    rf_df = pd.DataFrame(rf_mx)
    rf_df.to_csv('files.txt', sep=' ', index=False)


def nodes_gen(num_nodes):
    """Generate num_nodes number of random nodes with sizes within a given range specified by rn_size"""
    rn_name = []
    rn_size = []
    for j in range(num_nodes):
        rn_name.append('node'+str(j))
        rn_size.append(random.randint(10, 100000))

    rn_mx = {'# node': rn_name, 'size': rn_size}
    rn_df = pd.DataFrame(rn_mx)
    rn_df.to_csv('nodes.txt', sep=' ', index=False)

number_of_files = 5000
number_of_nodes = 500

files_gen(number_of_files)
nodes_gen(number_of_nodes)

import pandas as pd
import argparse
import sys

# It is made sure that the program runs in any computer without the need for extensive libraries
# Pandas and argparse is chosen for their effectiveness and ease of use, if these are not pre-installed with Py3,
# kindly install it before running the code


def get_invalidrows(file_inv):
    """Catch commented rows and inform during file input.
    Returns hash_index (list with all commented line indices)"""
    hash_index = []
    dummy_no = 0
    with open(file_inv) as f:
        for row_inv in f:
            if '#' in row_inv:
                hash_index.append(dummy_no)
            dummy_no += 1
    return(hash_index)


def maxfile_to_maxnode(sorted_f1, sorted_n1):
    """Allocate files to nodes by checking if file size (descending) less than or equal to node size (descending)
    After allocation, update remaining space in node and drop the allocated files from future input
    Mergesort is chosen for stability (negligible time difference when compared to other sorting algorithms)"""

    sorted_n1 = sorted_n1.sort_values('node_size', ascending=0, kind='mergesort')

    it_files = sorted_f1.iterrows()
    it_nodes = sorted_n1.iterrows()

    for ind_f, rf in it_files:
        for ind_n, rn in it_nodes:
            if rf[1] <= rn[1]:
                rf_o.append(rf[0])
                rn_o.append(rn[0])
                dsort_nodes.iloc[ind_n, 1] = dsort_nodes.iloc[ind_n, 1] - rf[1]
                dsort_files.drop(ind_f, inplace=True)
                break


def maxfile_to_minnode(sorted_f2, sorted_n2):
    """Allocate files to nodes by checking if file size (descending) less than or equal to node size (ascending)
    After allocation, update remaining space in node and drop the allocated files from future input"""

    sorted_n2 = sorted_n2.sort_values('node_size', ascending=1, kind='mergesort')

    it_files = sorted_f2.iterrows()
    it_nodes = sorted_n2.iterrows()

    for ind_f, rf in it_files:
        for ind_n, rn in it_nodes:
            if rf[1] <= rn[1]:
                rf_o.append(rf[0])
                rn_o.append(rn[0])
                dsort_nodes.iloc[ind_n, 1] = dsort_nodes.iloc[ind_n, 1] - rf[1]
                dsort_files.drop(ind_f, inplace=True)
                break


def maxout(sorted_f3):
    """Assign NULL to file that has size greater than the size of maximum node size
     This is done under the assumption that files cannot be segmented and distributed to smaller nodes"""

    max_ind = sorted_f3.iloc[:, 1].argmax()
    max_val = dsort_files.loc[max_ind]
    rf_o.append(max_val[0])
    rn_o.append('NULL')
    dsort_files.drop(max_ind, inplace=True)

parser = argparse.ArgumentParser()
parser.add_argument('-f', required=True, help="file name of list of files in .txt format")
parser.add_argument('-n', required=True, help="file name of list of nodes in .txt format")
parser.add_argument('-o', help="output file name in .txt format", default='result.txt')

args = parser.parse_args()


# read input files and nodes
try:
    ip_files = pd.read_csv(str(args.f), sep=' ', skiprows=get_invalidrows(str(args.f)),
                           names=['file_name', 'file_size'], dtype={'file_name': 'str', 'file_size': 'int'})
    ip_nodes = pd.read_csv(str(args.n), sep=' ', skiprows=get_invalidrows(str(args.n)),
                           names=['node_name', 'node_size'], dtype={'node_name': 'str', 'node_size': 'int'})
except ValueError:
    print('Error in input file: Check source file data formats (file name, node name = string) (file size, node size = integer)')
    sys.exit()
except FileNotFoundError:
    print('File not found. Make sure you have given the complete path, along with the format extension')
    sys.exit()

# initial sorting and initialization
dsort_files = ip_files.sort_values('file_size', ascending=0, kind='mergesort')
dsort_files = dsort_files.reset_index(drop=True)
dsort_nodes = ip_nodes.sort_values('node_size', ascending=0, kind='mergesort')
dsort_nodes = dsort_nodes.reset_index(drop=True)

rf_o = []
rn_o = []
remfiles = [len(dsort_files)]
total_files = len(dsort_files)
files_alloc = 0


for sweep in range(0, 100000):

    # show progress of file allocation

    sys.stdout.write('\r')
    sys.stdout.write('Files processed: '+str(files_alloc)+'/'+str(total_files))
    sys.stdout.flush()

    # break after allocating all files, otherwise alternatively loop through the two allocation functions
    # assign NULL to files that maxout and assign NULL to files if the program stops converging

    if files_alloc == total_files:
        break
    else:
        if (sweep == 0 or remfiles[-1] < remfiles[-2]) and (len(dsort_files) > 0):
            if max(dsort_files.iloc[:, 1]) <= max(dsort_nodes.iloc[:, 1]):
                maxfile_to_maxnode(dsort_files, dsort_nodes)
                maxfile_to_minnode(dsort_files, dsort_nodes)
                remfiles.append(len(dsort_files))
            else:
                maxout(dsort_files)
                remfiles.append(len(dsort_files))
        else:
            it_files = dsort_files.iterrows()
            for ind_f, rf in it_files:
                rf_o.append(rf[0])
                rn_o.append('NULL')
            break
        files_alloc = len(rf_o)

_o = {'#filename': rf_o, 'node': rn_o}
output = pd.DataFrame(_o)
output.to_csv(str(args.o), sep=' ', index=False, header=False)

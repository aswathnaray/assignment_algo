import pandas as pd
import statistics as st


def get_invalidrows(file_inv):
    """Get invalid rows and pass on hash_index to input readers"""
    hash_index = []
    dummy_no = 0
    with open(file_inv) as f:
        for row_inv in f:
            if '#' in row_inv:
                hash_index.append(dummy_no)
            dummy_no += 1
    return(hash_index)


outp = pd.read_csv('result.txt', sep=' ')
inp_f = pd.read_csv('files.txt', sep=' ', skiprows=get_invalidrows('files.txt'), names=['file_name', 'file_size'])
inp_n = pd.read_csv('nodes.txt', sep=' ', skiprows=get_invalidrows('nodes.txt'), names=['node_name', 'node_size'])

iter_n = inp_n.iterrows()
iter_op = outp.iterrows()

fsz_fin = []

# Find files in each node and their cumulative sums | find mean and standard deviation to get an idea of balance
# This value is not a conclusive indicator as the loading depends on node sizes too

for ind_n, rn in iter_n:
    fsz = []
    f_list = [outp.iloc[i, 0] for i, x in enumerate(outp.iloc[:, 1] == rn[0]) if x]
    for fl in f_list:
        f_size = [inp_f.iloc[j, 1] for j, y in enumerate(inp_f.iloc[:, 0] == fl) if y]
        fsz.extend(f_size)
    fsz_fin.append(sum(fsz))

print(st.mean(fsz_fin), st.stdev(fsz_fin))
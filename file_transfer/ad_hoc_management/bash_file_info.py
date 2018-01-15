#
# S. Van Hoey
# 
# ENRAM meeting 2017-01
#

import sys
import argparse

import h5py
import pandas as pd

def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.add_argument('filename', type=str, 
                        help='filename of hdf5 file to check')
    args = parser.parse_args()

    filename = args.filename

    quantities = []
    elangles = []
    shapes = []

    hf = h5py.File(filename, 'r')
    gen_info = dict(hf.get("what").attrs.items())

    for key, val in hf.items():
        if hf.get("/{}/data1/what".format(key)):
            quantities.append(dict(hf.get("/{}/data1/what".format(key)).attrs.items())["quantity"].decode("utf-8"))
        if hf.get("/{}/where".format(key)):
            elangles.append(dict(hf.get("/{}/where".format(key)).attrs.items())["elangle"])
        if hf.get("/{}/data1".format(key)):
            temp = hf.get("/{}/data1/data".format(key)).__str__()
            shapes.append(temp[28:38])
    file_info = pd.DataFrame({"quantity":quantities, "elangle": elangles, "shape": shapes})

    print(gen_info['date'].decode("utf-8"), gen_info["time"].decode("utf-8"), gen_info["source"].decode("utf-8").split(":")[-1][:5])
    print(file_info.sort_values("elangle"))


if __name__ == "__main__":
    sys.exit(main())
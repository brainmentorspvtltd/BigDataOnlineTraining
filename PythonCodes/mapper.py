import sys

# input comes from STDIN (Standard Input)
for line in sys.stdin:
    # remove leading and trailing whitespaces
    line = line.strip()
    # split the line or sentence into words
    words = line.split()

    for word in words:
        # write the result to STDOUT (Standard Output)
        # what we output here will be the input from the reduce
        print('%s \t %s'%(word,1))
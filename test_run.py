import argparse

parser = argparse.ArgumentParser(description='parse arguments to run script on prod or preprod')
parser.add_argument("--opt")

args = parser.parse_args()

try:
    opt_value = args.opt2
    print(opt_value)
except:
    print('no args passed')

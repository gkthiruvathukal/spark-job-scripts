import argparse
import sys
import os
import os.path

TEMPLATES = [ "start-spark.sh.in", "submit-spark.sh.in" ]

DEFAULT_VARS = {
  'note' : 'This script is generated. Do not edit.'
}

def parse_options():
    parser = argparse.ArgumentParser()
    parser.add_argument('--spark_home', required=True, help="path to Apache Spark (this must exist)")
    parser.add_argument('--spark_conf_dir', default='%(spark_home)s/conf', help="path to conf dir (vars filled in as a template)")
    parser.add_argument('--spark_log_dir', default='%(spark_home)s/logs', help="path to logs dir(vars filled in as a template)")
    parser.add_argument('--spark_slaves', default='%(spark_home)s/conf/slaves.${COBALT_JOBID}', help="path to slaves (vars filled in as a template)")
    parser.add_argument('--spark_job_info_file', default='$HOME/spark-hostname-${COBALT_JOBID}.txt',  help="job info file (shared between spark-submit and start-spark)")
    parser.add_argument('--script_install_dir', default=os.path.join(os.getcwd(),"bin"),  help="job info file (shared between spark-submit and start-spark)")
    parser.add_argument('--create', default=False, action="store_true", help="create the --script_install_dir, if not exists")
    return parser.parse_args()

def main():
    env = process_options()
    env.update(DEFAULT_VARS)
    for filename in TEMPLATES:
       generate_file(filename, env)

def generate_file(filename, env):
    (script_sh, extension) = os.path.splitext(filename)
    if extension != '.in':
       return
    with open(filename) as infile:
       text = infile.read()

    text = text % env

    output_path = os.path.join(env['script_install_dir'], script_sh)
    with open(output_path, "w") as outfile:
       outfile.write(text)

def process_options():
    args = parse_options()
    args_ns = vars(args)

    if not os.path.exists(args.spark_home):
       print("Path to $SPARK_HOME (e.g. <full-path-to>/spark) is required")
       sys.exit(1)
    
    if not os.path.exists(args.script_install_dir):
       if not args.create:
          print("Destination dir does not exist; use --create to attempt creation")
          sys.exit(2)
       else:
          os.makedirs(args.script_install_dir)

    bound_args = dict(args_ns) # TODO: make this a comprehension!
    for arg in args_ns:
       if type(args_ns[arg]) != type(''):
          continue
       if args_ns[arg].find('%') >= 0:
          bound_args[arg] = args_ns[arg] % args_ns
    return bound_args
 
if __name__ == '__main__':
  main()

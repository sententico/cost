#!/usr/bin/python3

import  sys
import  os
import  argparse
import  pickle
import  time
from    datetime                import  datetime,timedelta
import  random
import  signal
import  socket
import  ssl
import  json
import  subprocess
import  boto3
from    botocore.exceptions     import  ProfileNotFound,ClientError,EndpointConnectionError,ConnectionClosedError
import  datadog
import  awslib.patterns         as      aws

class GError(Exception):
    '''Module exception'''
    pass

KVP=   {                                # key-value pair defaults/validators (overridden by command-line)
        'settings':     '.cmon_settings.json',
       }

def overrideKVP(overrides):
    '''Override KVP dictionary defaults with command-line inputs'''
    for k,v in overrides.items():
        dfv,meta = KVP.get(k), KVP.get('_' + k)
        if k.startswith('_') or dfv is None: raise GError('{} key unrecognized'.format(k))
        if v in {'', '?'}:
            if not meta or len(meta) < 3:
                raise GError('specify value for {}; default is {}'.format(k, KVP[k]))
            raise GError('specify value in [{},{}] range for {}; default is {}'.format(
                         meta[1], meta[2], k, KVP[k]))
        if meta:
            try:
                if len(meta) == 1:      # apply validator function to value
                    v = meta[0](k, v)
                    if v is None: raise GError('validation failed')
                elif len(meta) >= 3:    # convert string value to desired type with bounds check
                    try:    ev = eval(v) if type(v) is str else v
                    except: raise GError('invalid value')
                    v = meta[0](ev)
                    if   v < meta[1]: v = meta[1]
                    elif v > meta[2]: v = meta[2]
            except (ValueError, GError) as e:
                raise GError('{} cannot be set ({})'.format(k, e))
        KVP[k] = v

def getSettings():
    pass

def gophEC2AWS():
    pass

def gophRDSAWS():
    pass

def main():
    '''Parse command line args and run command'''
    gophModels =   {                    # gopher model map
                    'ec2.aws':  [gophEC2AWS,        'description'],
                    'rds.aws':  [gophRDSAWS,        'description'],
                   }

    # define and parse command line parameters
    parser = argparse.ArgumentParser(description='''This command fetches cmon object model updates''')
    parser.add_argument('models',        nargs='+', metavar='model',
                        help='''cmon object model; {} are supported'''.format(', '.join(gophModels)))
    args = parser.parse_args()

    # run command
    try:                                # initialize, process arguments
        sf,kvd = {}, statXforms[args.xform][2] if args.xform else {}
        kvd.update({k.partition('=')[0].strip():k.partition('=')[2].strip() for k in args.key})
        overrideKVP(kvd)
        try:                            # open AWS stats DB
            with open(KVP['db'], 'rb') as p: sd = pickle.load(p)
        except  KeyboardInterrupt: raise
        except: sd = {'useq':0, 'update':{}, 'accts':{},
                      'ec2':{}, 'ebs':{}, 'rds':{},
                      '.ec2':{},'.ebs':{},'.rds':{}}
 
        if args.update or args.all:     # update stats DB as requested
            statUpdate(sd, args)
            try:
                with open(KVP['db'], 'wb') as p: pickle.dump(sd, p)
            except  KeyboardInterrupt: raise
            except: raise SError('unable to update stats DB')
        if args.xform:                  # filter and transform stats as requested
            for f in [preFilter,    statFilters[args.filter][0] if args.filter else statXforms[args.xform][1],
                      postFilter,   statXforms[args.xform][0]]:
                f(sd, sf, args)
                                        # handle exceptions; broken pipe exit avoids console errors
    except  BrokenPipeError:            os._exit(0)
    except  KeyboardInterrupt:          sys.stderr.write('\n** command interrupted **\n')
    except (AssertionError, IOError,
            GError) as e:               sys.stderr.write('** {} **\n'.format(e if e else 'unknown exception'))

if __name__ == '__main__':  main()      # called as script
else:                       pass        # loaded as module

#!/usr/bin/python3

import  sys
import  os
import  argparse
from    datetime                import  datetime,timedelta
import  signal
import  json
import  boto3
from    botocore.exceptions     import  ProfileNotFound,ClientError,EndpointConnectionError,ConnectionClosedError
#import  datadog

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

def terminate(sig, frame):
    raise KeyboardInterrupt
def ex(err, code):
    if err: sys.stderr.write(err)
    sys.exit(code)

def csvWriter(m, cols):
    section = ''
    def csvWrite(s, row):
        nonlocal m, cols, section
        if not section:     sys.stdout.write('#!id {} results from gopher\n{}\n'.format(m, '\t'.join(cols)))
        if section != s:    sys.stdout.write('\n#!section {}\n'.format(s)); section = s
        sys.stdout.write('"{}"\n'.format('"\t"'.join([row.get(n,'') for n in cols])))
    return csvWrite

def gophEC2AWS(cmon, m):
    csv = csvWriter(m, ['acct','type','plat','az','ami','state','spot','tags'])
    flt = str.maketrans('','','"=\t')
    for a in ['927185244192']:
        session = boto3.Session(profile_name=a)
        for r in ['us-east-1', 'us-east-2']:
            ec2, s = session.resource('ec2', region_name=r), a+':'+r
            for i in ec2.instances.all():
                csv(s, {'acct':     a,
                        'type':     i.instance_type,
                        'plat':     '' if not i.platform else i.platform,
                        'az':       i.placement.get('AvailabilityZone',r),
                        'ami':      '' if not i.image_id else i.image_id,
                        'state':    i.state.get('Name',''),
                        'spot':     '' if not i.spot_instance_request_id else i.spot_instance_request_id,
                        'tags':     '' if not i.tags else '{}'.format('\t'.join([
                                    '{}={}'.format(t['Key'].translate(flt), t['Value'].translate(flt))
                                    for t in i.tags if t['Value'] not in {'','--','unknown','Unknown'} and
                                                       not t['Key'].startswith(('SCRM'))])),
                        })

def gophRDSAWS(cmon, m):
    sys.stdout.write('gopher getting {} data: {}\n'.format(m, cmon))
    csv = csvWriter(m, ['acct','type','stype','size','engine','ver','lic','az','multiaz','create','state','tags'])
    flt = str.maketrans('','','"=\t')
    for a in ['927185244192']:
        session = boto3.Session(profile_name=a)
        for r in ['us-east-1', 'us-east-2']:
            rds, s = session.client('rds', region_name=r), a+':'+r
            for d in rds.describe_db_instances().get('DBInstances',[]):
                csv(s, {'acct':     a,
                        'type':     d.get('DBInstanceClass'),
                        'stype':    d.get('StorageType'),
                        'size':     str(d.get('AllocatedStorage')),
                        'engine':   d.get('Engine',''),
                        'ver':      d.get('EngineVersion',''),
                        'lic':      d.get('LicenseModel',''),
                        'az':       d.get('AvailabilityZone',r),
                        'multiaz':  str(d.get('MultiAZ')),
                        'create':   d.get('InstanceCreateTime').isoformat(),
                        'state':    d.get('DBInstanceStatus',''),
                        #'tags':    ...,
                       })

def main():
    '''Parse command line args and run gopher command'''
    gophModels = {                      # gopher model map
        'ec2.aws':      [gophEC2AWS,    'fetch EC2 resources from AWS'],
        'rds.aws':      [gophRDSAWS,    'fetch RDS resources from AWS'],
    }
                                        # define and parse command line parameters
    parser = argparse.ArgumentParser(description='''This command fetches cmon object model updates''')
    parser.add_argument('models',           nargs='+', choices=gophModels, metavar='model',
                        help='''cmon object model; {} are supported'''.format(', '.join(gophModels)))
    parser.add_argument('-k',   '--key',    action='append', metavar='kvp', default=[],
                        help='''key-value pair of the form <k>=<v> (key one of: {})'''.format(
                             ', '.join(['{} [{}]'.format(k, KVP[k]) for k in KVP
                             if not k.startswith('_')])))
    args = parser.parse_args()

    try:                                # run gopher command
        signal.signal(signal.SIGTERM, terminate)
        overrideKVP({k.partition('=')[0].strip():k.partition('=')[2].strip() for k in args.key})
        cmon = json.load(sys.stdin)

        for model in args.models:
            gophModels[model][0](cmon, model)
                                        # handle exceptions; broken pipe exit avoids console errors
    except  json.JSONDecodeError:       ex('** invalid settings file **\n', 1)
    except  BrokenPipeError:            os._exit(0)
    except  KeyboardInterrupt:          ex('\n** command interrupted **\n', 10)
    except (AssertionError, IOError, RuntimeError,
            GError) as e:               ex('** {} **\n'.format(e if e else 'unknown exception'), 10)

if __name__ == '__main__':  main()      # called as script
else:                       pass        # loaded as module
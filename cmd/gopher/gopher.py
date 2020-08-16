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
    section, flt = None, str.maketrans('\n',' ','\r')
    def csvWrite(s, row):
        nonlocal m, cols, section, flt
        if section is None:
            sys.stdout.write('#!begin gopher {} # at {}\n{}\n'.format(m,
                             datetime.now().isoformat(), '\t'.join(cols)))
            section = ''
        if row:
            if s and s != section:
                sys.stdout.write('\n#!section {}\n'.format(s))
                section = s
            sys.stdout.write('"{}"\n'.format('"\t"'.join([row.get(n,'').translate(flt).replace('"','""')
                                                          for n in cols])))
        else:
            sys.stdout.write('\n#!end gopher {} # at {}\n'.format(m, datetime.now().isoformat()))
    return csvWrite

def gophEC2AWS(m, cmon, args):
    if not cmon.get('AWS'): raise GError('no AWS configuration for {}'.format(m))
    csv = csvWriter(m, ['id','acct','type','plat','az','ami','state','spot','tags'])
    flt = str.maketrans('\t',' ','=')
    for a,ar in cmon['AWS']['Accounts'].items():
        session = boto3.Session(profile_name=a)
        for r,u in ar.items():
            if u < 0: continue
            ec2, s = session.resource('ec2', region_name=r), a+':'+r
            for i in ec2.instances.all():
                csv(s, {'id':       i.id,
                        'acct':     a,
                        'type':     i.instance_type,
                        'plat':     '' if not i.platform else i.platform,
                        'az':       i.placement.get('AvailabilityZone',r),
                        'ami':      '' if not i.image_id else i.image_id,
                        'state':    i.state.get('Name',''),
                        'spot':     '' if not i.spot_instance_request_id else i.spot_instance_request_id,
                        'tags':     '' if not i.tags else '{}'.format('\t'.join([
                                    '{}={}'.format(t['Key'], t['Value'].translate(flt)) for t in i.tags if
                                    t['Value'] not in {'','--','unknown','Unknown'} and (t['Key'] in {'env','dc','product','app',
                                    'role','cust','customer','team','group','alert','slack','version','release','build','stop',
                                    'SCRM_Group','SCRM_Instance_Stop'} or t['Key'].startswith(('aws:')))])),
                        })
    csv(None, None)

def gophEBSAWS(m, cmon, args):
    if not cmon.get('AWS'): raise GError('no AWS configuration for {}'.format(m))
    csv = csvWriter(m, ['id','acct','type','size','iops','az','state','mount','tags'])
    flt = str.maketrans('\t',' ','=')
    for a,ar in cmon['AWS']['Accounts'].items():
        session = boto3.Session(profile_name=a)
        for r,u in ar.items():
            if u < 0: continue
            ec2, s = session.resource('ec2', region_name=r), a+':'+r
            for v in ec2.volumes.all():
                csv(s, {'id':       v.id,
                        'acct':     a,
                        'type':     v.volume_type,
                        'size':     str(v.size),
                        'iops':     str(v.iops),
                        'az':       v.availability_zone,
                        'state':    v.state,
                        'mount':    '{}:{}:{}'.format(v.attachments[0]['InstanceId'],v.attachments[0]['Device'],
                                    v.attachments[0]['DeleteOnTermination']) if len(v.attachments)==1 else
                                    '{} attachments'.format(len(v.attachments)),
                        'tags':     '' if not v.tags else '{}'.format('\t'.join([
                                    '{}={}'.format(t['Key'], t['Value'].translate(flt)) for t in v.tags if
                                    t['Value'] not in {'','--','unknown','Unknown'} and (t['Key'] in {'env','dc','product','app',
                                    'role','cust','customer','team','group','alert','slack','version','release','build','stop',
                                    'SCRM_Group','SCRM_Instance_Stop'} or t['Key'].startswith(('aws:')))])),
                        })
    csv(None, None)

def gophRDSAWS(m, cmon, args):
    if not cmon.get('AWS'): raise GError('no AWS configuration for {}'.format(m))
    csv = csvWriter(m, ['id','acct','type','stype','size','engine','ver','lic','az','multiaz','state','tags'])
    flt = str.maketrans('\t',' ','=')
    for a,ar in cmon['AWS']['Accounts'].items():
        session = boto3.Session(profile_name=a)
        for r,u in ar.items():
            if u < 0: continue
            rds, s = session.client('rds', region_name=r), a+':'+r
            for d in rds.describe_db_instances().get('DBInstances',[]):
                csv(s, {'id':       d.get('DBInstanceArn'),
                        'acct':     a,
                        'type':     d.get('DBInstanceClass'),
                        'stype':    d.get('StorageType'),
                        'size':     str(d.get('AllocatedStorage')),
                        'engine':   d.get('Engine',''),
                        'ver':      d.get('EngineVersion',''),
                        'lic':      d.get('LicenseModel',''),
                        'az':       d.get('AvailabilityZone',r),
                        'multiaz':  str(d.get('MultiAZ')),
                        'state':    d.get('DBInstanceStatus',''),
                        'tags':     'placeholder="value"'.translate(flt),
                       })
    csv(None, None)

def main():
    '''Parse command line args and run gopher command'''
    gophModels = {                      # gopher model map
        'ec2.aws':      [gophEC2AWS,    'fetch EC2 instances from AWS'],
        'ebs.aws':      [gophEBSAWS,    'fetch EBS volumes from AWS'],
        'rds.aws':      [gophRDSAWS,    'fetch RDS databases from AWS'],
    }
                                        # define and parse command line parameters
    parser = argparse.ArgumentParser(description='''This command fetches cmon object model updates''')
    parser.add_argument('models',           nargs='+', choices=gophModels, metavar='model',
                        help='''cmon object model; {} are supported'''.format(', '.join(gophModels)))
    parser.add_argument('-o',   '--opt',    action='append', metavar='option', default=[],
                        help='''command option''')
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
            gophModels[model][0](model, cmon, args)
                                        # handle exceptions; broken pipe exit avoids console errors
    except  json.JSONDecodeError:       ex('** invalid settings file **\n', 1)
    except  BrokenPipeError:            os._exit(0)
    except  KeyboardInterrupt:          ex('\n** command interrupted **\n', 10)
    except (AssertionError, IOError, RuntimeError,
            ProfileNotFound, ClientError, EndpointConnectionError, ConnectionClosedError,
            GError) as e:               ex('** {} **\n'.format(e if e else 'unknown exception'), 10)

if __name__ == '__main__':  main()      # called as script
else:                       pass        # loaded as module
#!/usr/bin/python3

import  sys
import  os
import  argparse
from    datetime                import  datetime,timedelta
import  random
import  signal
import  json
import  subprocess
import  csv
import  boto3
from    botocore.exceptions     import  ProfileNotFound,ClientError,EndpointConnectionError,ConnectionClosedError
import  awslib.patterns         as      aws
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

def getWriter(m, cols):
    section, flt, buf = '', str.maketrans('\n',' ','\r'), [
        '#!begin gopher {} # at {}'.format(m, datetime.now().isoformat()),
        '\t'.join(cols),
    ]
    def csvWrite(s, row):
        nonlocal m, cols, section, flt, buf
        if row:
            if s and s != section:
                buf.append('\n#!section {}'.format(s))
                section = s
            buf.append('"{}"'.format('"\t"'.join([row.get(n,'').translate(flt).replace('"','""')
                                                  for n in cols])))
            sys.stdout.write('{}\n'.format('\n'.join(buf)))
            buf = []
        elif not buf:
            sys.stdout.write('\n#!end gopher {} # at {}\n'.format(m, datetime.now().isoformat()))
    return csvWrite

def gophEC2AWS(m, cmon, args):
    if not cmon.get('AWS'): raise GError('no AWS configuration for {}'.format(m))
    pipe = getWriter(m, ['id','acct','type','plat','vol','az','ami','state','spot','tag'])
    flt = str.maketrans('\t',' ','=')
    for a,ar in cmon['AWS']['Accounts'].items():
        session = boto3.Session(profile_name=a)
        for r,u in ar.items():
            if u < 1.0 and u <= random.random(): continue
            ec2, s = session.resource('ec2', region_name=r), a+':'+r
            for i in ec2.instances.all():
                pipe(s, {'id':      i.id,
                         'acct':    a,
                         'type':    i.instance_type,
                         'plat':    '' if not i.platform else i.platform,
                         'vol':     str(len(i.block_device_mappings)),
                         'az':      i.placement.get('AvailabilityZone',r),
                         'ami':     '' if not i.image_id else i.image_id,
                         'state':   i.state.get('Name',''),
                         'spot':    '' if not i.spot_instance_request_id else i.spot_instance_request_id,
                         'tag':     '' if not i.tags else '{}'.format('\t'.join([
                                    '{}={}'.format(t['Key'].translate(flt), t['Value'].translate(flt)) for t in i.tags if
                                    t['Value'] not in {'','--','unknown','Unknown'} and (t['Key'] in {'env','dc','product','app',
                                    'role','cust','customer','team','group','alert','slack','version','release','build','stop',
                                    'SCRM_Group','SCRM_Instance_Stop'} or t['Key'].startswith(('aws:')))])),
                        })
    pipe(None, None)

def gophEBSAWS(m, cmon, args):
    if not cmon.get('AWS'): raise GError('no AWS configuration for {}'.format(m))
    pipe = getWriter(m, ['id','acct','type','size','iops','az','state','mount','tag'])
    flt = str.maketrans('\t',' ','=')
    for a,ar in cmon['AWS']['Accounts'].items():
        session = boto3.Session(profile_name=a)
        for r,u in ar.items():
            if u < 1.0 and u <= random.random(): continue
            ec2, s = session.resource('ec2', region_name=r), a+':'+r
            for v in ec2.volumes.all():
                pipe(s, {'id':      v.id,
                         'acct':    a,
                         'type':    v.volume_type,
                         'size':    str(v.size),
                         'iops':    str(v.iops),
                         'az':      v.availability_zone,
                         'state':   v.state,
                         'mount':   '{}:{}:{}'.format(v.attachments[0]['InstanceId'],v.attachments[0]['Device'],
                                    v.attachments[0]['DeleteOnTermination']) if len(v.attachments)==1 else
                                    '{} attachments'.format(len(v.attachments)),
                         'tag':     '' if not v.tags else '{}'.format('\t'.join([
                                    '{}={}'.format(t['Key'].translate(flt), t['Value'].translate(flt)) for t in v.tags if
                                    t['Value'] not in {'','--','unknown','Unknown'} and (t['Key'] in {'env','dc','product','app',
                                    'role','cust','customer','team','group','alert','slack','version','release','build','stop',
                                    'SCRM_Group','SCRM_Instance_Stop'} or t['Key'].startswith(('aws:')))])),
                        })
    pipe(None, None)

def gophRDSAWS(m, cmon, args):
    if not cmon.get('AWS'): raise GError('no AWS configuration for {}'.format(m))
    pipe = getWriter(m, ['id','acct','type','stype','size','iops','engine','ver','lic','az','multiaz','state','tag'])
    flt = str.maketrans('\t',' ','=')
    for a,ar in cmon['AWS']['Accounts'].items():
        session = boto3.Session(profile_name=a)
        for r,u in ar.items():
            if u < 1.0 and u <= random.random(): continue
            rds, s = session.client('rds', region_name=r), a+':'+r
            for d in rds.describe_db_instances().get('DBInstances',[]):
                arn = d['DBInstanceArn']
                try:    dtags = rds.list_tags_for_resource(ResourceName=arn)['TagList']
                except  KeyboardInterrupt: raise
                except: dtags = None
                pipe(s, {'id':      arn,
                         'acct':    a,
                         'type':    d.get('DBInstanceClass'),
                         'stype':   d.get('StorageType'),
                         'size':    str(d.get('AllocatedStorage',0)),
                         'iops':    str(d.get('Iops',0)),
                         'engine':  d.get('Engine',''),
                         'ver':     d.get('EngineVersion',''),
                         'lic':     d.get('LicenseModel',''),
                         'az':      d.get('AvailabilityZone',r),
                         'multiaz': str(d.get('MultiAZ',False)),
                         'state':   d.get('DBInstanceStatus',''),
                         'tag':     '' if not dtags else '{}'.format('\t'.join([
                                    '{}={}'.format(t['Key'].translate(flt), t['Value'].translate(flt)) for t in dtags if
                                    t['Value'] not in {'','--','unknown','Unknown'} and (t['Key'] in {'env','dc','product','app',
                                    'role','cust','customer','team','group','alert','slack','version','release','build','stop',
                                    'SCRM_Group','SCRM_Instance_Stop'} or t['Key'].startswith(('aws:')))])),
                        })
    pipe(None, None)

def gophCURAWS(m, cmon, args):
    if not cmon.get('BinDir'): raise GError('no bin directory for {}'.format(m))
    pipe, head, ids, s = getWriter(m, ['id','acct','typ','hour','svc','utyp','uop','az','rid','desc','usg','cost',
                                       'name','env','dc','prod','app','cust','team','ver',
                                      ]), {}, set(), ""
    with subprocess.Popen([cmon.get('BinDir').rstrip('/')+'/goph_curaws.sh'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True) as p:
        for l in p.stdout:
            if l.startswith('identity/LineItemId,'):
                head = {h:i for i,h in enumerate(l[:-1].split(','))}

            elif not l.startswith('#!'):
                for col in csv.reader([l]): break
                if len(col) != len(head): continue
                # lineItem/ProductCode (how differs from product/ProductName?)
                # lineItem/Operation (usefully extends lineItem/UsageType?)
                # lineItem/AvailabilityZone (ever present when product/region is not?)
                # lineItem/UnblendedCost (how differs from lineItem/BlendedCost & pricing/publicOnDemandCost?)
                # lineItem/LineItemDescription (useful?)
                id,typ = col[0], col[head['lineItem/LineItemType']]
                rec = {
                    'id':       id,                                     # line item ID
                    'hour':     col[head['lineItem/UsageStartDate']],   # GMT timestamp (YYYY-MM-DDThh:mm:ssZ)
                }
                if typ != 'RIFee':
                    ubl,fee = col[head['lineItem/UnblendedCost']], col[head['reservation/RecurringFeeForUsage']]
                    try:    rec.update({
                        'usg':  col[head['lineItem/UsageAmount']],      # usage quantity/cost
                        'cost': ubl if not fee else str(float(fee)+float(ubl)),
                    })
                    except  ValueError: continue
                else: rec.update({
                        'usg':  col[head['reservation/UnusedQuantity']],# unused reservation quantity/cost
                        'cost': col[head['reservation/UnusedRecurringFee']],
                    })

                if id not in ids:
                    svc,uop,rid = col[head['product/ProductName']], col[head['lineItem/Operation']], col[head['lineItem/ResourceId']]
                    rec.update({        # TODO: process fixed line-item content leveraging regex library
                        'acct': col[head['lineItem/UsageAccountId']],   # (not billing account)
                        'typ':  typ,                                    # line item type (Usage, Tax, ...)
                        'svc': {'Amazon Elastic Compute Cloud':        'EC2',
                                'Amazon Simple Storage Service':       'S3',
                                'Amazon Relational Database Service':  'RDS',
                                'AmazonCloudWatch':                    'CloudWatch',
                               }.get(svc,svc.replace(
                                'Amazon ',              ''      ).replace(
                                'AWS ',                 ''      )),     # service name
                        'utyp': col[head['lineItem/UsageType']],        # usage detail (append ext utyp?)
                        'uop':  "" if uop in {'Any','any','ANY','Nil','nil','None','none','Null','null',
                                              'NoOperation','Not Applicable','N/A','n/a','Unknown','unknown',
                                             } else uop,                # usage operation
                        'az':   col[head['product/region']],            # service region
                        'rid':  rid.rsplit(':',1)[-1] if rid.startswith('arn:')
                                                      else rid,         # resource ID (i-, vol-, ...)
                        'desc': col[head['lineItem/LineItemDescription']].replace(
                                'USD ',                 '$'     ).replace(
                                'USD',                  '$'     ).replace(
                                '$0.00 ',               '$0 '   ).replace(
                                '$0.0 ',                '$0 '   ).replace(
                                '$$',                   '$'     ).replace(
                                ' per ',                '/'     ).replace(
                                ' - ',                  '; '    ).replace(
                                '  ',                   ' '     ).replace(
                                '-month',               '-mo'   ).replace(
                                '-Month',               '-mo'   ).replace(
                                ' / month',             '/mo'   ).replace(
                                '-hour',                '-hr'   ).replace(
                                '(or partial hour)','(or partial)').replace(
                                'Linux/UNIX',           'Linux' ).replace(
                                'transfer',             'xfer'  ).replace(
                                'Northern ',            'N. '   ).replace(
                                ' reserved instance ',  ' RI '  ),      # service description
                        'name': col[head['resourceTags/user:Name']],    # user-supplied resource name
                        'env':  col[head['resourceTags/user:env']],     # environment (prod, dev, ...)
                        'dc':   col[head['resourceTags/user:dc']],      # cost location (orl, iad, ...)
                        'prod': col[head['resourceTags/user:product']], # product (high-level)
                        'app':  col[head['resourceTags/user:app']],     # application (low-level)
                        'cust': col[head['resourceTags/user:cust']],    # cost or owning org
                        'team': col[head['resourceTags/user:team']],    # operating org
                        'ver':  col[head['resourceTags/user:version']], # major.minor
                    })
                    ids.add(id)
                pipe(s, rec)

            elif l.startswith('#!begin '):
                s = l[:-1].partition(' ')[2].partition('~link')[0]
        pipe(None, None)

def main():
    '''Parse command line args and run gopher command'''
    gophModels = {                      # gopher model map
        'ec2.aws':      [gophEC2AWS,    'fetch EC2 instances from AWS'],
        'ebs.aws':      [gophEBSAWS,    'fetch EBS volumes from AWS'],
        'rds.aws':      [gophRDSAWS,    'fetch RDS databases from AWS'],
        'cur.aws':      [gophCURAWS,    'fetch CUR cost/usage data from AWS'],
    }
                                        # define and parse command line parameters
    parser = argparse.ArgumentParser(description='''This command fetches cmon object model updates''')
    parser.add_argument('models',       nargs='+', choices=gophModels, metavar='model',
                        help='''cmon object model; {} are supported'''.format(', '.join(gophModels)))
    parser.add_argument('-o','--opt',   action='append', metavar='option', default=[],
                        help='''command option''')
    parser.add_argument('-k','--key',   action='append', metavar='kvp', default=[],
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
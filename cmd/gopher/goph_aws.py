#!/usr/bin/python3

import  sys
import  os
import  signal
import  argparse
import  json
from    datetime                import  datetime,timedelta
import  subprocess
import  random
import  csv
import  boto3
import  botocore
from    botocore.exceptions     import  ProfileNotFound,ClientError,EndpointConnectionError,ConnectionClosedError
#import  datadog
#import  awslib.patterns         as      aws

class GError(Exception):
    '''Module exception'''
    pass

KVP={                                   # key-value pair defaults/validators (overridden by command-line)
    'settings':         '~stdin',
    'metricsrngM':            60,       '_metricsrngM':     [int,      1,   1440],
    'metricsperS':           180,       '_metricsperS':     [int,      1,   3600],
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
                    except  KeyboardInterrupt: raise
                    except: raise GError('invalid value')
                    v = meta[0](ev)
                    if   v < meta[1]: v = meta[1]
                    elif v > meta[2]: v = meta[2]
            except (ValueError, GError) as e:
                raise GError('{} cannot be set ({})'.format(k, e))
        KVP[k] = v

def terminate(sig, frame):
    '''Raise a keyboard interrupt to terminate process'''
    raise KeyboardInterrupt
def ex(err, code):
    '''Exit process with specified return code'''
    if err: sys.stderr.write(err)
    sys.exit(code)

def getWriter(m, cols):
    '''Return a CSV (TSV) writer closure for cols output columns with interspersed metadata'''
    section,flt,buf = '', str.maketrans('\n',' ','\r'), [
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

def getTagFilter(settings):
    '''Return a tag filter closure for filtering and mapping AWS resource tags'''
    trd = {rsk:(set(            rsv.get('~filters',{}).get('include',[])),
                tuple(set(      rsv.get('~filters',{}).get('prefixes',[])+['cmon:'])),
                tuple(set(      rsv.get('~filters',{}).get('suffixes',[]))),
                {t:             r.get('~aliases',[]) for t,r in rsv.items() if t.startswith('cmon:')},
                set().union(*[  r.get('~aliases',[]) for t,r in rsv.items() if t.startswith('cmon:')]),
           ) for rsk,rsv in settings['AWS'].get('TagRules',{}).items()
                if rsk and not rsk.startswith('~') and type(rsv) is dict}

    def filterTags(at, tl):
        nonlocal trd
        incl,pfxs,sfxs,amap,aset = trd.get(at.get('~tagrules','default'),(set(),('cmon:'),(),{},set()))
 
        td,ad = {}, {}
        for t in tl:                    # filter resource tags; "*" wildcard includes all tags
            k,v = t.get('Key'), t.get('Value')
            if type(k) is not str or type(v) is not str or k in {
               '','--'} or v in {'','--','unknown','Unknown',
               }:                                                                   continue
            if k in aset:                                                           ad[k] = v
            if k.startswith(pfxs) or '*' in incl or k in incl or k.endswith(sfxs):  td[k] = v
        for n,al in amap.items():       # map aliases to native cmon tags
            if n not in td:
                for a in al:
                    if a in ad:                                                     td[n] = ad[a]; break
        return td
    return filterTags

def gophEC2AWS(model, settings, inputs, args):
    '''Fetch EBS volume detail from AWS'''
    if not settings.get('AWS'): raise GError('no AWS settings for {}'.format(model))
    metrics,tagf,pipe,flt,prof,sts = model.split('/',1)[-1]=='metrics', getTagFilter(settings), getWriter(model, [
        'id','acct','type','plat','vol','az','vpc','ami','state','spot','tag','metric',
    ]), str.maketrans('\t',' '), settings['AWS']['Profiles'], boto3.client('sts')
    if metrics:
        per,now = KVP['metricsperS'], datetime.utcnow(); ago = now-timedelta(minutes=KVP['metricsrngM'])

    for a,at in settings['AWS']['Accounts'].items():
        if not at.get('~profile') or not prof.get(at['~profile']): continue
        if at.get('~arn'):
            cred = sts.assume_role(RoleArn=at['~arn'], RoleSessionName=a)['Credentials']
            session =   boto3.Session(aws_access_key_id=cred['AccessKeyId'],
                                      aws_secret_access_key=cred['SecretAccessKey'],
                                      aws_session_token=cred['SessionToken'])
        else: session = boto3.Session(profile_name=a)
        if metrics:
            cw = session.resource('cloudwatch')
            cpu = cw.Metric('AWS/EC2','CPUUtilization')
        for r,u in prof[at['~profile']].items():
            if u < 1.0 and u <= random.random(): continue
            ec2, s = session.resource('ec2', region_name=r), a+':'+r
            for i in ec2.instances.all():
                pipe(s, {'id':      i.id,
                         'acct':    a,
                         'type':    i.instance_type,
                         'plat':    '' if not i.platform else i.platform,
                         'vol':     str(len(i.block_device_mappings)),
                         'az':      i.placement.get('AvailabilityZone',r),
                         'vpc':     '' if not i.vpc_id else i.vpc_id,
                         'ami':     '' if not i.image_id else i.image_id,
                         'state':   i.state.get('Name',''),
                         'spot':    '' if not i.spot_instance_request_id else i.spot_instance_request_id,
                         'tag':     '' if not i.tags else '\t'.join([s.translate(flt)
                                    for kv in tagf(at, i.tags).items() for s in kv]),
                         'metric':  '' if not metrics else '\t'.join([s for m,ts,f in [
                                        ('cpu', [p['ExtendedStatistics']['p90'] for p in cpu.get_statistics(Dimensions=[
                                         {'Name':'InstanceId','Value':i.id},
                                         ], EndTime=now, StartTime=ago, Period=per, ExtendedStatistics=['p90']).get('Datapoints',[])],
                                         lambda v:round(max(v),1)),
                                    ] if ts for s in [m, str(f(ts))]]),
                        })
    pipe(None, None)

def gophEBSAWS(model, settings, inputs, args):
    '''Fetch EC2 instance detail from AWS'''
    if not settings.get('AWS'): raise GError('no AWS settings for {}'.format(model))
    metrics,tagf,pipe,flt,prof,sts = model.split('/',1)[-1]=='metrics', getTagFilter(settings), getWriter(model, [
        'id','acct','type','size','iops','az','state','mount','tag','metric',
    ]), str.maketrans('\t',' '), settings['AWS']['Profiles'], boto3.client('sts')
    if metrics:
        per,now = KVP['metricsperS'], datetime.utcnow(); ago = now-timedelta(minutes=KVP['metricsrngM'])

    for a,at in settings['AWS']['Accounts'].items():
        if not at.get('~profile') or not prof.get(at['~profile']): continue
        if at.get('~arn'):
            cred = sts.assume_role(RoleArn=at['~arn'], RoleSessionName=a)['Credentials']
            session =   boto3.Session(aws_access_key_id=cred['AccessKeyId'],
                                      aws_secret_access_key=cred['SecretAccessKey'],
                                      aws_session_token=cred['SessionToken'])
        else: session = boto3.Session(profile_name=a)
        if metrics:
            cw = session.resource('cloudwatch')
            idle,ioq = cw.Metric('AWS/EBS','VolumeIdleTime'), cw.Metric('AWS/EBS','VolumeQueueLength')
        for r,u in prof[at['~profile']].items():
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
                         'tag':     '' if not v.tags else '\t'.join([s.translate(flt)
                                    for kv in tagf(at, v.tags).items() for s in kv]),
                         'metric':  '' if not metrics else '\t'.join([s for m,ts,f in [
                                        ('idle', [p['Sum'] for p in idle.get_statistics(Dimensions=[
                                         {'Name':'VolumeId','Value':v.id},
                                         ], EndTime=now, StartTime=ago, Period=per, Statistics=['Sum']).get('Datapoints',[])],
                                         lambda v:round(min(v)*100.0/per,1)),
                                        ('ioq', [p['ExtendedStatistics']['p90'] for p in ioq.get_statistics(Dimensions=[
                                         {'Name':'VolumeId','Value':v.id},
                                         ], EndTime=now, StartTime=ago, Period=per, ExtendedStatistics=['p90']).get('Datapoints',[])],
                                         lambda v:round(max(v),1)),
                                    ] if ts for s in [m, str(f(ts))]]),
                        })
    pipe(None, None)

def gophRDSAWS(model, settings, inputs, args):
    '''Fetch RDS database detail from AWS'''
    if not settings.get('AWS'): raise GError('no AWS settings for {}'.format(model))
    metrics,tagf,pipe,flt,prof,sts = model.split('/',1)[-1]=='metrics', getTagFilter(settings), getWriter(model, [
        'id','acct','type','stype','size','iops','engine','ver','lic','az','multiaz','vpc','state','tag','metric',
    ]), str.maketrans('\t',' '), settings['AWS']['Profiles'], boto3.client('sts')
    if metrics:
        per,now = KVP['metricsperS'], datetime.utcnow(); ago = now-timedelta(minutes=KVP['metricsrngM'])

    for a,at in settings['AWS']['Accounts'].items():
        if not at.get('~profile') or not prof.get(at['~profile']): continue
        if at.get('~arn'):
            cred = sts.assume_role(RoleArn=at['~arn'], RoleSessionName=a)['Credentials']
            session =   boto3.Session(aws_access_key_id=cred['AccessKeyId'],
                                      aws_secret_access_key=cred['SecretAccessKey'],
                                      aws_session_token=cred['SessionToken'])
        else: session = boto3.Session(profile_name=a)
        if metrics:
            cw = session.resource('cloudwatch')
            conn,cpu,ioq = cw.Metric('AWS/RDS','DatabaseConnections'), cw.Metric('AWS/RDS','CPUUtilization'), cw.Metric('AWS/RDS','DiskQueueDepth')
        for r,u in prof[at['~profile']].items():
            if u < 1.0 and u <= random.random(): continue
            rds, s = session.client('rds', region_name=r), a+':'+r
            for d in rds.describe_db_instances().get('DBInstances',[]):
                arn = d['DBInstanceArn']
                if metrics: dim = [{'Name':'DBInstanceIdentifier','Value':d['DBInstanceIdentifier']}]
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
                         'vpc':     d.get('DBSubnetGroup',{}).get('VpcId',''),
                         'state':   d.get('DBInstanceStatus',''),
                         'tag':     '' if not dtags else '\t'.join([s.translate(flt)
                                    for kv in tagf(at, dtags).items() for s in kv]),
                         'metric':  '' if not metrics else '\t'.join([s for m,ts,f in [
                                    #   ('conn', [p['Average'] for p in conn.get_statistics(Dimensions=dim,
                                    #    EndTime=now, StartTime=ago, Period=per, Statistics=['Average']).get('Datapoints',[])],
                                    #    lambda v:round(max(v),2)),
                                        ('conn', [p['ExtendedStatistics']['p90'] for p in conn.get_statistics(Dimensions=dim,
                                         EndTime=now, StartTime=ago, Period=per, ExtendedStatistics=['p90']).get('Datapoints',[])],
                                         lambda v:round(max(v),2)),
                                        ('cpu', [p['ExtendedStatistics']['p90'] for p in cpu.get_statistics(Dimensions=dim,
                                         EndTime=now, StartTime=ago, Period=per, ExtendedStatistics=['p90']).get('Datapoints',[])],
                                         lambda v:round(max(v),1)),
                                        ('ioq', [p['ExtendedStatistics']['p90'] for p in ioq.get_statistics(Dimensions=dim,
                                         EndTime=now, StartTime=ago, Period=per, ExtendedStatistics=['p90']).get('Datapoints',[])],
                                         lambda v:round(max(v),2)),
                                    ] if ts for s in [m, str(f(ts))]]),
                        })
    pipe(None, None)

def gophSNAPAWS(model, settings, inputs, args):
    '''Fetch EBS snapshot detail from AWS'''
    if not settings.get('AWS'): raise GError('no AWS settings for {}'.format(model))
    tagf,pipe,flt,prof,cfg,sts = getTagFilter(settings), getWriter(model, [
        'id','acct','type','vsiz','reg','vol','desc','tag','since',
    ]), str.maketrans('\t',' '), settings['AWS']['Profiles'], botocore.config.Config(read_timeout=300), boto3.client('sts')

    for a,at in settings['AWS']['Accounts'].items():
        if not at.get('~profile') or not prof.get(at['~profile']): continue
        if at.get('~arn'):
            cred = sts.assume_role(RoleArn=at['~arn'], RoleSessionName=a)['Credentials']
            session =   boto3.Session(aws_access_key_id=cred['AccessKeyId'],
                                      aws_secret_access_key=cred['SecretAccessKey'],
                                      aws_session_token=cred['SessionToken'])
        else: session = boto3.Session(profile_name=a)
        for r,u in prof[at['~profile']].items():
            if u < 1.0 and u <= random.random(): continue
            ec2, s = session.client('ec2', region_name=r, config=cfg), a+':'+r
            for page in ec2.get_paginator('describe_snapshots').paginate(OwnerIds=[a]):
                for snap in page['Snapshots']:
                    if snap.get('State') != 'completed': continue
                    pipe(s, {'id':      snap.get('SnapshotId',''),
                             'acct':    a,
                             'type':    snap.get('StorageTier','standard'),
                             'vsiz':    str(snap.get('VolumeSize',0)),
                             'reg':     r,
                             'vol':     snap.get('VolumeId','vol-ffffffff'),
                             'desc':    snap.get('Description',''),
                             'tag':     '' if not snap.get('Tags') else '\t'.join([s.translate(flt)
                                        for kv in tagf(at, snap['Tags']).items() for s in kv]),
                             'since':   snap['StartTime'].isoformat(),
                            })
    pipe(None, None)

def gophCURAWS(model, settings, inputs, args):
    '''Fetch CUR (Cost & Usage Report) line item detail from AWS'''
    if not settings.get('BinDir'): raise GError('no bin directory for {}'.format(model))
    if not settings.get('AWS',{}).get('CUR'): raise GError('no CUR settings for {}'.format(model))
    tlist = ['cmon:Name','cmon:Env','cmon:Cust','cmon:Oper','cmon:Prod','cmon:Role','cmon:Ver','cmon:Prov',]
    cur,accts,rules,edp,pipe,flt,head,ids,s = settings['AWS']['CUR'], settings['AWS'].get('Accounts',{}), settings[
        'AWS'].get('TagRules',{}), settings['AWS'].get('EDPAdj',1.0), getWriter(model, [
        'id','hour','usg','cost','acct','typ','svc','utyp','uop','reg','rid','desc','ivl',
    ]+tlist), str.maketrans('\t',' '), {}, {}, ""

    def getcid(id):
        '''Return cached compact line item ID with new-reference flag; full IDs unnecessarily large'''
        cid = id[-9:]; fid = ids.get(cid)
        if fid is None:
            ids[cid] = id;  return cid, True
        elif fid == id:     return cid, False
        elif id in ids:     return id,  False
        ids[id] = '';       return id,  True
    def getcol(hl, ex, hm, c, dc=-1):
        '''Return first non-ex column value from heading list hl as offset by hm'''
        for h in hl:
            o = hm.get(h, -1)
            if o >= 0 and c[o] not in ex: return c[o]
        return c[dc]

    with subprocess.Popen([settings.get('BinDir').rstrip('/')+'/goph_curaws.sh',
            cur.get('account','default'), cur.get('bucket','cost-reporting/CUR/hourly'), cur.get('label','hourly')],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True) as p:
        for l in p.stdout:
            if l.startswith('identity/LineItemId,'):                            # https://docs.aws.amazon.com/cur/latest/userguide/data-dictionary.html
                head = {h:i for i,h in enumerate(l[:-1].split(','))}

            elif not l.startswith('#!'):
                for col in csv.reader([l]): break
                if len(col) != len(head): continue
                col.append('')                                                  # default value for missing columns
                typ = { 'Usage':                                'usage',
                        'DiscountedUsage':                      'RI usage',
                        'RIFee':                                'RI unused',
                        'Fee':                                  'fee',
                        'SavingsPlanUpfrontFee':                'fee',
                        'SavingsPlanCoveredUsage':              'SP usage',
                        'SavingsPlanRecurringFee':              'SP unused',
                        'Refund':                               'EDP/refund',
                        'EdpDiscount':                          'EDP/refund',
                        'BundledDiscount':                      'EDP/refund',
                        'Credit':                               'CSC/WMP/credit',
                        'Tax':                                  'tax',
                        'SavingsPlanNegation':                  'skip',
                      }.get(col[head['lineItem/LineItemType']], 'unknown')
                if typ == 'skip': continue
                id,new = getcid(col[0]); hour = col[head['lineItem/UsageStartDate']]; rec = {
                    'id':       id,                                             # compact line item ID
                    'hour':     hour,                                           # GMT timestamp (YYYY-MM-DDThh:mm:ssZ)
                    'usg':      col[head['lineItem/UsageAmount']],              # default usage quantity
                }
                if   typ == 'RI usage': rec['cost'] = col[head['reservation/EffectiveCost']]
                elif typ == 'RI unused':
                    try:    rec['usg'], rec['cost'] = col[head['reservation/UnusedQuantity']], str(float(
                                                      col[head['reservation/UnusedAmortizedUpfrontFeeForBillingPeriod']])+float(
                                                      col[head['reservation/UnusedRecurringFee']]))
                    except ValueError: continue
                elif typ == 'SP usage': rec['cost'] = col[head['savingsPlan/SavingsPlanEffectiveCost']]
                elif typ == 'SP unused':
                    try:                rec['cost'] = str(float(
                                                      col[head['savingsPlan/TotalCommitmentToDate']])-float(
                                                      col[head['savingsPlan/UsedCommitment']]))
                    except ValueError: continue
                else:                   rec['cost'] = col[head['lineItem/UnblendedCost']] if (
                                                      col[head['lineItem/LineItemDescription']]!='Enterprise Program Discount' or
                                                      edp==1.0) else str(float(
                                                      col[head['lineItem/UnblendedCost']])*edp)

                if new:
                    acct,svc,uop,az,rid,end=\
                        col[head['lineItem/UsageAccountId']],col[head['product/ProductName']],  col[head['lineItem/Operation']],\
                        col[head['product/region']],        col[head['lineItem/ResourceId']],   col[head['lineItem/UsageEndDate']]
                    try:    ivl = int(timedelta.total_seconds(datetime.fromisoformat(end[:-1])-datetime.fromisoformat(hour[:-1])))
                    except  ValueError: continue

                    rec.update({
                        'acct': acct,                                           # usage (not billing) account
                        'typ': {'AWS':                  '',
                                'AWS Marketplace':      'mkt ',                 # source
                               }.get(col[head['bill/BillingEntity']],'other ') +
                               (''          if ivl < 3602       else (
                                'daily '    if ivl < 86402      else (
                                'monthly '  if ivl < 2678402    else (
                                'periodic ' if ivl < 31103999   else
                                'annual ')))) +                                 # usage interval category
                               typ,                                             # line item type
                        'svc': {'Amazon Elastic Compute Cloud':                         'EC2',
                                'Amazon Simple Storage Service':                        'S3',
                                'Amazon Simple Notification Service':                   'SNS',
                                'Amazon Elastic Container Service':                     'ECS',
                                'Amazon Elastic Container Service for Kubernetes':      'EKS',
                                'Elastic Load Balancing':                               'ELB',
                                'AmazonCloudWatch':                                     'CloudWatch',
                                'Amazon Virtual Private Cloud':                         'VPC',
                                'AWS Key Management Service':                           'KMS',
                                'Amazon Simple Queue Service':                          'SQS',
                                'Amazon Relational Database Service':                   'RDS',
                                'Amazon EC2 Container Registry (ECR)':                  'ECR',
                                'Amazon Elastic File System':                           'EFS',
                                'CloudEndure Disaster Recovery to AWS':                 'CloudEndure',
                                'Repstance Advanced Edition':                           'Repstance AE',
                                'Contact Center Telecommunications (service sold by AMCS, LLC) ':'Amazon Connect telecom',
                               }.get(svc,svc.replace(
                                'with support by',      'by'    ).replace(
                                ' supported by',        'by'    ).replace(
                                'Enterprise Linux',     'Linux' ).replace(
                                'Amazon ',              ''      ).replace(
                                'AWS ',                 ''      )),             # service name
                        'utyp': col[head['lineItem/UsageType']],                # usage detail
                        'uop':  '' if uop in {'Any','any','ANY','Nil','nil','None','none','Null','null',
                                              'NoOperation','Not Applicable','N/A','n/a','Unknown','unknown',
                                             } else uop,                        # usage operation
                        'reg':  {'us-east-1':           'USE1', 'ap-east-1':            'APE1',
                                 'us-east-2':           'USE2', 'ap-northeast-1':       'APN1',
                                 'us-west-1':           'USW1', 'ap-northeast-2':       'APN2',
                                 'us-west-2':           'USW2', 'ap-northeast-3':       'APN3',
                                 '':                    'none', 'ap-southeast-1':       'APS1',
                                                                'ap-southeast-2':       'APS2',
                                 'eu-central-1':        'EUC1', 'ap-southeast-3':       'APS4',
                                 'eu-north-1':          'EUN1', 'ap-south-1':           'APS3',
                                 'eu-south-1':          'EUS1', 'ca-central-1':         'CAN1',
                                 'eu-west-1':           'EUW1', 'sa-east-1':            'SAE1',
                                 'eu-west-2':           'EUW2', 'af-south-1':           'CPT',
                                 'eu-west-3':           'EUW3', 'me-south-1':           'MES1',
                                }.get(az,az),                                   # service region
                        'rid':  rid.split(':', 5)[-1]   if      rid.startswith('arn:')
                                                        else    rid,            # resource ID (i-, vol-, ...)
                        'desc': col[head['lineItem/LineItemDescription']].replace(
                                'USD',                  '$'     ).replace(
                                '$ ',                   '$'     ).replace(
                                '$0.0000 ',             '$0 '   ).replace(
                                '$0.000 ',              '$0 '   ).replace(
                                '$0.00 ',               '$0 '   ).replace(
                                '$0.0 ',                '$0 '   ).replace(
                                '$$',                   '$'     ).replace(
                                ' per ',                '/'     ).replace(
                                '  ',                   ' '     ).replace(
                                ' - ',                  '; '    ).replace(
                                ' / month',             '/mo'   ).replace(
                                'onthly',               'o'     ).replace(
                                'onth',                 'o'     ).replace(
                                '/hour',                '/hr'   ).replace(
                                ' hourly fee',          '/hr'   ).replace(
                                'hourly',               '/hr'   ).replace(
                                'Hourly',               '/hr'   ).replace(
                                'or partial hour',     'or part').replace(
                                ' hour',                ' hr'   ).replace(
                                ' Hour',                ' hr'   ).replace(
                                '-hour',                '-hr'   ).replace(
                                '-Hour',                '-hr'   ).replace(
                                ' reserved instance ',  ' RI '  ).replace(
                                ' instance',            ' inst' ).replace(
                                ' Instance ',           ' inst ').replace(
                                ' Instance-',           ' inst-').replace(
                                ' request',             ' req'  ).replace(
                                'Requests/',            'req/'  ).replace(
                               #' Request',             ' req'  ).replace(
                                ' million',             'M'     ).replace(
                                '/million',             '/M'    ).replace(
                                'On Demand',            'OD'    ).replace(
                                'rovisioned IOPS',      'IOPS'  ).replace(
                                'rovisioned',           'rov'   ).replace(
                                ' storage',             ' sto'  ).replace(
                                ' capacity',            ' cap'  ).replace(
                                ' in VPC Zone',      ' VPC Zone').replace(
                                ' Outbound',            ' out'  ).replace(
                                'IP address',           'IP'    ).replace(
                                'Linux/UNIX',           'Linux' ).replace(
                                'PostgreSQL',           'PSQL'  ).replace(
                                'SQL Server',           'SQL'   ).replace(
                                'SQL Standard',         'SQL SE').replace(
                                'SQL Std',              'SQL SE').replace(
                                'SQL Enterprise',       'SQL EE').replace(
                                'SQL Express',          'SQL EX').replace(
                                '(Amazon VPC)',         'VPC'   ).replace(
                                'transfer',             'xfer'  ).replace(
                                'thereafter',           'after' ).replace(
                                'us-east-1',            'USE1'  ).replace(
                                'us-east-2',            'USE2'  ).replace(
                                'us-west-2',            'USW2'  ).replace(
                                'eu-central-1',         'EUC1'  ).replace(
                                'eu-west-1',            'EUW1'  ).replace(
                                'eu-west-2',            'EUW2'  ).replace(
                                'Virginia',             'VA'    ).replace(
                                'Ohio',                 'OH'    ).replace(
                                'Oregon',               'OR'    ).replace(
                                'California',           'CA'    ).replace(
                                'Asia Pacific',         'APAC'  ).replace(
                                'Northern ',            ''      ).replace(
                                'N. ',                  ''      ).replace(
                                'N.',                   ''      ),              # service description
                        'ivl':  str(ivl),                                       # usage interval (seconds)
                    })                                                          # cmon tags or mappings...
                    rs = rules.get(accts.get(acct,{}).get('~tagrules','default'),{})
                    rec.update({t:getcol(['resourceTags/user:'+a for a in [t]+rs.get(t,{}).get('~aliases',[])],
                                         # translate as with API-sourced tags for sake of potential matching
                                         {'','--','unknown','Unknown'}, head, col).translate(flt) for t in tlist})
                pipe(s, rec)

            elif l.startswith('#!begin '):
                ps,s = s, l[:-1].partition(' ')[2].partition('~link')[0]
                if ps and not s.startswith(ps[:6]): ids = {}
        pipe(None, None)

def main():
    '''Parse command line args and release the gopher'''
    gophModels = {                      # gopher model map
        'ec2.aws':                      [gophEC2AWS,            'fetch EC2 instances from AWS'],
        'ec2.aws/metrics':              [gophEC2AWS,            'fetch EC2 instances from AWS with metrics'],
        'ebs.aws':                      [gophEBSAWS,            'fetch EBS volumes from AWS'],
        'ebs.aws/metrics':              [gophEBSAWS,            'fetch EBS volumes from AWS with metrics'],
        'rds.aws':                      [gophRDSAWS,            'fetch RDS databases from AWS'],
        'rds.aws/metrics':              [gophRDSAWS,            'fetch RDS databases from AWS with metrics'],
        'snap.aws':                     [gophSNAPAWS,           'fetch EBS snapshots from AWS'],
        'cur.aws':                      [gophCURAWS,            'fetch CUR line items from AWS'],
    }
                                        # define and parse command line parameters
    parser = argparse.ArgumentParser(description='''This gopher agent fetches Cloud Monitor content for an AWS model''')
    parser.add_argument('model',        choices=gophModels, metavar='model',
                        help='''AWS model; {} are supported'''.format(', '.join(gophModels)))
    parser.add_argument('-o','--opt',   action='append', metavar='option', default=[],
                        help='''gopher option''')
    parser.add_argument('-k','--key',   action='append', metavar='kvp', default=[],
                        help='''key-value pair of the form <k>=<v> (key one of: {})'''.format(
                             ', '.join(['{} [{}]'.format(k, KVP[k]) for k in KVP
                             if not k.startswith('_')])))
    args = parser.parse_args()

    try:                                # release the gopher!
        signal.signal(signal.SIGTERM, terminate)
        overrideKVP({k.partition('=')[0].strip():k.partition('=')[2].strip() for k in args.key})
        settings,inputs = json.loads(sys.stdin.readline().strip()) if KVP['settings'] == '~stdin' else json.load(open(KVP['settings'], 'r')), []
        for line in sys.stdin:
            inputs.append(json.loads(line.strip()))

        gophModels[args.model][0](args.model, settings, inputs, args)
                                        # handle exceptions; broken pipe exit avoids console errors
    except  json.JSONDecodeError:       ex('\n** invalid JSON input **\n\n', 1)
    except  FileNotFoundError:          ex('\n** settings not found **\n\n', 1)
    except  BrokenPipeError:            os._exit(0)
    except  KeyboardInterrupt:          ex('\n** gopher interrupted **\n\n', 10)
    except (AssertionError, RuntimeError, AttributeError, KeyError, TypeError, IndexError, IOError,
            ProfileNotFound, ClientError, EndpointConnectionError, ConnectionClosedError,
            GError) as e:               ex('\n** {} **\n\n'.format(e if e else 'unknown exception'), 10)

if __name__ == '__main__':  main()      # called as gopher
else:                       pass        # loaded as module
#!/usr/bin/python3

# K8s notes
# ...

import  sys
import  os
import  argparse
from    datetime                import  datetime,timedelta
import  random
import  signal
import  json
import  subprocess

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

def gophXXXK8s(m, cmon, args):
    if not cmon.get('K8s'): raise GError('no K8s configuration for {}'.format(m))
    if not cmon.get('BinDir'): raise GError('no bin directory for {}'.format(m))
    csv, s = getWriter(m, ['id','type']), ""

    # TODO: replace stub
    with subprocess.Popen([cmon.get('BinDir').rstrip('/')+'/goph_xxxk8s.sh'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True) as p:
        for l in p.stdout:
            if not l.startswith('#!'):
                col = l.split(',', 10)
                if len(col) <= 10: continue

                csv(s, {'id':       col[0],     # line item ID
                        'type':     col[9],     # line item type
                        })
            elif l.startswith('#!begin '):
                s = l[:-1].partition(' ')[2].partition('~link')[0]
        csv(None, None)

def main():
    '''Parse command line args and run gopher command'''
    gophModels = {                      # gopher model map
        'xxx.K8s':      [gophXXXK8s,    'fetch ... from K8s'],
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
            GError) as e:               ex('** {} **\n'.format(e if e else 'unknown exception'), 10)

if __name__ == '__main__':  main()      # called as script
else:                       pass        # loaded as module
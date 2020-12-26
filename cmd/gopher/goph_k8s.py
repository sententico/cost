#!/usr/bin/python3

# K8s notes
# ...

import  sys
import  os
import  signal
import  argparse
import  json
from    datetime                import  datetime,timedelta
#import  subprocess
#import  awslib.patterns         as      aws

class GError(Exception):
    '''Module exception'''
    pass

KVP={                                   # key-value pair defaults/validators (overridden by command-line)
    'settings':     '~stdin',
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
    raise KeyboardInterrupt
def ex(err, code):
    if err: sys.stderr.write(err)
    sys.exit(code)

def getWriter(m, cols):
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

def gophXXXK8s(model, settings, inputs, args):
    if not settings.get('K8s'): raise GError('no Kubernetes settings for {}'.format(model))
    pipe,s = getWriter(model, [
        'id','status',
    ]), ''
    pipe(s, {
        'id':       'gopher',
        'status':   'great',
    })
    pipe(s, {
        'id':       'weasel',
        'status':   'super',
    })
    pipe(None, None)

def main():
    '''Parse command line args and release the gopher'''
    gophModels = {                      # gopher model map
        'xxx.k8s':      [gophXXXK8s,    'fetch ... from Kubernetes'],
    }
                                        # define and parse command line parameters
    parser = argparse.ArgumentParser(description='''This gopher agent fetches Cloud Monitor content for a Kubernetes model''')
    parser.add_argument('model',        choices=gophModels, metavar='model',
                        help='''Kubernetes model; {} are supported'''.format(', '.join(gophModels)))
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
            GError) as e:               ex('\n** {} **\n\n'.format(e if e else 'unknown exception'), 10)

if __name__ == '__main__':  main()      # called as gopher
else:                       pass        # loaded as module
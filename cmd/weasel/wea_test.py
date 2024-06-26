#!/usr/bin/python3

import  sys
import  os
import  signal
import  argparse
import  json
#from    datetime                import  datetime,timedelta
#import  subprocess
#import  awslib.patterns         as      aws

class WError(Exception):
    '''Module exception'''
    pass

KVP={                                   # key-value pair defaults/validators (overridden by command-line)
    'settings':     '~stdin',
    }

def overrideKVP(overrides):
    '''Override KVP dictionary defaults with command-line inputs'''
    for k,v in overrides.items():
        dfv,meta = KVP.get(k), KVP.get('_' + k)
        if k.startswith('_') or dfv is None: raise WError('{} key unrecognized'.format(k))
        if v in {'', '?'}:
            if not meta or len(meta) < 3:
                raise WError('specify value for {}; default is {}'.format(k, KVP[k]))
            raise WError('specify value in [{},{}] range for {}; default is {}'.format(
                         meta[1], meta[2], k, KVP[k]))
        if meta:
            try:
                if len(meta) == 1:      # apply validator function to value
                    v = meta[0](k, v)
                    if v is None: raise WError('validation failed')
                elif len(meta) >= 3:    # convert string value to desired type with bounds check
                    try:    ev = eval(v) if type(v) is str else v
                    except  KeyboardInterrupt: raise
                    except: raise WError('invalid value')
                    v = meta[0](ev)
                    if   v < meta[1]: v = meta[1]
                    elif v > meta[2]: v = meta[2]
            except (ValueError, WError) as e:
                raise WError('{} cannot be set ({})'.format(k, e))
        KVP[k] = v

def terminate(sig, frame):
    raise KeyboardInterrupt
def ex(err, code):
    if err: sys.stderr.write(err)
    sys.exit(code)

def weaUPTEST(service, settings, args):
    #if not settings.get('test'): raise WError('no test settings for {}'.format(service))
    for line in sys.stdin:
        obj = json.loads(line.strip())
        sys.stdout.write('{}\n'.format(json.dumps([obj[0].upper()])))

def main():
    '''Parse command line args and release the weasel'''
    weaServices = {                     # weasel service map
        'up.test':      [weaUPTEST,     'deliver first string in list in uppercase'],
    }
                                        # define and parse command line parameters
    parser = argparse.ArgumentParser(description='''This weasel agent delivers Cloud Monitor content to a test service''')
    parser.add_argument('service',      choices=weaServices, metavar='service',
                        help='''test service; {} are supported'''.format(', '.join(weaServices)))
    parser.add_argument('-o','--opt',   action='append', metavar='option', default=[],
                        help='''weasel option''')
    parser.add_argument('-k','--key',   action='append', metavar='kvp', default=[],
                        help='''key-value pair of the form <k>=<v> (key one of: {})'''.format(
                             ', '.join(['{} [{}]'.format(k, KVP[k]) for k in KVP
                             if not k.startswith('_')])))
    args = parser.parse_args()

    try:                                # release the weasel!
        signal.signal(signal.SIGTERM, terminate)
        overrideKVP({k.partition('=')[0].strip():k.partition('=')[2].strip() for k in args.key})
        settings = json.loads(sys.stdin.readline().strip()) if KVP['settings'] == '~stdin' else json.load(open(KVP['settings'], 'r'))

        weaServices[args.service][0](args.service, settings, args)
                                        # handle exceptions; broken pipe exit avoids console errors
    except  json.JSONDecodeError:       ex('\n** invalid JSON input **\n\n', 1)
    except  FileNotFoundError:          ex('\n** settings not found **\n\n', 1)
    except  BrokenPipeError:            os._exit(0)
    except  KeyboardInterrupt:          ex('\n** weasel interrupted **\n\n', 10)
    except (AssertionError, RuntimeError, AttributeError, KeyError, TypeError, IndexError, IOError,
            WError) as e:               ex('\n** {} **\n\n'.format(e if e else 'unknown exception'), 10)

if __name__ == '__main__':  main()      # called as weasel
else:                       pass        # loaded as module
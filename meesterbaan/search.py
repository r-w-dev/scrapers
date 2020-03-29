from sys import argv

from meesterbaan.utils import Duo

if __name__ == "__main__":
    print(f'Searching school... \t\t ({", ".join(argv[1:])})\n')

    kwargs = {}
    for arg in argv[1:]:
        key, val = arg.split('=')
        kwargs[key] = val

    duo = Duo()
    res = duo.find_school(**kwargs)
    print(res)

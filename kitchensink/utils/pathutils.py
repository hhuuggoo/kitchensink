from os.path import join, dirname, split, realpath, exists, abspath
import imp
import posixpath
from serializations import jsload

def urlsplit(path, basepath, maxdepth=10):
    """splits /home/hugo/foo/bar/baz into foo, bar, baz, assuming
    /home/hugo is the basepath
    """
    if maxdepth == 0:
        return []
    if path == basepath:
        return []
    else:
        urlpath, path = posixpath.split(path)
        results = urlsplit(urlpath, basepath, maxdepth=maxdepth - 1)
        results.append(path)
        return results

def _dirsplit(path, basepath, maxdepth=10):
    if maxdepth == 0:
        return []
    if path == basepath:
        return []
    else:
        dirpath, path = split(path)
        if path == "":
            return []
        else:
            results = dirsplit(dirpath, basepath, maxdepth=maxdepth - 1)
            results.append(path)
            return results

def dirsplit(path, basepath, maxdepth=10):
    """splits /home/hugo/foo/bar/baz into foo, bar, baz, assuming
    /home/hugo is the basepath
    """
    path = realpath(path)
    basepath = realpath(basepath)
    return _dirsplit(path, basepath, maxdepth=maxdepth)

def urljoin(*paths):
    return posixpath.join(*paths)

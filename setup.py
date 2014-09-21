# needs to be tested
from __future__ import print_function
import sys
if len(sys.argv)>1 and sys.argv[1] == 'develop':
    # Only import setuptools if we have to
    import site
    from os.path import dirname, abspath, join
    site_packages = site.getsitepackages()[0]
    fname = join(site_packages, "kitchensink.pth")
    path = abspath(dirname(__file__))
    with open(fname, "w+") as f:
        f.write(path)
    print("develop mode, wrote path (%s) to (%s)" % (path, fname))
    sys.exit()
from distutils.core import setup
import os
import sys
__version__ = (0, 2)
setup(
    name = 'kitchensink',
    version = '.'.join([str(x) for x in __version__]),
    packages = ['kitchensink',
                'kitchensink.admin',
                'kitchensink.api',
                'kitchensink.clients',
                'kitchensink.data',
                'kitchensink.rpc',
                'kitchensink.scripts',
                'kitchensink.serialization',
                'kitchensink.taskqueue',
                'kitchensink.testutils',
                'kitchensink.utils',
                ],

    url = 'http://github.com/hhuuggoo/kitchensink',
    description = 'Kitchen Sink',
    zip_safe=False,
    license = 'New BSD',
)

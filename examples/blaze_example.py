import kitchensink.settings as settings
from kitchensink.data import du, do, dp
from blaze import Table, compute
settings.setup_client('http://localhost:6323/')
c = settings.client()
import pandas as pd
#df = pd.DataFrame({'a' : [1,2,3,4,5], 'b' : [2,3,4,5,6]})
#do(df).save(url='temp1')
obj = du('temp1')
from blaze.compute.kitchensink import SingleBlazeKitchensinkArray
obj = SingleBlazeKitchensinkArray(obj)
t = Table(obj)
print t.dshape
print compute(t['a'].sum())

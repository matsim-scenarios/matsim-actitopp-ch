import pandas as pd, numpy as np
idx = pd.date_range('1/1/2000', periods=1000)
df  = pd.DataFrame(np.random.randn(1000, 4), index=idx, columns=list('ABCD')).cumsum()

import hvplot.pandas  # noqa
df.hvplot()
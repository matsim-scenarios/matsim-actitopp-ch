#!/usr/bin/env python
# coding: utf-8

# # Trip CSV Summary Notebook

# In[8]:


# Trip CSV Summary Notebook
# @author Billy Charlton <charlton@vsp.tu-berlin.de>

import pandas as pd
import panel as pn
import numpy as np
from dfply import *
from bokeh.io import output_notebook, show
from bokeh.plotting import figure
from bokeh.models import BasicTickFormatter

# this directs all output graphics to the notebook itself instead of PNG files/etc
output_notebook()


# In[10]:


# Read CSV and calculate some useful columns we'll need
trips = pd.read_csv('output/eth-trips.csv', comment='#')

trips >>= mutate(
     duration = X.finishTime - X.time,
     clock_time_hours = X.time / 3600,
     clock_time_minutes = (X.time / 60).astype('int')
)


# ### Summary stats

# In[15]:


# Some basic summary values
print('Number of activities: ', len(trips))
print('Number of people: ', trips.personId.value_counts().size)

print('\nDistance metrics:')
trips['duration'].describe()


# In[9]:


p = figure(title="Trips by Time of Day", plot_width=800, plot_height=400)
p.xgrid.grid_line_color = None
p.xaxis.axis_label = "Time"
p.xaxis.major_label_orientation = 1.2
p.xaxis.formatter = BasicTickFormatter(use_scientific=False)
p.yaxis.formatter = BasicTickFormatter(use_scientific=False)

p.circle(trips.clock_time_hours, trips.distance, size=2, fill_alpha=0.6)

show(p)


# ## Start location of trips

# In[5]:


from bokeh.palettes import Viridis256
from bokeh.util.hex import hexbin

bins = hexbin(trips.x, trips.y, 0.01)

# color map the bins by hand, will see how to use linear_cmap later
color = [Viridis256[int(i)] for i in bins.counts/max(bins.counts)*255]

# match_aspect ensures neither dimension is squished, regardless of the plot size
p = figure(width=800, tools="wheel_zoom,reset", match_aspect=True, background_fill_color='#111111')
p.grid.visible = False

p.hex_tile(bins.q, bins.r, size=0.1, line_color=None, fill_color=color)

show(p)


# In[6]:


import pandas as pd
import numpy as np
from bokeh.plotting import figure
from bokeh.io import output_notebook, show, output_file
from bokeh.models import ColumnDataSource, HoverTool, Panel
from bokeh.models.widgets import Tabs

def hist_hover(dataframe, column, colors=["SteelBlue", "Tan"], bins=30, log_scale=False, show_plot=True):

    # build histogram data with Numpy
    hist, edges = np.histogram(dataframe[column], bins = bins)
    hist_df = pd.DataFrame({column: hist,
                             "left": edges[:-1],
                             "right": edges[1:]})
    hist_df["interval"] = ["%d to %d" % (left, right) for left, 
                           right in zip(hist_df["left"], hist_df["right"])]

    # bokeh histogram with hover tool
    if log_scale == True:
        hist_df["log"] = np.log(hist_df[column])
        src = ColumnDataSource(hist_df)
        plot = figure(plot_height = 600, plot_width = 600,
              title = "Histogram of {}".format(column.capitalize()),
              x_axis_label = column.capitalize(),
              y_axis_label = "Log Count")    
        plot.quad(bottom = 0, top = "log",left = "left", 
            right = "right", source = src, fill_color = colors[0], 
            line_color = "black", fill_alpha = 0.7,
            hover_fill_alpha = 1.0, hover_fill_color = colors[1])
    else:
        src = ColumnDataSource(hist_df)
        plot = figure(plot_height = 600, plot_width = 600,
              title = "Histogram of {}".format(column.capitalize()),
              x_axis_label = column.capitalize(),
              y_axis_label = "Count")    
        plot.quad(bottom = 0, top = column,left = "left", 
            right = "right", source = src, fill_color = colors[0], 
            line_color = "black", fill_alpha = 0.7,
            hover_fill_alpha = 1.0, hover_fill_color = colors[1])
    # hover tool
    hover = HoverTool(tooltips = [('Interval', '@interval'),
                              ('Count', str("@" + column))])
    plot.add_tools(hover)
    # output
    if show_plot == True:
        show(plot)
    else:
        return plot


# In[7]:


from bokeh.plotting import figure, show, output_file
hist, edges = np.histogram(trips, density=True, bins=100)

p1 = figure(title="Normal Distribution (μ=0, σ=0.5)",tools="save",
            background_fill_color="#E8DDCB")

hist_hover(trips, 'distance', bins=5)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





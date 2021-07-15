#!/usr/bin/env python
# coding: utf-8

# In[1]:


serie = pd.read_csv("C:/Users/Abidat/Desktop/TP_BDM_ABIDAT_Ikram_ST4/IOT-temp.csv")


# In[2]:


import pandas as pd


# In[5]:


serie = pd.read_csv("C:/Users/Abidat/Desktop/TP_BDM_ABIDAT_Ikram_ST4/IOT-temp.csv", index_col='noted_date', parse_dates=['noted_date'])


# In[10]:


serie.head(20)


# In[11]:


import os


# In[12]:


import warnings


# In[13]:


warnings.filterwarnings('ignore')


# In[14]:


import numpy as np


# In[15]:


import pandas as pd


# In[16]:


import matplotlib.pyplot as plt


# In[17]:


plt.style.use('fivethirtyeight') 


# In[22]:


get_ipython().run_line_magic('matplotlib', 'inline')
from pylab import rcParams
from plotly import tools
import plotly.plotly as py
from plotly.offline import init_notebook_mode, iplot
init_notebook_mode(connected=True)
import plotly.graph_objs as go
import plotly.figure_factory as ff
import statsmodels.api as sm
from numpy.random import normal, seed
from scipy.stats import norm
from statsmodels.tsa.arima_model import ARMA
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_process import ArmaProcess
from statsmodels.tsa.arima_model import ARIMA
import math
from sklearn.metrics import mean_squared_error
print(os.listdir("../input"))


# In[19]:


from plotly import tools


# In[20]:


pip install plotly


# In[21]:


from plotly import tools


# In[23]:


import plotly.plotly as py


# In[24]:


_chart_studio_error("plotly")


# In[25]:


from pylab import rcParams


# In[26]:


from plotly import tools


# In[27]:


import plotly.plotly as py


# In[28]:


from plotly.offline import init_notebook_mode, iplot
init_notebook_mode(connected=True)


# In[29]:


import plotly.graph_objs as go


# In[30]:


import plotly.figure_factory as ff


# In[31]:


import statsmodels.api as sm


# In[ ]:


pip install statsmodels


# In[ ]:


from numpy.random import normal, seed


# In[ ]:


from scipy.stats import norm


# In[ ]:


from statsmodels.tsa.arima_model import ARMA
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_process import ArmaProcess
from statsmodels.tsa.arima_model import ARIMA


# In[ ]:


import math
from sklearn.metrics import mean_squared_error


# In[42]:


serie.head()


# In[43]:


serie = serie.iloc[1:]
serie = serie.fillna(method='ffill')
serie.head()


# In[61]:


import os
import warnings
warnings.filterwarnings('ignore')
import numpy as np 
import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight') 


# In[62]:


get_ipython().run_line_magic('matplotlib', 'inline')


# In[63]:


from pylab import rcParams
from plotly import tools


# In[67]:


pip install plotly


# In[68]:


import plotly.plotly as py


# In[69]:


pip install chart_studio


# In[70]:


import chart_studio.plotly


# In[71]:


from _plotly_future_ import _chart_studio_error


# In[73]:


from plotly.offline import init_notebook_mode, iplot
init_notebook_mode(connected=True)


# In[74]:


import plotly.graph_objs as go
import plotly.figure_factory as ff


# In[75]:


import statsmodels.api as sm
from numpy.random import normal, seed
from scipy.stats import norm


# In[76]:


from statsmodels.tsa.arima_model import ARMA
from statsmodels.tsa.stattools import adfuller
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_process import ArmaProcess
from statsmodels.tsa.arima_model import ARIMA
import math
from sklearn.metrics import mean_squared_error


# In[82]:


serie1 = pd.read_csv("C:/Users/Abidat/Desktop/TP_BDM_ABIDAT_Ikram_ST4/IOT-temp.csv", header=0)


# In[83]:


serie1.head()


# In[85]:


serie1.head()


# In[86]:


serie.head()


# In[88]:


serie.head(10)


# In[91]:


pip install findspark


# In[92]:


import findspark


# In[94]:


findspark.init("C:/Spark/spark-3.1.1-bin-hadoop2.7")


# In[95]:


import pyspark


# In[96]:


spark = SparkSession.builder.master("local[2]").appName("BDM").getOrCreate()


# In[97]:


import os
import pandas as pd
import numpy as np


# In[98]:


from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext


# In[99]:


from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col


# In[100]:


from pyspark.ml.regression import LinearRegression
from pyspark.mllib.evaluation import RegressionMetrics


# In[101]:


from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.evaluation import RegressionEvaluator


# In[102]:


import seaborn as sns
import matplotlib.pyplot as plt


# In[103]:


from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"


# In[104]:


pd.set_option('display.max_columns', 200)
pd.set_option('display.max_colwidth', 400)


# In[105]:


from matplotlib import rcParams
sns.set(context='notebook', style='whitegrid', rc={'figure.figsize': (18,4)})
rcParams['figure.figsize'] = 18,4


# In[106]:


get_ipython().run_line_magic('matplotlib', 'inline')
get_ipython().run_line_magic('config', "InlineBackend.figure_format = 'retina'")


# In[107]:


rnd_seed=23
np.random.seed=rnd_seed
np.random.set_state=rnd_seed


# In[108]:


spark = SparkSession.builder.master("local[2]").appName("BDM").getOrCreate()


# In[109]:


spark


# In[110]:


sc = spark.sparkContext
sc


# In[111]:


sqlContext = SQLContext(spark.sparkContext)
sqlContext


# In[117]:


data_f = spark.read.csv("C:/Users/Abidat/Desktop/TP_BDM_ABIDAT_Ikram_ST4/IOT-temp.csv").cache()


# In[119]:


data_f.take(5)


# In[126]:


HOUSING_DATA = 'C:/Users/Abidat/Desktop/TP_BDM_ABIDAT_Ikram_ST4/IOT-temp.csv'


# In[129]:


schema = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("room_id/id", StringType(), nullable=True),
    StructField("noted_date", StringType(), nullable=True),
    StructField("temp", FloatType(), nullable=True),
    StructField("out/in", StringType(), nullable=True)]
)


# In[130]:


housing_data = spark.read.csv(path=HOUSING_DATA, schema=schema).cache()


# In[131]:


housing_data.take(5)


# In[132]:


housing_data.show(5)


# In[133]:


housing_data.columns


# In[134]:


housing_data.printSchema()


# In[135]:


housing_data.select('noted_date','temp').show(10)


# In[145]:


result_df = housing_data.groupBy("temp").count().sort("temp", ascending=False)


# In[146]:


result_df.show(10)


# In[142]:


result_df.toPandas().plot.bar(x='temp',figsize=(14, 6))


# In[147]:


from statsmodels.tsa.tsatools import detrend


# In[151]:


housing_data = housing_data.na.drop()


# In[153]:


import matplotlib.pyplot as plt


# In[154]:


housing_data.show(10)


# In[156]:


y_temp = [val.temp for val in housing_data.select('temp').collect()]
x_noted_date = [val.noted_date for val in housing_data.select('noted_date').collect()]

plt.plot(x_noted_date, y_temp)

plt.ylabel('temp')
plt.xlabel('noted_date')
plt.title('Les valeurs de la température dans le temps')
plt.legend(['temp'], loc='upper left')

plt.show()


# In[ ]:





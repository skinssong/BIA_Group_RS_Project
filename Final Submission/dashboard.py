import plotly.offline as pyo
import plotly.graph_objs as go
import pandas as pd
import datetime as dt

review_df = pd.read_csv('https://raw.githubusercontent.com/skinssong/BIA_660D/master/Final_Project/data_gathering/final_df.csv',sep='|')
review_df['TweetTime'] = review_df['TweetTime'].astype('datetime64')
print(review_df.columns)

list_of_emotion_cols = ['anger', 'disgust', 'fear', 'joy', 'sadness']
df2 = review_df.loc[review_df['TweetTime'].dt.date == dt.date(2018,4,2),list_of_emotion_cols + ['TweetTime']]

traces = [go.Scatter(x=df2['TweetTime'], y= df2[name],mode='markers+lines', name=name) for name in list_of_emotion_cols]


layout = go.Layout(
    title = 'Population Estimates of the Six New England States'
)
fig = go.Figure(data=traces,layout=layout)
pyo.plot(fig, filename='line2.html')

# data = [go.Scatter(
#     x = random_x,
#     y = random_y,
#     mode = 'markers',
# )]
#
#
# layout = go.Layout(
#     title = 'Random Data Scatterplot', # Graph title
#     xaxis = dict(title = 'Some random x-values'), # x-axis label
#     yaxis = dict(title = 'Some random y-values'), # y-axis label
#     hovermode ='closest' # handles multiple points landing on the same vertical
# )
# fig = go.Figure(data=data, layout=layout)
# pyo.plot(fig, filename='scatter2.html')
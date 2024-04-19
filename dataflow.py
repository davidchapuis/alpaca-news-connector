'''
This script is an example of Alpaca News connector that retrieves 
data from Alpaca in real-time. This is based on websockets and bytewax 
(open-source framework to build highly scalable pipelines).

Input: ticker list
Output: stream of articles 
(each element being a tuple with source of the article and content of the article)

Example
Input: ["*"]
Output:

'''
import os
import json
from datetime import timedelta

from bytewax import operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async

import websockets

# Credentials for Alpaca API
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

#Input: News for any ticker
ticker_list = ["*"]

# Async function getting news from Alpaca News API
async def _ws_agen(ticker_list):
    uri = "wss://stream.data.alpaca.markets/v1beta1/news"
    async with websockets.connect(uri) as ws:
        # Authenticating
        await ws.send(
            json.dumps(
                {
                    "action": "auth",
                    "key": API_KEY,
                    "secret": API_SECRET,
                }
            )
        )
        await ws.recv()

        # Subscribing to the news
        await ws.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "news": ticker_list,
                }
            )
        )

        # Getting news in real-time
        while True:
            message = await ws.recv()
            try:
                articles = json.loads(message)
            except json.JSONDecodeError:
                continue
            for article in articles:
                if "source" in article:
                    yield (article["source"], article)
                else:
                    yield (article)


# News partition class inherited from Bytewax input StatefulSourcePartition
class NewsPartition(StatefulSourcePartition):
    '''
    Input partition that maintains state of its position.
    '''
    def __init__(self, ticker_list):
        '''
        Get async messages from Yahoo input and batch them
        up to 0,5 seconds or 100 messages.
        '''
        agen = _ws_agen(ticker_list)
        self._batcher = batch_async(agen, timedelta(seconds=0.5), 100)

    def next_batch(self):
        '''
        Attempt to get the next batch of items.
        '''
        return next(self._batcher)

    def snapshot(self):
        '''
        Snapshot the position of the next read of this partition.
        Returned via the resume_state parameter of the input builder.
        '''
        return None

# News source class inherited from Bytewax input FixedPartitionedSource
class NewsSource(FixedPartitionedSource):
    '''
    Input source with a fixed number of independent partitions.
    '''
    def __init__(self, ticker_list):
        self.ticker_list = ticker_list

    def list_parts(self):
        '''
        List all partitions the worker has access to.
        '''
        return ["single-part"]

    def build_part(self, step_id, for_key, _resume_state):
        '''
        Build anew or resume an input partition.
        Returns the built partition
        '''
        return NewsPartition(self.ticker_list)

# Creating dataflow and input
flow = Dataflow("analyzer")
inp = op.input("input", flow, NewsSource(ticker_list))
# Printing dataflow
op.inspect("inspect", inp)

# Type in your terminal the following command to run the dataflow:
# python -m bytewax.run dataflow
# Please note that will launch an infinite loop, use Ctrl+C to interrupt...
# ...(you will get an error message you can ignore)

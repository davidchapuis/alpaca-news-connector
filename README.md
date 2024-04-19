# Alpaca News Connector

This script is an example of Alpaca News connector that retrieves 
news from Alpaca in real-time. This is based on websockets and bytewax 
(open-source framework to build highly scalable pipelines).

## ****Prerequisites****

Make sure you are in the folder of the project and have a virtual environment activated.
Install the Python modules (Websockets, Bytewax) by runing the following command in your terminal:

``` bash
pip install -r requirements.txt
```

## **Running the Dataflow**

If needed, grant permissions:

``` bash
chmod +x run.sh
```

Then, run the following command:

``` bash
./run.sh
```
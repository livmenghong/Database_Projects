
# Trade Submission API

The Trade Submission API is a Python API based on [FastAPI](https://fastapi.tiangolo.com/) Framework. This API can be used to submit a new trade to the MongoDB database set in the config.yaml file in the config folder. This API can also delete a trade from that same MongoDB database. The default MongoDB used for this API is database Equity and collection CourseworkTwo. The name of the database and collection in config.yaml can be changed without affecting the code's logic.

## Using the API

### Before launching the API, make sure that all required modules have been installed.

Run the following commands:

```
cd .
pip install -r requirements.txt

```

### Launch the Trade Submission API using Uvicorn

In order to launch the API, in your terminal, please follow this command:

```
python App.py

```

By following these commands, you will change the working directory to 3.CourseworkThree and launch the API on the  [Uvicorn](https://www.uvicorn.org/) server and listening on port 8000.

To access the API UI on your browser and information about the API, with the API launched in the step above, please go to the URL below:

* For Swagger: http://localhost:8000/docs#/
* For Schemas: http://localhost:8000/openapi.json

### APIs: 

#### 1. Post a new trade:

##### Requirements: JSON format values for the body

Example: 

```yaml 
{
  "DateTime": "2023-01-16T15:06:51.762Z", (optional)
  "TradeId": "string", (optional)
  "Trader": "Warren", (required)
  "Symbol": "APPL", (required)
  "Price": 130, (required)
  "Quantity": 2000, (required)
  "Notional": 260000, (required)
  "TradeType": "BUY", (required)
  "Ccy": "USD", (required)
  "Counterparty": "JPM"" (required)
}
```

##### Response: Message regarding the submission

Example:

* If the trade submission has been accepted, a JSON response containing the inserted document and a message will be created:

```yaml
[
  {
    "DateTime": "2023-01-16 15:11:11Z",
    "TradeId": "sstringstring20230116151111",
    "Trader": "string",
    "Symbol": "string",
    "Price": 1,
    "Quantity": 1,
    "Notional": 1,
    "TradeType": "string",
    "Ccy": "string",
    "Counterparty": "string"
  },
  {
    "msg": "Trade has been submitted successfully"
  }
]
```

#### 2. Delete a trade:

##### Requirements: TradeId of the trade

Example: BMRH5231NCLH20200106100324

##### Response: Message regarding the delete

Example:

* If the trade entered has been successfully deleted, a JSON response will be created that will contain HTTP status code " 202 Accepted ".


  

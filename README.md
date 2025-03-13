
# **LMNH ETL CLI**

## **Overview**
This Python CLI tool is designed for the Liverpool Museum of Natural History (LMNH) to extract kiosk data from a Kafka topic, transform the data, and load it into an AWS RDS database. 

## **Features**
- Extracts real-time data from a Kafka topic.
- Transforms the raw data into a structured format.
- Loads the processed data into an AWS RDS PostgreSQL database.
- Configurable environment variables for flexibility.
- CLI interface for easy execution and management.
- Allow logging of invalid data to local file.

## **Prerequisites**
Before running the script, ensure you have the following:
- **Python**: Version 3.8+ installed
- **Kafka Broker**: Access to the Kafka topic containing kiosk data
- **AWS RDS**: PostgreSQL instance set up and accessible
- **Environment Variables**: Required keys stored in a `.env` file

## **Installation**
1. **Clone the Repository**:
   ```sh
   git clone <repository-url>
   cd lmnh-etl-cli
   ```

2. **Create a Virtual Environment** (Optional but recommended):
   ```sh
   python3 -m venv .venv
   source .venv/bin/activate  # macOS/Linux
   .venv\Scripts\activate     # Windows
   ```

3. **Install Dependencies**:
   ```sh
   pip install -r requirements.txt
   ```

## **Configuration**
Create a `.env` file in the project root directory and add the necessary environment variables:
```
BOOTSTRAP_SERVERS=<your-bootstrap-server>
SECURITY_PROTOCOL=<recommended: SASL_SSL>
SASL_MECHANISM=<recommended: PLAIN>
USERNAME=<your-kafka-username>
PASSWORD=<your-kafka-password>
GROUP_ID=<your-consumer-group-id>
AUTO_OFFSET_RESET=<recommended: earliest>
TOPIC=<your-topic-name>
ACCESS_KEY=<your-aws-access-key>
SECRET_ACCESS_KEY=<your-aws-secret-access-key>
DATABASE_NAME=<your-aws-rds-db-name>
DATABASE_USERNAME=<your-aws-rds-db-username>
DATABASE_PASSWORD=<your-aws-rds-db-password>
DATABASE_IP=<your-aws-rds-db-ip-address>
DATABASE_PORT=<your-aws-rds-db-port>
```

## **Usage**
Run the CLI tool with:
```sh
python run.py
```
Options and arguments:
```sh
python run.py -l # log invalid messages to file
```

## **Project Structure**
```
lmnh-etl-cli/
├── README.md                     # Current file
├── clear_db                      # Bash script for clearing the database interaction data
├── invalid_messages.log          # Log where invalid messages are appended
├── lmnh_etl_cli
│   ├── __init__.py
│   ├── models
│   │   ├── __init__.py
│   │   ├── extractor.py
│   │   ├── kiosk_event.py
│   │   ├── loader.py
│   │   ├── pipeline.py
│   │   └── transformer.py
│   └── tests
│       ├── __init__.py
│       ├── test_extractor.py
│       ├── test_kiosk_event.py
│       └── test_transformer.py
├── .env                          # Created by user
├── requirements.txt              # Python dependencies
└── run.py                        # Main CLI script
```

## **Logging & Error Handling**
- Logs are written to `invalid_messages.log`

## **Testing**
Run unit tests:
```sh
pytest lmnh_etl_cli/tests
```

## **Clearing the database tables**

The `clear_db` script is designed to remove all rating and request interaction data from the PostgreSQL database while preserving the structure and other essential data, such as exhibition details. 

This script follows the same configuration as above.

Run:
```sh
bash clear_db
```

---
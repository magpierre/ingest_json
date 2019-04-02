# ingest_json
Sample Go client reading json files and ingest them as documents into a MapR Database

To build a docker container:
docker build -t ingest_json .

to run the docker container:
docker run ingest_csv which will show the parameters

To run with parameters (example):
docker run --rm -it ingest_json -password mapr -mapr-url somehost:5678 -mapr-tablename /tmp/ingested_data ~/input.csv

Or:
docker run --rm -it ingest_json -password mapr -mapr-url somehost:5678 -mapr-tablename /tmp/ingested_data < ~/input.csv


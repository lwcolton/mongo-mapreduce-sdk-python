from pymongo.mongo_client import MongoClient
config = {
    "google_oauth_client_id": "241046823849-mcl3m1ultrsmn96t0dg5hqdtlohoplt4.apps.googleusercontent.com",
    "google_oauth_client_secret": "ndfE1y3K0E2WSYdI1kqeO2ZT",
    "google_oauth_project_id": "inlaid-rig-260623",
    "site_domain": "localhost",
    "version": 1,
    "jwt_secret": "9f346979aba8b305bece21a8d42f306b0ebd949c504690e0305f47bbe4a8942132e1eb6855e2e536"
}
client = MongoClient("mongodb://web_mongod_1")
client["test"]["config"].insert_one(config)

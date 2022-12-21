from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL



RDS_DRIVER_NAME = "postgresql+psycopg2"
RDS_HOST = "airflowhudietl.cuvi1zuz0k0c.us-east-1.rds.amazonaws.com"
RDS_PORT = 5432
RDS_DATABASE = "postgres"
RDS_USERNAME = "airflowUser"
RDS_PASSWORD = "Talent2022"
# ==================================

RDS_URL = URL.create(
                    drivername=RDS_DRIVER_NAME,
                    host = RDS_HOST,
                    port = RDS_PORT,
                    database = RDS_DATABASE,
                    username = RDS_USERNAME,
                    password = RDS_PASSWORD
                )

engine = create_engine(RDS_URL)

x = engine.execute("show tables")
print(x.all())